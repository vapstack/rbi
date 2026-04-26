package rbi

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sort"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

type aggregateMetricOp uint8

const (
	aggregateMetricCount aggregateMetricOp = iota
	aggregateMetricSum
	aggregateMetricAvg
	aggregateMetricMin
	aggregateMetricMax
	aggregateMetricDistinct
	aggregateMetricCountDistinct
)

type aggregateValueKind uint8

const (
	aggregateValueInvalid aggregateValueKind = iota
	aggregateValueSigned
	aggregateValueUnsigned
	aggregateValueFloat
	aggregateValueBool
	aggregateValueString
)

type aggregateFieldRef struct {
	name      string
	out       string
	ordinary  indexedFieldAccessor
	measure   measureFieldAccessor
	isMeasure bool
	kind      aggregateValueKind
}

type aggregateMetric struct {
	op       aggregateMetricOp
	out      string
	field    aggregateFieldRef
	rowCount bool
}

type aggregateOrder struct {
	index int
	desc  bool
}

type aggregateHavingOp uint8

const (
	aggregateHavingAnd aggregateHavingOp = iota + 1
	aggregateHavingOr
	aggregateHavingNot
	aggregateHavingEQ
	aggregateHavingNE
	aggregateHavingGT
	aggregateHavingGTE
	aggregateHavingLT
	aggregateHavingLTE
	aggregateHavingIN
	aggregateHavingExists
	aggregateHavingIsNull
)

type aggregateHavingExpr struct {
	op     aggregateHavingOp
	index  int
	value  Value
	values []Value
	args   []aggregateHavingExpr
}

type aggregateQuery struct {
	filter    *qir.Query
	groups    []aggregateFieldRef
	metrics   []aggregateMetric
	having    aggregateHavingExpr
	hasHaving bool
	order     []aggregateOrder
	offset    uint64
	limit     uint64
}

type aggregateMetricState struct {
	metric aggregateMetric
	seen   bool
	count  uint64

	intSum   int64
	uintSum  uint64
	floatSum float64

	best Value
}

// Aggregate evaluates a reduction query against the current index snapshot.
func (db *DB[K, V]) Aggregate(q *qx.QX) (Result, error) {
	if err := db.beginOp(); err != nil {
		return Result{}, err
	}
	defer db.endOp()

	if db.transparent {
		return Result{}, ErrNoIndex
	}

	prepared, err := db.prepareAggregate(q)
	if err != nil {
		return Result{}, err
	}
	defer prepared.release()

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)

	ids, err := view.aggregateMatchedIDs(prepared.filter)
	if err != nil {
		return Result{}, err
	}
	defer ids.Release()

	result, err := view.executeAggregate(prepared, ids)
	if err != nil {
		return Result{}, err
	}
	if prepared.hasHaving {
		result = applyAggregateHaving(result, prepared.having)
	}
	if len(prepared.order) > 0 {
		sort.Sort(aggregateRowSorter{rows: result.Rows, order: prepared.order})
	}
	return applyAggregateWindow(result, prepared.offset, prepared.limit), nil
}

func (q *aggregateQuery) release() {
	if q == nil {
		return
	}
	if q.filter != nil {
		q.filter.Release()
		q.filter = nil
	}
	q.groups = nil
	q.metrics = nil
	q.having = aggregateHavingExpr{}
	q.hasHaving = false
	q.order = nil
}

func (db *DB[K, V]) prepareAggregate(src *qx.QX) (*aggregateQuery, error) {
	if src == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if src.Reduction == nil || src.Reduction.IsEmpty() {
		return nil, fmt.Errorf("%w: aggregate query requires reduction", ErrInvalidQuery)
	}
	if len(src.Projection) > 0 {
		return nil, fmt.Errorf("%w: aggregate projection is not supported", ErrInvalidQuery)
	}

	filter, err := qir.PrepareQueryResolved(&qx.QX{Filter: src.Filter}, preparedFieldResolver[K, V]{db: db})
	if err != nil {
		return nil, err
	}

	out := &aggregateQuery{
		filter: filter,
		offset: src.Window.Offset,
		limit:  src.Window.Limit,
	}
	outputPositions := make(map[string]int, len(src.Reduction.Group)+len(src.Reduction.Metrics))

	for i := range src.Reduction.Group {
		group, err := db.prepareAggregateGroup(src.Reduction.Group[i])
		if err != nil {
			out.release()
			return nil, err
		}
		if err := reserveAggregateOutputName(outputPositions, group.out, len(outputPositions)); err != nil {
			out.release()
			return nil, err
		}
		out.groups = append(out.groups, group)
	}

	for i := range src.Reduction.Metrics {
		metric, err := db.prepareAggregateMetric(src.Reduction.Metrics[i])
		if err != nil {
			out.release()
			return nil, err
		}
		if err := reserveAggregateOutputName(outputPositions, metric.out, len(outputPositions)); err != nil {
			out.release()
			return nil, err
		}
		out.metrics = append(out.metrics, metric)
	}
	for i := range out.metrics {
		if out.metrics[i].op != aggregateMetricDistinct {
			continue
		}
		if len(out.groups) != 0 || len(out.metrics) != 1 {
			out.release()
			return nil, fmt.Errorf("%w: DISTINCT is supported only as a single ungrouped metric", ErrInvalidQuery)
		}
	}

	if len(out.metrics) == 0 && len(out.groups) == 0 {
		out.release()
		return nil, fmt.Errorf("%w: aggregate query has no groups or metrics", ErrInvalidQuery)
	}
	if !src.Reduction.Having.IsZero() {
		having, err := prepareAggregateHaving(src.Reduction.Having, outputPositions)
		if err != nil {
			out.release()
			return nil, err
		}
		out.having = having
		out.hasHaving = true
	}
	if len(src.Order) > 0 {
		order, err := prepareAggregateOrder(src.Order, outputPositions)
		if err != nil {
			out.release()
			return nil, err
		}
		out.order = order
	}
	return out, nil
}

func reserveAggregateOutputName(seen map[string]int, name string, pos int) error {
	if _, ok := seen[name]; ok {
		return fmt.Errorf("%w: duplicate aggregate output %q", ErrInvalidQuery, name)
	}
	seen[name] = pos
	return nil
}

func prepareAggregateOrder(src []qx.Order, outputs map[string]int) ([]aggregateOrder, error) {
	order := make([]aggregateOrder, 0, len(src))
	for i := range src {
		by := src[i].By
		if by.Kind != qx.KindOUT || by.Name == "" {
			return nil, fmt.Errorf("%w: aggregate ORDER supports only OUT references", ErrInvalidQuery)
		}
		pos, ok := outputs[by.Name]
		if !ok {
			return nil, fmt.Errorf("%w: unknown aggregate output %q in ORDER", ErrInvalidQuery, by.Name)
		}
		order = append(order, aggregateOrder{index: pos, desc: src[i].Desc})
	}
	return order, nil
}

func prepareAggregateHaving(expr qx.Expr, outputs map[string]int) (aggregateHavingExpr, error) {
	if expr.Kind != qx.KindOP {
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING root must be a predicate", ErrInvalidQuery)
	}
	switch expr.Name {
	case qx.OpAND, qx.OpOR:
		args := make([]aggregateHavingExpr, len(expr.Args))
		for i := range expr.Args {
			arg, err := prepareAggregateHaving(expr.Args[i], outputs)
			if err != nil {
				return aggregateHavingExpr{}, err
			}
			args[i] = arg
		}
		op := aggregateHavingAnd
		if expr.Name == qx.OpOR {
			op = aggregateHavingOr
		}
		return aggregateHavingExpr{op: op, args: args}, nil
	case qx.OpNOT:
		if len(expr.Args) != 1 {
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING NOT expects one predicate", ErrInvalidQuery)
		}
		arg, err := prepareAggregateHaving(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		return aggregateHavingExpr{op: aggregateHavingNot, args: []aggregateHavingExpr{arg}}, nil
	case qx.OpEXISTS, qx.OpISNULL:
		if len(expr.Args) != 1 {
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING %s expects one OUT reference", ErrInvalidQuery, expr.Name)
		}
		pos, err := prepareAggregateHavingOutput(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		op := aggregateHavingExists
		if expr.Name == qx.OpISNULL {
			op = aggregateHavingIsNull
		}
		return aggregateHavingExpr{op: op, index: pos}, nil
	case qx.OpEQ, qx.OpNE, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		if len(expr.Args) != 2 {
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING %s expects OUT and literal", ErrInvalidQuery, expr.Name)
		}
		pos, err := prepareAggregateHavingOutput(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		value, err := aggregateLiteralValue(expr.Args[1])
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		return aggregateHavingExpr{op: aggregateHavingPredicateOp(expr.Name), index: pos, value: value}, nil
	case qx.OpIN:
		if len(expr.Args) != 2 {
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING IN expects OUT and literal slice", ErrInvalidQuery)
		}
		pos, err := prepareAggregateHavingOutput(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		values, err := aggregateLiteralValueSlice(expr.Args[1])
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		return aggregateHavingExpr{op: aggregateHavingIN, index: pos, values: values}, nil
	default:
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING supports only simple OUT predicates", ErrInvalidQuery)
	}
}

func prepareAggregateHavingOutput(expr qx.Expr, outputs map[string]int) (int, error) {
	if expr.Kind != qx.KindOUT || expr.Name == "" {
		return 0, fmt.Errorf("%w: aggregate HAVING left side supports only OUT references", ErrInvalidQuery)
	}
	pos, ok := outputs[expr.Name]
	if !ok {
		return 0, fmt.Errorf("%w: unknown aggregate output %q in HAVING", ErrInvalidQuery, expr.Name)
	}
	return pos, nil
}

func aggregateHavingPredicateOp(name string) aggregateHavingOp {
	switch name {
	case qx.OpEQ:
		return aggregateHavingEQ
	case qx.OpNE:
		return aggregateHavingNE
	case qx.OpGT:
		return aggregateHavingGT
	case qx.OpGTE:
		return aggregateHavingGTE
	case qx.OpLT:
		return aggregateHavingLT
	default:
		return aggregateHavingLTE
	}
}

func aggregateLiteralValue(expr qx.Expr) (Value, error) {
	if expr.Kind != qx.KindLIT {
		return Value{}, fmt.Errorf("%w: aggregate HAVING right side supports only literals", ErrInvalidQuery)
	}
	return aggregateLiteralRawValue(expr.Value)
}

func aggregateLiteralRawValue(raw any) (Value, error) {
	v := reflect.ValueOf(raw)
	if !v.IsValid() {
		return Value{}, nil
	}
	v, isNil := unwrapExprValue(v)
	if isNil {
		return Value{}, nil
	}
	if unix, ok := queryValueToUnixSeconds(v); ok {
		return Value{num: uint64(unix), any: ValueKindInt}, nil
	}
	switch v.Kind() {
	case reflect.String:
		return valueFromSafeString(v.String()), nil
	case reflect.Bool:
		if v.Bool() {
			return Value{num: 1, any: ValueKindBool}, nil
		}
		return Value{num: 0, any: ValueKindBool}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Value{num: uint64(v.Int()), any: ValueKindInt}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return Value{num: v.Uint(), any: ValueKindUint}, nil
	case reflect.Float32, reflect.Float64:
		return Value{num: math.Float64bits(v.Float()), any: ValueKindFloat}, nil
	default:
		return Value{}, fmt.Errorf("%w: unsupported aggregate HAVING literal type %T", ErrInvalidQuery, raw)
	}
}

func aggregateLiteralValueSlice(expr qx.Expr) ([]Value, error) {
	if expr.Kind != qx.KindLIT {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", ErrInvalidQuery)
	}
	raw := reflect.ValueOf(expr.Value)
	if !raw.IsValid() {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", ErrInvalidQuery)
	}
	raw, isNil := unwrapExprValue(raw)
	if isNil || raw.Kind() != reflect.Slice {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", ErrInvalidQuery)
	}
	if raw.Len() == 0 {
		return nil, fmt.Errorf("%w: aggregate HAVING IN: no values provided", ErrInvalidQuery)
	}
	values := make([]Value, raw.Len())
	for i := 0; i < raw.Len(); i++ {
		value, err := aggregateLiteralRawValue(raw.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func (db *DB[K, V]) prepareAggregateGroup(expr qx.Expr) (aggregateFieldRef, error) {
	if expr.Kind != qx.KindREF || expr.Name == "" {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY supports only field references", ErrInvalidQuery)
	}
	acc, ok := db.indexedFieldByName[expr.Name]
	if !ok {
		if _, measure := db.measureFieldByName[expr.Name]; measure {
			return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY measure field %q is not supported", ErrInvalidQuery, expr.Name)
		}
		return aggregateFieldRef{}, fmt.Errorf("%w: no index for group field %q", ErrInvalidQuery, expr.Name)
	}
	if acc.field.Slice {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY slice field %q is not supported", ErrInvalidQuery, expr.Name)
	}
	out := expr.Alias
	if out == "" {
		out = expr.Name
	}
	return aggregateFieldRef{name: expr.Name, out: out, ordinary: acc, kind: aggregateFieldValueKind(acc.field)}, nil
}

func (db *DB[K, V]) prepareAggregateMetric(expr qx.Expr) (aggregateMetric, error) {
	if expr.Kind != qx.KindOP {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric must be an operation", ErrInvalidQuery)
	}
	metric := aggregateMetric{out: expr.Alias}
	switch expr.Name {
	case qx.OpCOUNT:
		metric.op = aggregateMetricCount
		if len(expr.Args) == 0 {
			metric.rowCount = true
			if metric.out == "" {
				metric.out = "count"
			}
			return metric, nil
		}
		if len(expr.Args) == 1 && expr.Args[0].Kind == qx.KindOP && expr.Args[0].Name == qx.OpDISTINCT {
			metric.op = aggregateMetricCountDistinct
			if len(expr.Args[0].Args) != 1 || expr.Args[0].Args[0].Kind != qx.KindREF || expr.Args[0].Args[0].Name == "" {
				return aggregateMetric{}, fmt.Errorf("%w: COUNT(DISTINCT) supports only direct field reference", ErrInvalidQuery)
			}
			field, err := db.prepareAggregateMetricField(expr.Args[0].Args[0].Name, metric.op)
			if err != nil {
				return aggregateMetric{}, err
			}
			metric.field = field
			if metric.out == "" {
				metric.out = defaultAggregateMetricName(metric.op, field.name)
			}
			return metric, nil
		}
	case qx.OpSUM:
		metric.op = aggregateMetricSum
	case qx.OpAVG:
		metric.op = aggregateMetricAvg
	case qx.OpMIN:
		metric.op = aggregateMetricMin
	case qx.OpMAX:
		metric.op = aggregateMetricMax
	case qx.OpDISTINCT:
		metric.op = aggregateMetricDistinct
	default:
		return aggregateMetric{}, fmt.Errorf("%w: unsupported aggregate metric %q", ErrInvalidQuery, expr.Name)
	}
	if len(expr.Args) != 1 || expr.Args[0].Kind != qx.KindREF || expr.Args[0].Name == "" {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric %q supports only direct field reference", ErrInvalidQuery, expr.Name)
	}
	field, err := db.prepareAggregateMetricField(expr.Args[0].Name, metric.op)
	if err != nil {
		return aggregateMetric{}, err
	}
	metric.field = field
	if metric.out == "" {
		metric.out = defaultAggregateMetricName(metric.op, field.name)
	}
	return metric, nil
}

func (db *DB[K, V]) prepareAggregateMetricField(name string, op aggregateMetricOp) (aggregateFieldRef, error) {
	if acc, ok := db.measureFieldByName[name]; ok {
		if op == aggregateMetricDistinct || op == aggregateMetricCountDistinct {
			return aggregateFieldRef{}, fmt.Errorf("%w: DISTINCT over measure field %q is not supported", ErrInvalidQuery, name)
		}
		return aggregateFieldRef{name: name, measure: acc, isMeasure: true, kind: aggregateMeasureValueKind(acc.kind)}, nil
	}
	acc, ok := db.indexedFieldByName[name]
	if !ok {
		return aggregateFieldRef{}, fmt.Errorf("%w: no index for aggregate field %q", ErrInvalidQuery, name)
	}
	if acc.field.Slice {
		return aggregateFieldRef{}, fmt.Errorf("%w: aggregate over slice field %q is not supported", ErrInvalidQuery, name)
	}
	kind := aggregateFieldValueKind(acc.field)
	if op == aggregateMetricSum || op == aggregateMetricAvg {
		if acc.field.UseVI || !isAggregateNumericKind(acc.field.Kind) {
			return aggregateFieldRef{}, fmt.Errorf("%w: %s requires numeric field %q", ErrInvalidQuery, aggregateMetricOpName(op), name)
		}
	}
	return aggregateFieldRef{name: name, ordinary: acc, kind: kind}, nil
}

func defaultAggregateMetricName(op aggregateMetricOp, field string) string {
	switch op {
	case aggregateMetricCount:
		return "count_" + field
	case aggregateMetricCountDistinct:
		return "count_distinct_" + field
	case aggregateMetricSum:
		return "sum_" + field
	case aggregateMetricAvg:
		return "avg_" + field
	case aggregateMetricMin:
		return "min_" + field
	case aggregateMetricMax:
		return "max_" + field
	case aggregateMetricDistinct:
		return "distinct_" + field
	default:
		return field
	}
}

func aggregateMetricOpName(op aggregateMetricOp) string {
	switch op {
	case aggregateMetricSum:
		return "SUM"
	case aggregateMetricAvg:
		return "AVG"
	default:
		return "aggregate"
	}
}

func aggregateFieldValueKind(f *field) aggregateValueKind {
	if f.UseVI {
		return aggregateValueString
	}
	if isNativeTimeField(f) {
		return aggregateValueSigned
	}
	switch f.Kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return aggregateValueSigned
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return aggregateValueUnsigned
	case reflect.Float32, reflect.Float64:
		return aggregateValueFloat
	case reflect.Bool:
		return aggregateValueBool
	default:
		return aggregateValueString
	}
}

func aggregateMeasureValueKind(kind measureValueKind) aggregateValueKind {
	switch kind {
	case measureValueSigned:
		return aggregateValueSigned
	case measureValueUnsigned:
		return aggregateValueUnsigned
	case measureValueFloat:
		return aggregateValueFloat
	default:
		return aggregateValueInvalid
	}
}

func isAggregateNumericKind(kind reflect.Kind) bool {
	return isAggregateSignedKind(kind) || isAggregateUnsignedKind(kind) || isAggregateFloatKind(kind)
}

func isAggregateSignedKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func isAggregateUnsignedKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func isAggregateFloatKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func (qv *queryView[K, V]) aggregateMatchedIDs(q *qir.Query) (posting.List, error) {
	if q == nil {
		return qv.snapshotUniverseView(), nil
	}
	shape := qir.NewShape(q)
	res, err := qv.evalExpr(shape.Expr)
	if err != nil {
		return posting.List{}, err
	}
	if res.neg {
		ids := qv.snapshotUniverseView()
		if !res.ids.IsEmpty() {
			ids = ids.BuildAndNot(res.ids)
		}
		res.release()
		return ids, nil
	}
	ids := res.ids.Clone()
	res.release()
	return ids, nil
}

func (qv *queryView[K, V]) executeAggregate(q *aggregateQuery, ids posting.List) (Result, error) {
	if ids.IsEmpty() {
		if len(q.metrics) == 1 && q.metrics[0].op == aggregateMetricDistinct && len(q.groups) == 0 {
			return Result{Layout: []string{q.metrics[0].out}, Rows: make([]Row, 0)}, nil
		}
		if len(q.groups) > 0 {
			return Result{Layout: groupedAggregateLayout(q), Rows: make([]Row, 0)}, nil
		}
	}
	if len(q.metrics) == 1 && q.metrics[0].op == aggregateMetricDistinct && len(q.groups) == 0 {
		return qv.executeDistinctAggregate(q.metrics[0], ids)
	}
	if len(q.groups) > 0 {
		return qv.executeGroupedAggregate(q, ids)
	}
	return qv.executeUngroupedAggregate(q, ids)
}

func (qv *queryView[K, V]) executeUngroupedAggregate(q *aggregateQuery, ids posting.List) (Result, error) {
	layout := make([]string, len(q.metrics))
	states := make([]aggregateMetricState, len(q.metrics))
	empty := ids.IsEmpty()
	for i := range q.metrics {
		layout[i] = q.metrics[i].out
		states[i].metric = q.metrics[i]
		if !empty {
			err := qv.foldAggregateMetric(&states[i], ids)
			if err != nil {
				return Result{}, err
			}
		}
	}
	row := make(Row, len(states))
	for i := range states {
		row[i] = states[i].finish()
	}
	return Result{Layout: layout, Rows: []Row{row}}, nil
}

func groupedAggregateLayout(q *aggregateQuery) []string {
	layout := make([]string, 0, len(q.groups)+len(q.metrics))
	for i := range q.groups {
		layout = append(layout, q.groups[i].out)
	}
	for i := range q.metrics {
		layout = append(layout, q.metrics[i].out)
	}
	return layout
}

func (qv *queryView[K, V]) executeDistinctAggregate(metric aggregateMetric, ids posting.List) (Result, error) {
	rows := make([]Row, 0)
	if err := qv.appendDistinctRows(&rows, metric.field.ordinary, ids); err != nil {
		return Result{}, err
	}
	return Result{Layout: []string{metric.out}, Rows: rows}, nil
}

func (qv *queryView[K, V]) executeGroupedAggregate(q *aggregateQuery, ids posting.List) (Result, error) {
	layout := groupedAggregateLayout(q)
	rows := make([]Row, 0)
	groupValues := make([]Value, len(q.groups))
	if err := qv.aggregateGroupRecursive(q, ids, 0, groupValues, &rows); err != nil {
		return Result{}, err
	}
	return Result{Layout: layout, Rows: rows}, nil
}

func (qv *queryView[K, V]) aggregateGroupRecursive(
	q *aggregateQuery,
	current posting.List,
	level int,
	groupValues []Value,
	rows *[]Row,
) error {
	if level == len(q.groups) {
		states := make([]aggregateMetricState, len(q.metrics))
		for i := range q.metrics {
			states[i].metric = q.metrics[i]
			if err := qv.foldAggregateMetric(&states[i], current); err != nil {
				return err
			}
		}
		row := make(Row, len(groupValues)+len(states))
		copy(row, groupValues)
		for i := range states {
			row[len(groupValues)+i] = states[i].finish()
		}
		*rows = append(*rows, row)
		return nil
	}

	acc := q.groups[level].ordinary
	ov := newFieldOverlayStorage(qv.snap.index.Get(acc.ordinal))
	for rank := 0; rank < ov.keyCount(); rank++ {
		bucketIDs := ov.postingAt(rank)
		next := current.Borrow().BuildAnd(bucketIDs)
		if next.IsEmpty() {
			next.Release()
			continue
		}
		groupValues[level] = aggregateValueFromIndexKey(acc.field, ov.keyAt(rank))
		if err := qv.aggregateGroupRecursive(q, next, level+1, groupValues, rows); err != nil {
			next.Release()
			return err
		}
		next.Release()
	}
	nilIDs := newFieldOverlayStorage(qv.snap.nilIndex.Get(acc.ordinal)).lookupPostingRetained(nilIndexEntryKey)
	if !nilIDs.IsEmpty() {
		next := current.Borrow().BuildAnd(nilIDs)
		if !next.IsEmpty() {
			groupValues[level] = Value{}
			if err := qv.aggregateGroupRecursive(q, next, level+1, groupValues, rows); err != nil {
				next.Release()
				return err
			}
		}
		next.Release()
	}
	return nil
}

func (qv *queryView[K, V]) appendDistinctRows(rows *[]Row, acc indexedFieldAccessor, ids posting.List) error {
	ov := newFieldOverlayStorage(qv.snap.index.Get(acc.ordinal))
	for rank := 0; rank < ov.keyCount(); rank++ {
		bucketIDs := ov.postingAt(rank)
		if aggregateIntersectCardinality(ids, bucketIDs, qv.snapshotUniverseCardinality()) == 0 {
			continue
		}
		*rows = append(*rows, Row{aggregateValueFromIndexKey(acc.field, ov.keyAt(rank))})
	}
	nilIDs := newFieldOverlayStorage(qv.snap.nilIndex.Get(acc.ordinal)).lookupPostingRetained(nilIndexEntryKey)
	if aggregateIntersectCardinality(ids, nilIDs, qv.snapshotUniverseCardinality()) > 0 {
		*rows = append(*rows, Row{Value{}})
	}
	return nil
}

func (qv *queryView[K, V]) foldAggregateMetric(state *aggregateMetricState, ids posting.List) error {
	metric := state.metric
	if metric.rowCount {
		state.count = ids.Cardinality()
		state.seen = true
		return nil
	}
	if metric.field.isMeasure {
		return qv.foldMeasureMetric(state, ids)
	}
	return qv.foldOrdinaryMetric(state, ids)
}

func (qv *queryView[K, V]) foldMeasureMetric(state *aggregateMetricState, ids posting.List) error {
	acc := state.metric.field.measure
	storage := qv.snap.measure.Get(acc.ordinal)
	if ids.IsEmpty() || storage.rows() == 0 {
		return nil
	}
	if ids.Cardinality() == qv.snapshotUniverseCardinality() {
		if state.metric.op == aggregateMetricCount {
			state.count += uint64(storage.rows())
			state.seen = true
			return nil
		}
		return state.addMeasureStorageAll(storage, acc.kind)
	}
	if useMeasureMergeScan(ids.Cardinality(), storage) {
		return state.addMeasureStorageIntersect(storage, acc.kind, ids)
	}
	it := ids.Iter()
	for it.HasNext() {
		id := it.Next()
		raw, ok := storage.lookup(id)
		if !ok {
			continue
		}
		if err := state.addRawMeasure(raw, acc.kind, 1); err != nil {
			it.Release()
			return err
		}
	}
	it.Release()
	return nil
}

func useMeasureMergeScan(idCardinality uint64, storage measureFieldStorage) bool {
	lookupSteps := measureStorageLookupSteps(storage)
	if lookupSteps <= 1 {
		return false
	}
	return idCardinality > uint64(storage.rows())/(lookupSteps-1)
}

func measureStorageLookupSteps(storage measureFieldStorage) uint64 {
	if storage.flat != nil {
		return uint64(bits.Len64(uint64(storage.flat.ids.Len())))
	}
	if storage.chunked != nil {
		return uint64(bits.Len64(uint64(storage.chunked.refsByID.Len())) + bits.Len64(measureChunkTargetRows))
	}
	return 1
}

func (state *aggregateMetricState) addMeasureStorageAll(storage measureFieldStorage, kind measureValueKind) error {
	if storage.flat != nil {
		return state.addMeasureFlatAll(storage.flat, kind)
	}
	if storage.chunked != nil {
		return state.addMeasureChunkedAll(storage.chunked, kind)
	}
	return nil
}

func (state *aggregateMetricState) addMeasureFlatAll(root *measureFlatRoot, kind measureValueKind) error {
	for i := 0; i < root.values.Len(); i++ {
		if err := state.addRawMeasure(root.values.Get(i), kind, 1); err != nil {
			return err
		}
	}
	return nil
}

func (state *aggregateMetricState) addMeasureChunkedAll(root *measureChunkedRoot, kind measureValueKind) error {
	for chunkPos := 0; chunkPos < root.refsByID.Len(); chunkPos++ {
		chunk := root.refsByID.Get(chunkPos).chunk
		for i := 0; i < chunk.values.Len(); i++ {
			if err := state.addRawMeasure(chunk.values.Get(i), kind, 1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (state *aggregateMetricState) addMeasureStorageIntersect(storage measureFieldStorage, kind measureValueKind, ids posting.List) error {
	it := ids.Iter()
	if !it.HasNext() {
		it.Release()
		return nil
	}
	filterID := it.Next()
	if storage.flat != nil {
		for i := 0; i < storage.flat.ids.Len(); i++ {
			measureID := storage.flat.ids.Get(i)
			for filterID < measureID {
				if !it.HasNext() {
					it.Release()
					return nil
				}
				filterID = it.Next()
			}
			if filterID == measureID {
				if err := state.addRawMeasure(storage.flat.values.Get(i), kind, 1); err != nil {
					it.Release()
					return err
				}
			}
		}
		it.Release()
		return nil
	}
	if storage.chunked != nil {
		for chunkPos := 0; chunkPos < storage.chunked.refsByID.Len(); chunkPos++ {
			chunk := storage.chunked.refsByID.Get(chunkPos).chunk
			for i := 0; i < chunk.ids.Len(); i++ {
				measureID := chunk.ids.Get(i)
				for filterID < measureID {
					if !it.HasNext() {
						it.Release()
						return nil
					}
					filterID = it.Next()
				}
				if filterID == measureID {
					if err := state.addRawMeasure(chunk.values.Get(i), kind, 1); err != nil {
						it.Release()
						return err
					}
				}
			}
		}
	}
	it.Release()
	return nil
}

func (qv *queryView[K, V]) foldOrdinaryMetric(state *aggregateMetricState, ids posting.List) error {
	acc := state.metric.field.ordinary
	ov := newFieldOverlayStorage(qv.snap.index.Get(acc.ordinal))
	universe := qv.snapshotUniverseCardinality()
	for rank := 0; rank < ov.keyCount(); rank++ {
		bucketIDs := ov.postingAt(rank)
		n := aggregateIntersectCardinality(ids, bucketIDs, universe)
		if n == 0 {
			continue
		}
		if state.metric.op == aggregateMetricCount {
			state.count += n
			state.seen = true
			continue
		}
		if state.metric.op == aggregateMetricCountDistinct {
			state.count++
			state.seen = true
			continue
		}
		value := aggregateValueFromIndexKey(acc.field, ov.keyAt(rank))
		if err := state.addValue(value, n); err != nil {
			return err
		}
	}
	return nil
}

func aggregateIntersectCardinality(filter posting.List, bucket posting.List, universe uint64) uint64 {
	if filter.IsEmpty() || bucket.IsEmpty() {
		return 0
	}
	if filter.Cardinality() == universe {
		return bucket.Cardinality()
	}
	ids := filter.Borrow().BuildAnd(bucket)
	card := ids.Cardinality()
	ids.Release()
	return card
}

func (state *aggregateMetricState) addRawMeasure(raw uint64, kind measureValueKind, n uint64) error {
	switch kind {
	case measureValueSigned:
		return state.addSigned(int64(raw), n)
	case measureValueUnsigned:
		return state.addUnsigned(raw, n)
	case measureValueFloat:
		return state.addFloat(math.Float64frombits(raw), n)
	default:
		return fmt.Errorf("%w: unsupported measure value kind", ErrInvalidQuery)
	}
}

func (state *aggregateMetricState) addValue(value Value, n uint64) error {
	if state.metric.op == aggregateMetricCount {
		state.count += n
		state.seen = true
		return nil
	}
	switch state.metric.field.kind {
	case aggregateValueSigned:
		v, ok := value.Int()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-signed index value", ErrInvalidQuery, state.metric.field.name)
		}
		return state.addSigned(v, n)
	case aggregateValueUnsigned:
		v, ok := value.Uint()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-unsigned index value", ErrInvalidQuery, state.metric.field.name)
		}
		return state.addUnsigned(v, n)
	case aggregateValueFloat:
		v, ok := value.Float()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-float index value", ErrInvalidQuery, state.metric.field.name)
		}
		return state.addFloat(v, n)
	default:
		if state.metric.op == aggregateMetricMin || state.metric.op == aggregateMetricMax {
			state.addBest(value)
			return nil
		}
		return fmt.Errorf("%w: aggregate requires numeric field %q", ErrInvalidQuery, state.metric.field.name)
	}
}

func (state *aggregateMetricState) addSigned(v int64, n uint64) error {
	if state.metric.op == aggregateMetricCount {
		state.count += n
		state.seen = true
		return nil
	}
	if state.metric.op == aggregateMetricMin || state.metric.op == aggregateMetricMax {
		state.addBest(Value{num: uint64(v), any: ValueKindInt})
		return nil
	}
	state.count += n
	state.floatSum += float64(v) * float64(n)
	if state.metric.op == aggregateMetricSum {
		if n > uint64(math.MaxInt64) {
			return fmt.Errorf("%w: integer SUM overflow", ErrInvalidQuery)
		}
		add := int64(n) * v
		if v != 0 && add/v != int64(n) {
			return fmt.Errorf("%w: integer SUM overflow", ErrInvalidQuery)
		}
		if (add > 0 && state.intSum > math.MaxInt64-add) || (add < 0 && state.intSum < math.MinInt64-add) {
			return fmt.Errorf("%w: integer SUM overflow", ErrInvalidQuery)
		}
		state.intSum += add
	}
	state.seen = true
	return nil
}

func (state *aggregateMetricState) addUnsigned(v uint64, n uint64) error {
	if state.metric.op == aggregateMetricCount {
		state.count += n
		state.seen = true
		return nil
	}
	if state.metric.op == aggregateMetricMin || state.metric.op == aggregateMetricMax {
		state.addBest(Value{num: v, any: ValueKindUint})
		return nil
	}
	state.count += n
	state.floatSum += float64(v) * float64(n)
	if state.metric.op == aggregateMetricSum {
		if n != 0 && v > math.MaxUint64/n {
			return fmt.Errorf("%w: unsigned SUM overflow", ErrInvalidQuery)
		}
		add := v * n
		if state.uintSum > math.MaxUint64-add {
			return fmt.Errorf("%w: unsigned SUM overflow", ErrInvalidQuery)
		}
		state.uintSum += add
	}
	state.seen = true
	return nil
}

func (state *aggregateMetricState) addFloat(v float64, n uint64) error {
	if state.metric.op == aggregateMetricCount {
		state.count += n
		state.seen = true
		return nil
	}
	if state.metric.op == aggregateMetricMin || state.metric.op == aggregateMetricMax {
		state.addBest(Value{num: math.Float64bits(v), any: ValueKindFloat})
		return nil
	}
	state.count += n
	state.floatSum += v * float64(n)
	state.seen = true
	return nil
}

func (state *aggregateMetricState) addBest(value Value) {
	if !state.seen {
		state.best = value
		state.seen = true
		return
	}
	cmp := aggregateCompareValues(value, state.best, state.metric.field.kind)
	if (state.metric.op == aggregateMetricMin && cmp < 0) || (state.metric.op == aggregateMetricMax && cmp > 0) {
		state.best = value
	}
}

func (state *aggregateMetricState) finish() Value {
	switch state.metric.op {
	case aggregateMetricCount, aggregateMetricCountDistinct:
		return Value{num: state.count, any: ValueKindUint}
	case aggregateMetricSum:
		if !state.seen {
			switch state.metric.field.kind {
			case aggregateValueUnsigned:
				return Value{num: 0, any: ValueKindUint}
			case aggregateValueFloat:
				return Value{num: math.Float64bits(0), any: ValueKindFloat}
			default:
				return Value{num: 0, any: ValueKindInt}
			}
		}
		if state.metric.field.isMeasure {
			switch state.metric.field.measure.kind {
			case measureValueSigned:
				return Value{num: uint64(state.intSum), any: ValueKindInt}
			case measureValueUnsigned:
				return Value{num: state.uintSum, any: ValueKindUint}
			case measureValueFloat:
				return Value{num: math.Float64bits(state.floatSum), any: ValueKindFloat}
			}
		}
		switch state.metric.field.kind {
		case aggregateValueSigned:
			return Value{num: uint64(state.intSum), any: ValueKindInt}
		case aggregateValueUnsigned:
			return Value{num: state.uintSum, any: ValueKindUint}
		default:
			return Value{num: math.Float64bits(state.floatSum), any: ValueKindFloat}
		}
	case aggregateMetricAvg:
		if state.count == 0 {
			return Value{}
		}
		return Value{num: math.Float64bits(state.floatSum / float64(state.count)), any: ValueKindFloat}
	case aggregateMetricMin, aggregateMetricMax:
		if !state.seen {
			return Value{}
		}
		return state.best
	default:
		return Value{}
	}
}

func aggregateCompareValues(a Value, b Value, kind aggregateValueKind) int {
	switch kind {
	case aggregateValueSigned:
		av, _ := a.Int()
		bv, _ := b.Int()
		return compareInt64(av, bv)
	case aggregateValueUnsigned:
		av, _ := a.Uint()
		bv, _ := b.Uint()
		return compareUint64(av, bv)
	case aggregateValueFloat:
		av, _ := a.Float()
		bv, _ := b.Float()
		return compareFloat64QuerySemantics(av, bv)
	case aggregateValueBool:
		av, _ := a.Bool()
		bv, _ := b.Bool()
		return compareBool(av, bv)
	default:
		av, _ := a.String()
		bv, _ := b.String()
		return compareString(av, bv)
	}
}

func compareInt64(a int64, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareUint64(a uint64, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareBool(a bool, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func compareString(a string, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func aggregateValueFromIndexKey(f *field, key indexKey) Value {
	raw := key.meta
	if !key.isNumeric() {
		s := key.asUnsafeString()
		if !f.UseVI && f.Kind == reflect.Bool {
			if s == "1" {
				return Value{num: 1, any: ValueKindBool}
			}
			return Value{num: 0, any: ValueKindBool}
		}
		return valueFromUnsafeString(s)
	}
	switch {
	case isNativeTimeField(f):
		return Value{num: uint64(int64(raw ^ (uint64(1) << 63))), any: ValueKindInt}
	case isAggregateSignedKind(f.Kind):
		return Value{num: uint64(int64(raw ^ (uint64(1) << 63))), any: ValueKindInt}
	case isAggregateUnsignedKind(f.Kind):
		return Value{num: raw, any: ValueKindUint}
	case isAggregateFloatKind(f.Kind):
		return Value{num: math.Float64bits(float64FromOrderedKey(raw)), any: ValueKindFloat}
	default:
		return Value{num: raw, any: ValueKindUint}
	}
}

func float64FromOrderedKey(key uint64) float64 {
	const sign = uint64(1) << 63
	if key&sign != 0 {
		return math.Float64frombits(key ^ sign)
	}
	return math.Float64frombits(^key)
}

func (o fieldOverlay) keyAt(rank int) indexKey {
	if o.chunked != nil {
		pos := o.chunked.posForRank(rank)
		ref, ok := o.chunked.refAtChunk(pos.chunk)
		if !ok {
			return indexKey{}
		}
		return ref.chunk.keyAt(pos.entry)
	}
	return o.base[rank].Key
}

func applyAggregateHaving(result Result, having aggregateHavingExpr) Result {
	out := 0
	for i := range result.Rows {
		if having.eval(result.Rows[i]) {
			result.Rows[out] = result.Rows[i]
			out++
		}
	}
	for i := out; i < len(result.Rows); i++ {
		result.Rows[i] = nil
	}
	result.Rows = result.Rows[:out]
	return result
}

func (expr aggregateHavingExpr) eval(row Row) bool {
	switch expr.op {
	case aggregateHavingAnd:
		for i := range expr.args {
			if !expr.args[i].eval(row) {
				return false
			}
		}
		return true
	case aggregateHavingOr:
		for i := range expr.args {
			if expr.args[i].eval(row) {
				return true
			}
		}
		return false
	case aggregateHavingNot:
		return !expr.args[0].eval(row)
	case aggregateHavingExists:
		return row[expr.index].Kind() != ValueKindNone
	case aggregateHavingIsNull:
		return row[expr.index].Kind() == ValueKindNone
	case aggregateHavingIN:
		for i := range expr.values {
			cmp, ok := aggregateComparePredicateValues(row[expr.index], expr.values[i])
			if ok && cmp == 0 {
				return true
			}
		}
		return false
	default:
		left := row[expr.index]
		cmp, ok := aggregateComparePredicateValues(left, expr.value)
		switch expr.op {
		case aggregateHavingEQ:
			return ok && cmp == 0
		case aggregateHavingNE:
			return !ok || cmp != 0
		case aggregateHavingGT:
			return ok && left.Kind() != ValueKindNone && expr.value.Kind() != ValueKindNone && cmp > 0
		case aggregateHavingGTE:
			return ok && left.Kind() != ValueKindNone && expr.value.Kind() != ValueKindNone && cmp >= 0
		case aggregateHavingLT:
			return ok && left.Kind() != ValueKindNone && expr.value.Kind() != ValueKindNone && cmp < 0
		default:
			return ok && left.Kind() != ValueKindNone && expr.value.Kind() != ValueKindNone && cmp <= 0
		}
	}
}

type aggregateRowSorter struct {
	rows  []Row
	order []aggregateOrder
}

func (s aggregateRowSorter) Len() int {
	return len(s.rows)
}

func (s aggregateRowSorter) Less(i int, j int) bool {
	left := s.rows[i]
	right := s.rows[j]
	for k := range s.order {
		order := s.order[k]
		cmp := aggregateCompareOrderValues(left[order.index], right[order.index])
		if cmp == 0 {
			continue
		}
		if order.desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

func (s aggregateRowSorter) Swap(i int, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func aggregateComparePredicateValues(a Value, b Value) (int, bool) {
	ak := a.Kind()
	bk := b.Kind()
	if ak == ValueKindNone || bk == ValueKindNone {
		if ak == bk {
			return 0, true
		}
		return 0, false
	}
	if aggregateValueKindIsNumeric(ak) && aggregateValueKindIsNumeric(bk) {
		return aggregateCompareNumericValues(a, b)
	}
	if ak != bk {
		return 0, false
	}
	switch ak {
	case ValueKindBool:
		av, _ := a.Bool()
		bv, _ := b.Bool()
		return compareBool(av, bv), true
	case ValueKindString:
		av, _ := a.String()
		bv, _ := b.String()
		return compareString(av, bv), true
	default:
		return 0, false
	}
}

func aggregateCompareOrderValues(a Value, b Value) int {
	ak := a.Kind()
	bk := b.Kind()
	if ak == ValueKindNone || bk == ValueKindNone {
		return compareInt64(int64(ak), int64(bk))
	}
	if aggregateValueKindIsNumeric(ak) && aggregateValueKindIsNumeric(bk) {
		cmp, _ := aggregateCompareNumericValues(a, b)
		return cmp
	}
	if ak == bk {
		switch ak {
		case ValueKindBool:
			av, _ := a.Bool()
			bv, _ := b.Bool()
			return compareBool(av, bv)
		case ValueKindString:
			av, _ := a.String()
			bv, _ := b.String()
			return compareString(av, bv)
		}
	}
	return compareInt64(int64(ak), int64(bk))
}

func aggregateCompareNumericValues(a Value, b Value) (int, bool) {
	ak := a.Kind()
	bk := b.Kind()
	if ak == ValueKindFloat {
		av, _ := a.Float()
		switch bk {
		case ValueKindFloat:
			bv, _ := b.Float()
			return compareFloat64QuerySemantics(av, bv), true
		case ValueKindInt:
			bv, _ := b.Int()
			return -aggregateCompareInt64Float64(bv, av), true
		case ValueKindUint:
			bv, _ := b.Uint()
			return -aggregateCompareUint64Float64(bv, av), true
		}
		return 0, false
	}
	if bk == ValueKindFloat {
		bv, _ := b.Float()
		switch ak {
		case ValueKindInt:
			av, _ := a.Int()
			return aggregateCompareInt64Float64(av, bv), true
		case ValueKindUint:
			av, _ := a.Uint()
			return aggregateCompareUint64Float64(av, bv), true
		}
		return 0, false
	}
	if ak == ValueKindInt && bk == ValueKindInt {
		av, _ := a.Int()
		bv, _ := b.Int()
		return compareInt64(av, bv), true
	}
	if ak == ValueKindUint && bk == ValueKindUint {
		av, _ := a.Uint()
		bv, _ := b.Uint()
		return compareUint64(av, bv), true
	}
	if ak == ValueKindInt {
		av, _ := a.Int()
		bv, _ := b.Uint()
		if av < 0 {
			return -1, true
		}
		return compareUint64(uint64(av), bv), true
	}
	av, _ := a.Uint()
	bv, _ := b.Int()
	if bv < 0 {
		return 1, true
	}
	return compareUint64(av, uint64(bv)), true
}

func aggregateCompareInt64Float64(i int64, f float64) int {
	f = canonicalizeFloat64ForIndex(f)
	switch {
	case math.IsNaN(f), math.IsInf(f, 1):
		return -1
	case math.IsInf(f, -1):
		return 1
	case f < minInt64Float:
		return 1
	case f >= maxInt64Float:
		return -1
	}
	trunc := math.Trunc(f)
	base := int64(trunc)
	cmp := compareInt64(i, base)
	if cmp != 0 || f == trunc {
		return cmp
	}
	if f > 0 {
		return -1
	}
	return 1
}

func aggregateCompareUint64Float64(u uint64, f float64) int {
	f = canonicalizeFloat64ForIndex(f)
	switch {
	case math.IsNaN(f), math.IsInf(f, 1):
		return -1
	case math.IsInf(f, -1), f < 0:
		return 1
	case f >= maxUint64Float:
		return -1
	}
	trunc := math.Trunc(f)
	base := uint64(trunc)
	cmp := compareUint64(u, base)
	if cmp != 0 || f == trunc {
		return cmp
	}
	return -1
}

func aggregateValueKindIsNumeric(kind ValueKind) bool {
	return kind == ValueKindInt || kind == ValueKindUint || kind == ValueKindFloat
}

func applyAggregateWindow(result Result, offset uint64, limit uint64) Result {
	if offset == 0 && limit == 0 {
		return result
	}
	if offset >= uint64(len(result.Rows)) {
		result.Rows = nil
		return result
	}
	start := int(offset)
	end := len(result.Rows)
	if limit > 0 && limit < uint64(end-start) {
		end = start + int(limit)
	}
	rows := make([]Row, end-start)
	copy(rows, result.Rows[start:end])
	result.Rows = rows
	return result
}
