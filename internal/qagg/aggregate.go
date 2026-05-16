package qagg

import (
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
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

const aggregateGroupIDOrdinalMaxLen = 8 << 20

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
	ordinary  schema.IndexedFieldAccessor
	measure   schema.MeasureFieldAccessor
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

type Query struct {
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

type aggregateExecutor struct {
	snap *snapshot.View
}

func Execute(view *qexec.View, snap *snapshot.View, prepared *Query) (Result, error) {
	if len(prepared.groups) == 0 && len(prepared.metrics) == 1 && prepared.metrics[0].rowCount {
		count, err := Count(view, prepared.filter, true)
		if err != nil {
			return Result{}, err
		}
		result := Result{
			Layout: []string{prepared.metrics[0].out},
			Rows: []Row{{
				Value{num: count, any: ValueKindUint},
			}},
		}
		if prepared.hasHaving {
			result = applyAggregateHaving(result, prepared.having)
		}
		return applyAggregateWindow(result, prepared.offset, prepared.limit), nil
	}

	ids, err := view.Filter(prepared.filter)
	if err != nil {
		return Result{}, err
	}
	defer ids.Release()

	exec := aggregateExecutor{snap: snap}
	result, err := exec.executeAggregate(prepared, ids)
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

func (q *Query) Release() {
	if q.filter != nil {
		q.filter.Release()
		q.filter = nil
	}
	q.groups = nil
	q.metrics = nil
	q.having = aggregateHavingExpr{}
	q.hasHaving = false
	aggregateOrderSlicePool.Put(q.order)
	q.order = nil
}

func Prepare(src *qx.QX, rt *schema.Runtime) (*Query, error) {
	if src == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if src.Reduction == nil || src.Reduction.IsEmpty() {
		return nil, fmt.Errorf("%w: aggregate query requires reduction", qexec.ErrInvalidQuery)
	}
	if len(src.Projection) > 0 {
		return nil, fmt.Errorf("%w: aggregate projection is not supported", qexec.ErrInvalidQuery)
	}

	filter, err := qir.PrepareQuery(&qx.QX{Filter: src.Filter}, rt.IndexedByName)
	if err != nil {
		return nil, err
	}

	out := &Query{
		filter: filter,
		offset: src.Window.Offset,
		limit:  src.Window.Limit,
	}
	outputPositions := aggregateOutputPositionMapPool.Get(len(src.Reduction.Group) + len(src.Reduction.Metrics))
	defer aggregateOutputPositionMapPool.Put(outputPositions)

	for i := range src.Reduction.Group {
		group, err := prepareAggregateGroup(rt, src.Reduction.Group[i])
		if err != nil {
			out.Release()
			return nil, err
		}
		if err := reserveAggregateOutputName(outputPositions, group.out, len(outputPositions)); err != nil {
			out.Release()
			return nil, err
		}
		out.groups = append(out.groups, group)
	}

	for i := range src.Reduction.Metrics {
		metric, err := prepareAggregateMetric(rt, src.Reduction.Metrics[i])
		if err != nil {
			out.Release()
			return nil, err
		}
		if err = reserveAggregateOutputName(outputPositions, metric.out, len(outputPositions)); err != nil {
			out.Release()
			return nil, err
		}
		out.metrics = append(out.metrics, metric)
	}

	for i := range out.metrics {
		if out.metrics[i].op != aggregateMetricDistinct {
			continue
		}
		if len(out.groups) != 0 || len(out.metrics) != 1 {
			out.Release()
			return nil, fmt.Errorf("%w: DISTINCT is supported only as a single ungrouped metric", qexec.ErrInvalidQuery)
		}
	}

	if len(out.metrics) == 0 && len(out.groups) == 0 {
		out.Release()
		return nil, fmt.Errorf("%w: aggregate query has no groups or metrics", qexec.ErrInvalidQuery)
	}

	if !src.Reduction.Having.IsZero() {
		having, err := prepareAggregateHaving(src.Reduction.Having, outputPositions)
		if err != nil {
			out.Release()
			return nil, err
		}
		out.having = having
		out.hasHaving = true
	}

	if len(src.Order) > 0 {
		order, err := prepareAggregateOrder(src.Order, outputPositions)
		if err != nil {
			out.Release()
			return nil, err
		}
		out.order = order
	}

	return out, nil
}

func reserveAggregateOutputName(seen map[string]int, name string, pos int) error {
	if _, ok := seen[name]; ok {
		return fmt.Errorf("%w: duplicate aggregate output %q", qexec.ErrInvalidQuery, name)
	}
	seen[name] = pos
	return nil
}

func prepareAggregateOrder(src []qx.Order, outputs map[string]int) ([]aggregateOrder, error) {
	order := aggregateOrderSlicePool.Get(len(src))
	for i := range src {
		by := src[i].By
		if by.Kind != qx.KindOUT || by.Name == "" {
			aggregateOrderSlicePool.Put(order)
			return nil, fmt.Errorf("%w: aggregate ORDER supports only OUT references", qexec.ErrInvalidQuery)
		}
		pos, ok := outputs[by.Name]
		if !ok {
			aggregateOrderSlicePool.Put(order)
			return nil, fmt.Errorf("%w: unknown aggregate output %q in ORDER", qexec.ErrInvalidQuery, by.Name)
		}
		order = append(order, aggregateOrder{index: pos, desc: src[i].Desc})
	}
	return order, nil
}

func prepareAggregateHaving(expr qx.Expr, outputs map[string]int) (aggregateHavingExpr, error) {
	if expr.Kind != qx.KindOP {
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING root must be a predicate", qexec.ErrInvalidQuery)
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
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING NOT expects one predicate", qexec.ErrInvalidQuery)
		}
		arg, err := prepareAggregateHaving(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		return aggregateHavingExpr{op: aggregateHavingNot, args: []aggregateHavingExpr{arg}}, nil

	case qx.OpEXISTS, qx.OpISNULL:
		if len(expr.Args) != 1 {
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING %s expects one OUT reference", qexec.ErrInvalidQuery, expr.Name)
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
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING %s expects OUT and literal", qexec.ErrInvalidQuery, expr.Name)
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
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING IN expects OUT and literal slice", qexec.ErrInvalidQuery)
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
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING supports only simple OUT predicates", qexec.ErrInvalidQuery)
	}
}

func prepareAggregateHavingOutput(expr qx.Expr, outputs map[string]int) (int, error) {
	if expr.Kind != qx.KindOUT || expr.Name == "" {
		return 0, fmt.Errorf("%w: aggregate HAVING left side supports only OUT references", qexec.ErrInvalidQuery)
	}
	pos, ok := outputs[expr.Name]
	if !ok {
		return 0, fmt.Errorf("%w: unknown aggregate output %q in HAVING", qexec.ErrInvalidQuery, expr.Name)
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
		return Value{}, fmt.Errorf("%w: aggregate HAVING right side supports only literals", qexec.ErrInvalidQuery)
	}
	return aggregateLiteralRawValue(expr.Value)
}

func aggregateLiteralRawValue(raw any) (Value, error) {
	v := reflect.ValueOf(raw)
	if !v.IsValid() {
		return Value{}, nil
	}
	v, isNil := schema.UnwrapQueryValue(v)
	if isNil {
		return Value{}, nil
	}
	if unix, ok := schema.QueryValueToUnixSeconds(v); ok {
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
		return Value{}, fmt.Errorf("%w: unsupported aggregate HAVING literal type %T", qexec.ErrInvalidQuery, raw)
	}
}

func aggregateLiteralValueSlice(expr qx.Expr) ([]Value, error) {
	if expr.Kind != qx.KindLIT {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", qexec.ErrInvalidQuery)
	}
	raw := reflect.ValueOf(expr.Value)
	if !raw.IsValid() {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", qexec.ErrInvalidQuery)
	}
	raw, isNil := schema.UnwrapQueryValue(raw)
	if isNil || raw.Kind() != reflect.Slice {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", qexec.ErrInvalidQuery)
	}
	if raw.Len() == 0 {
		return nil, fmt.Errorf("%w: aggregate HAVING IN: no values provided", qexec.ErrInvalidQuery)
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

func prepareAggregateGroup(rt *schema.Runtime, expr qx.Expr) (aggregateFieldRef, error) {
	if expr.Kind != qx.KindREF || expr.Name == "" {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY supports only field references", qexec.ErrInvalidQuery)
	}

	acc, ok := rt.IndexedByName[expr.Name]
	if !ok {
		if _, measure := rt.MeasuresByName[expr.Name]; measure {
			return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY measure field %q is not supported", qexec.ErrInvalidQuery, expr.Name)
		}
		return aggregateFieldRef{}, fmt.Errorf("%w: no index for group field %q", qexec.ErrInvalidQuery, expr.Name)
	}

	if acc.Field.Slice {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY slice field %q is not supported", qexec.ErrInvalidQuery, expr.Name)
	}

	out := expr.Alias
	if out == "" {
		out = expr.Name
	}

	return aggregateFieldRef{name: expr.Name, out: out, ordinary: acc, kind: aggregateFieldValueKind(acc.Field)}, nil
}

func prepareAggregateMetric(rt *schema.Runtime, expr qx.Expr) (aggregateMetric, error) {
	if expr.Kind != qx.KindOP {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric must be an operation", qexec.ErrInvalidQuery)
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
				return aggregateMetric{}, fmt.Errorf("%w: COUNT(DISTINCT) supports only direct field reference", qexec.ErrInvalidQuery)
			}
			field, err := prepareAggregateMetricField(rt, expr.Args[0].Args[0].Name, metric.op)
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
		return aggregateMetric{}, fmt.Errorf("%w: unsupported aggregate metric %q", qexec.ErrInvalidQuery, expr.Name)
	}

	if len(expr.Args) != 1 || expr.Args[0].Kind != qx.KindREF || expr.Args[0].Name == "" {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric %q supports only direct field reference", qexec.ErrInvalidQuery, expr.Name)
	}

	f, err := prepareAggregateMetricField(rt, expr.Args[0].Name, metric.op)
	if err != nil {
		return aggregateMetric{}, err
	}

	metric.field = f
	if metric.out == "" {
		metric.out = defaultAggregateMetricName(metric.op, f.name)
	}
	return metric, nil
}

func prepareAggregateMetricField(rt *schema.Runtime, name string, op aggregateMetricOp) (aggregateFieldRef, error) {
	if acc, ok := rt.MeasuresByName[name]; ok {
		if op == aggregateMetricDistinct || op == aggregateMetricCountDistinct {
			return aggregateFieldRef{}, fmt.Errorf("%w: DISTINCT over measure field %q is not supported", qexec.ErrInvalidQuery, name)
		}
		return aggregateFieldRef{name: name, measure: acc, isMeasure: true, kind: aggregateMeasureValueKind(acc.Kind)}, nil
	}
	acc, ok := rt.IndexedByName[name]
	if !ok {
		return aggregateFieldRef{}, fmt.Errorf("%w: no index for aggregate field %q", qexec.ErrInvalidQuery, name)
	}
	if acc.Field.Slice {
		return aggregateFieldRef{}, fmt.Errorf("%w: aggregate over slice field %q is not supported", qexec.ErrInvalidQuery, name)
	}

	kind := aggregateFieldValueKind(acc.Field)
	if op == aggregateMetricSum || op == aggregateMetricAvg {
		if acc.Field.UseVI ||
			!(isAggregateSignedKind(acc.Field.Kind) ||
				isAggregateUnsignedKind(acc.Field.Kind) ||
				isAggregateFloatKind(acc.Field.Kind)) {
			return aggregateFieldRef{}, fmt.Errorf("%w: %s requires numeric field %q", qexec.ErrInvalidQuery, aggregateMetricOpName(op), name)
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

func aggregateFieldValueKind(f *schema.Field) aggregateValueKind {
	if f.UseVI {
		return aggregateValueString
	}
	if schema.IsNativeTimeField(f) {
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

func aggregateMeasureValueKind(kind schema.MeasureValueKind) aggregateValueKind {
	switch kind {
	case schema.MeasureValueSigned:
		return aggregateValueSigned
	case schema.MeasureValueUnsigned:
		return aggregateValueUnsigned
	case schema.MeasureValueFloat:
		return aggregateValueFloat
	default:
		return aggregateValueInvalid
	}
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

func (ae *aggregateExecutor) executeAggregate(q *Query, ids posting.List) (Result, error) {
	if ids.IsEmpty() {
		if len(q.metrics) == 1 && q.metrics[0].op == aggregateMetricDistinct && len(q.groups) == 0 {
			return Result{Layout: []string{q.metrics[0].out}, Rows: make([]Row, 0)}, nil
		}
		if len(q.groups) > 0 {
			return Result{Layout: groupedAggregateLayout(q), Rows: make([]Row, 0)}, nil
		}
	}
	if len(q.metrics) == 1 && q.metrics[0].op == aggregateMetricDistinct && len(q.groups) == 0 {
		return ae.executeDistinctAggregate(q.metrics[0], ids)
	}
	if len(q.groups) > 0 {
		return ae.executeGroupedAggregate(q, ids)
	}
	return ae.executeUngroupedAggregate(q, ids)
}

func (ae *aggregateExecutor) executeUngroupedAggregate(q *Query, ids posting.List) (Result, error) {
	layout := make([]string, len(q.metrics))
	states := aggregateMetricStateSlicePool.Get(len(q.metrics))[:len(q.metrics)]
	empty := ids.IsEmpty()
	for i := range q.metrics {
		layout[i] = q.metrics[i].out
		states[i].metric = q.metrics[i]
	}
	if !empty {
		err := ae.foldAggregateMetricStates(states, ids)
		if err != nil {
			aggregateMetricStateSlicePool.Put(states)
			return Result{}, err
		}
	}
	row := make(Row, len(states))
	for i := range states {
		row[i] = states[i].finish()
	}
	aggregateMetricStateSlicePool.Put(states)
	return Result{Layout: layout, Rows: []Row{row}}, nil
}

func groupedAggregateLayout(q *Query) []string {
	layout := make([]string, 0, len(q.groups)+len(q.metrics))
	for i := range q.groups {
		layout = append(layout, q.groups[i].out)
	}
	for i := range q.metrics {
		layout = append(layout, q.metrics[i].out)
	}
	return layout
}

func (ae *aggregateExecutor) executeDistinctAggregate(metric aggregateMetric, ids posting.List) (Result, error) {
	rows := make([]Row, 0)
	if err := ae.appendDistinctRows(&rows, metric.field.ordinary, ids); err != nil {
		return Result{}, err
	}
	return Result{Layout: []string{metric.out}, Rows: rows}, nil
}

func (ae *aggregateExecutor) executeGroupedAggregate(q *Query, ids posting.List) (Result, error) {
	if ae.canExecuteGroupedOrdinaryByID(q, ids) {
		return ae.executeGroupedOrdinaryByID(q, ids)
	}
	layout := groupedAggregateLayout(q)
	rows := make([]Row, 0)
	groupValues := make([]Value, len(q.groups))
	if err := ae.aggregateGroupRecursive(q, ids, 0, groupValues, &rows); err != nil {
		return Result{}, err
	}
	return Result{Layout: layout, Rows: rows}, nil
}

func (ae *aggregateExecutor) canExecuteGroupedOrdinaryByID(q *Query, ids posting.List) bool {
	hasOrdinary := false
	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount {
			continue
		}
		if metric.field.isMeasure {
			return false
		}
		hasOrdinary = true
	}
	if !hasOrdinary {
		return false
	}
	maxID, ok := ids.Maximum()
	if !ok || maxID >= aggregateGroupIDOrdinalMaxLen {
		return false
	}
	filterCardinality := ids.Cardinality()
	if maxID+1 > filterCardinality*16 {
		return false
	}

	metricKeys, metricRows, minMetricRows := ae.aggregateOrdinaryMetricWork(q)
	if metricRows == 0 || filterCardinality*8 < minMetricRows {
		return false
	}
	groupEstimate := ae.aggregateGroupCountUpperBound(q, filterCardinality)
	return aggregateMulGreater(groupEstimate, metricKeys, metricRows)
}

func (ae *aggregateExecutor) aggregateOrdinaryMetricWork(q *Query) (uint64, uint64, uint64) {
	var keys uint64
	var rows uint64
	var minRows uint64
	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount || hasPriorOrdinaryAggregateMetric(q.metrics, i) {
			continue
		}
		ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[metric.field.ordinary.Ordinal])
		fieldRows := ov.Rows()
		keys += uint64(ov.KeyCount())
		rows += fieldRows
		if minRows == 0 || fieldRows < minRows {
			minRows = fieldRows
		}
	}
	return keys, rows, minRows
}

func (ae *aggregateExecutor) aggregateGroupCountUpperBound(q *Query, filterCardinality uint64) uint64 {
	groups := uint64(1)
	for i := range q.groups {
		acc := q.groups[i].ordinary
		distinct := uint64(indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal]).KeyCount())
		if indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).KeyCount() > 0 {
			distinct++
		}
		if distinct == 0 {
			return 0
		}
		if groups > filterCardinality/distinct {
			return filterCardinality
		}
		groups *= distinct
	}
	if groups > filterCardinality {
		return filterCardinality
	}
	return groups
}

func aggregateMulGreater(a uint64, b uint64, limit uint64) bool {
	return b != 0 && a > limit/b
}

func (ae *aggregateExecutor) executeGroupedOrdinaryByID(q *Query, ids posting.List) (Result, error) {
	layout := groupedAggregateLayout(q)
	rows := make([]Row, 0)
	states := aggregateMetricStateSlicePool.Get(len(q.metrics))
	maxID, _ := ids.Maximum()
	groupByIDLen := int(maxID) + 1

	groupByID := pooled.GetUint32Slice(groupByIDLen)[:groupByIDLen]
	clear(groupByID)

	groupValues := make([]Value, len(q.groups))
	err := ae.buildGroupedOrdinaryIDMap(q, ids, 0, groupValues, &rows, &states, groupByID)
	if err == nil {
		err = ae.foldGroupedOrdinaryByID(q, rows, states, groupByID)
	}

	pooled.ReleaseUint32Slice(groupByID)

	if err != nil {
		aggregateMetricStateSlicePool.Put(states)
		return Result{}, err
	}
	for rowIdx := range rows {
		base := rowIdx * len(q.metrics)
		for metricIdx := range q.metrics {
			rows[rowIdx][len(q.groups)+metricIdx] = states[base+metricIdx].finish()
		}
	}
	aggregateMetricStateSlicePool.Put(states)

	return Result{Layout: layout, Rows: rows}, nil
}

func (ae *aggregateExecutor) buildGroupedOrdinaryIDMap(
	q *Query,
	current posting.List,
	level int,
	groupValues []Value,
	rows *[]Row,
	states *[]aggregateMetricState,
	groupByID []uint32,
) error {
	if level == len(q.groups) {
		rowIndex := len(*rows)
		if uint64(rowIndex) >= uint64(^uint32(0)) {
			return fmt.Errorf("%w: aggregate group count exceeds runtime limit", qexec.ErrInvalidQuery)
		}
		row := make(Row, len(groupValues)+len(q.metrics))
		copy(row, groupValues)
		*rows = append(*rows, row)

		rowCount := current.Cardinality()
		for i := range q.metrics {
			state := aggregateMetricState{metric: q.metrics[i]}
			if q.metrics[i].rowCount {
				state.count = rowCount
				state.seen = true
			}
			*states = append(*states, state)
		}

		groupOrdinal := uint32(rowIndex + 1)
		it := current.Iter()
		for it.HasNext() {
			id := it.Next()
			groupByID[int(id)] = groupOrdinal
		}
		it.Release()
		return nil
	}

	acc := q.groups[level].ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	for {
		key, bucketIDs, ok := cur.Next()
		if !ok {
			break
		}
		next := current.Borrow().BuildAnd(bucketIDs)
		if next.IsEmpty() {
			next.Release()
			continue
		}
		groupValues[level] = aggregateValueFromIndexKey(acc.Field, key)
		if err := ae.buildGroupedOrdinaryIDMap(q, next, level+1, groupValues, rows, states, groupByID); err != nil {
			next.Release()
			return err
		}
		next.Release()
	}

	nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
	if !nilIDs.IsEmpty() {
		next := current.Borrow().BuildAnd(nilIDs)
		if !next.IsEmpty() {
			groupValues[level] = Value{}
			if err := ae.buildGroupedOrdinaryIDMap(q, next, level+1, groupValues, rows, states, groupByID); err != nil {
				next.Release()
				return err
			}
		}
		next.Release()
	}

	return nil
}

func (ae *aggregateExecutor) foldGroupedOrdinaryByID(
	q *Query,
	rows []Row,
	states []aggregateMetricState,
	groupByID []uint32,
) error {

	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount {
			continue
		}
		if hasPriorOrdinaryAggregateMetric(q.metrics, i) {
			continue
		}
		if err := ae.foldGroupedOrdinaryFieldByID(q, rows, states, groupByID, i); err != nil {
			return err
		}
	}
	return nil
}

func hasPriorOrdinaryAggregateMetric(metrics []aggregateMetric, pos int) bool {
	metric := metrics[pos]
	for i := 0; i < pos; i++ {
		if aggregateMetricsShareOrdinaryField(metrics[i], metric) {
			return true
		}
	}
	return false
}

func (ae *aggregateExecutor) foldGroupedOrdinaryFieldByID(
	q *Query,
	rows []Row,
	states []aggregateMetricState,
	groupByID []uint32,
	first int,
) error {
	if len(rows) == 0 {
		return nil
	}

	counts := pooled.GetUint64Slice(len(rows))[:len(rows)]
	clear(counts)
	touched := pooled.GetIntSlice(len(rows))

	acc := q.metrics[first].field.ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	var err error
	for {
		key, bucketIDs, ok := cur.Next()
		if !ok {
			break
		}
		it := bucketIDs.Iter()
		for it.HasNext() {
			id := it.Next()
			if id >= uint64(len(groupByID)) {
				continue
			}
			groupOrdinal := groupByID[int(id)]
			if groupOrdinal == 0 {
				continue
			}
			groupIndex := int(groupOrdinal - 1)
			if counts[groupIndex] == 0 {
				touched = append(touched, groupIndex)
			}
			counts[groupIndex]++
		}
		it.Release()
		if len(touched) > 0 {
			err = addGroupedOrdinaryBucketValue(q, states, first, acc.Field, key, counts, touched)
			touched = resetAggregateGroupBucketCounts(counts, touched)
			if err != nil {
				break
			}
		}
	}

	touched = resetAggregateGroupBucketCounts(counts, touched)
	pooled.ReleaseIntSlice(touched)
	pooled.ReleaseUint64Slice(counts)
	return err
}

func addGroupedOrdinaryBucketValue(
	q *Query,
	states []aggregateMetricState,
	first int,
	field *schema.Field,
	key keycodec.IndexKey,
	counts []uint64,
	touched []int,
) error {

	var value Value
	valueReady := false

	for pos := range touched {
		groupIndex := touched[pos]
		n := counts[groupIndex]
		stateBase := groupIndex * len(q.metrics)
		for metricIdx := first; metricIdx < len(q.metrics); metricIdx++ {
			if !aggregateMetricsShareOrdinaryField(q.metrics[first], q.metrics[metricIdx]) {
				continue
			}
			state := &states[stateBase+metricIdx]
			switch state.metric.op {
			case aggregateMetricCount:
				state.count += n
				state.seen = true
			case aggregateMetricCountDistinct:
				state.count++
				state.seen = true
			default:
				if !valueReady {
					value = aggregateValueFromIndexKey(field, key)
					valueReady = true
				}
				if err := state.addValue(value, n); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func resetAggregateGroupBucketCounts(counts []uint64, touched []int) []int {
	for i := range touched {
		counts[touched[i]] = 0
	}
	return touched[:0]
}

func (ae *aggregateExecutor) aggregateGroupRecursive(
	q *Query,
	current posting.List,
	level int,
	groupValues []Value,
	rows *[]Row,
) error {

	if level == len(q.groups) {
		states := aggregateMetricStateSlicePool.Get(len(q.metrics))[:len(q.metrics)]
		for i := range q.metrics {
			states[i].metric = q.metrics[i]
		}
		if err := ae.foldAggregateMetricStates(states, current); err != nil {
			aggregateMetricStateSlicePool.Put(states)
			return err
		}
		row := make(Row, len(groupValues)+len(states))
		copy(row, groupValues)
		for i := range states {
			row[len(groupValues)+i] = states[i].finish()
		}
		aggregateMetricStateSlicePool.Put(states)
		*rows = append(*rows, row)
		return nil
	}

	acc := q.groups[level].ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	for {
		key, bucketIDs, ok := cur.Next()
		if !ok {
			break
		}
		next := current.Borrow().BuildAnd(bucketIDs)
		if next.IsEmpty() {
			next.Release()
			continue
		}
		groupValues[level] = aggregateValueFromIndexKey(acc.Field, key)
		if err := ae.aggregateGroupRecursive(q, next, level+1, groupValues, rows); err != nil {
			next.Release()
			return err
		}
		next.Release()
	}

	nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
	if !nilIDs.IsEmpty() {
		next := current.Borrow().BuildAnd(nilIDs)
		if !next.IsEmpty() {
			groupValues[level] = Value{}
			if err := ae.aggregateGroupRecursive(q, next, level+1, groupValues, rows); err != nil {
				next.Release()
				return err
			}
		}
		next.Release()
	}

	return nil
}

func (ae *aggregateExecutor) appendDistinctRows(rows *[]Row, acc schema.IndexedFieldAccessor, ids posting.List) error {
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	universe := ae.snap.Universe.Cardinality()
	filterCardinality := ids.Cardinality()
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)

	for {
		key, bucketIDs, ok := cur.Next()
		if !ok {
			break
		}
		if aggregateIntersectCardinalityKnown(ids, bucketIDs, filterCardinality, universe) == 0 {
			continue
		}
		*rows = append(*rows, Row{aggregateValueFromIndexKey(acc.Field, key)})
	}

	nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)

	if aggregateIntersectCardinalityKnown(ids, nilIDs, filterCardinality, universe) > 0 {
		*rows = append(*rows, Row{Value{}})
	}
	return nil
}

func (ae *aggregateExecutor) foldAggregateMetricStates(states []aggregateMetricState, ids posting.List) error {
	for i := range states {
		metric := states[i].metric
		if metric.rowCount {
			states[i].count = ids.Cardinality()
			states[i].seen = true
			continue
		}
		if metric.field.isMeasure {
			if err := ae.foldMeasureMetric(&states[i], ids); err != nil {
				return err
			}
			continue
		}
		if hasPriorOrdinaryAggregateMetricState(states, i) {
			continue
		}
		if err := ae.foldOrdinaryMetricStates(states, ids, i); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggregateExecutor) foldMeasureMetric(state *aggregateMetricState, ids posting.List) error {
	acc := state.metric.field.measure
	storage := ae.snap.Measure[acc.Ordinal]
	if ids.IsEmpty() || storage.Rows() == 0 {
		return nil
	}

	if ids.Cardinality() == ae.snap.Universe.Cardinality() {
		if state.metric.op == aggregateMetricCount {
			state.count += uint64(storage.Rows())
			state.seen = true
			return nil
		}
		return state.addMeasureStorageAll(storage, acc.Kind)
	}

	if useMeasureMergeScan(ids.Cardinality(), storage) {
		return state.addMeasureStorageIntersect(storage, acc.Kind, ids)
	}

	it := ids.Iter()
	for it.HasNext() {
		id := it.Next()
		raw, ok := storage.Lookup(id)
		if !ok {
			continue
		}
		if err := state.addRawMeasure(raw, acc.Kind, 1); err != nil {
			it.Release()
			return err
		}
	}
	it.Release()

	return nil
}

func useMeasureMergeScan(idCardinality uint64, storage indexdata.MeasureStorage) bool {
	lookupSteps := storage.LookupSteps()
	if lookupSteps <= 1 {
		return false
	}
	return idCardinality > uint64(storage.Rows())/(lookupSteps-1)
}

func (state *aggregateMetricState) addMeasureStorageAll(storage indexdata.MeasureStorage, kind schema.MeasureValueKind) error {
	_, values, ok := storage.FlatSlices()
	if ok {
		for i := range values {
			if err := state.addRawMeasure(values[i], kind, 1); err != nil {
				return err
			}
		}
		return nil
	}
	for chunkPos := 0; chunkPos < storage.ChunkCount(); chunkPos++ {
		_, chunkValues := storage.ChunkSlices(chunkPos)
		for i := range chunkValues {
			if err := state.addRawMeasure(chunkValues[i], kind, 1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (state *aggregateMetricState) addMeasureStorageIntersect(storage indexdata.MeasureStorage, kind schema.MeasureValueKind, ids posting.List) error {
	it := ids.Iter()
	if !it.HasNext() {
		it.Release()
		return nil
	}
	filterID := it.Next()
	measureIDs, values, ok := storage.FlatSlices()
	if ok {
		for i, measureID := range measureIDs {
			for filterID < measureID {
				if !it.HasNext() {
					it.Release()
					return nil
				}
				filterID = it.Next()
			}
			if filterID == measureID {
				if err := state.addRawMeasure(values[i], kind, 1); err != nil {
					it.Release()
					return err
				}
			}
		}
		it.Release()
		return nil
	}
	for chunkPos := 0; chunkPos < storage.ChunkCount(); chunkPos++ {
		measureIDs, values = storage.ChunkSlices(chunkPos)
		for i, measureID := range measureIDs {
			for filterID < measureID {
				if !it.HasNext() {
					it.Release()
					return nil
				}
				filterID = it.Next()
			}
			if filterID == measureID {
				if err := state.addRawMeasure(values[i], kind, 1); err != nil {
					it.Release()
					return err
				}
			}
		}
	}
	it.Release()
	return nil
}

func hasPriorOrdinaryAggregateMetricState(states []aggregateMetricState, pos int) bool {
	metric := states[pos].metric
	for i := 0; i < pos; i++ {
		if aggregateMetricsShareOrdinaryField(states[i].metric, metric) {
			return true
		}
	}
	return false
}

func aggregateMetricsShareOrdinaryField(a aggregateMetric, b aggregateMetric) bool {
	return !a.rowCount &&
		!b.rowCount &&
		!a.field.isMeasure &&
		!b.field.isMeasure &&
		a.field.ordinary.Ordinal == b.field.ordinary.Ordinal
}

func (ae *aggregateExecutor) foldOrdinaryMetricStates(states []aggregateMetricState, ids posting.List, first int) error {
	acc := states[first].metric.field.ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	universe := ae.snap.Universe.Cardinality()
	filterCardinality := ids.Cardinality()
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	for {
		key, bucketIDs, ok := cur.Next()
		if !ok {
			break
		}
		n := aggregateIntersectCardinalityKnown(ids, bucketIDs, filterCardinality, universe)
		if n == 0 {
			continue
		}
		var value Value
		valueReady := false
		for i := first; i < len(states); i++ {
			if !aggregateMetricsShareOrdinaryField(states[first].metric, states[i].metric) {
				continue
			}
			switch states[i].metric.op {

			case aggregateMetricCount:
				states[i].count += n
				states[i].seen = true

			case aggregateMetricCountDistinct:
				states[i].count++
				states[i].seen = true

			default:
				if !valueReady {
					value = aggregateValueFromIndexKey(acc.Field, key)
					valueReady = true
				}
				if err := states[i].addValue(value, n); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func aggregateIntersectCardinalityKnown(filter posting.List, bucket posting.List, filterCardinality uint64, universe uint64) uint64 {
	if filterCardinality == 0 || bucket.IsEmpty() {
		return 0
	}
	if filterCardinality == universe {
		return bucket.Cardinality()
	}
	return filter.AndCardinality(bucket)
}

func (state *aggregateMetricState) addRawMeasure(raw uint64, kind schema.MeasureValueKind, n uint64) error {
	switch kind {
	case schema.MeasureValueSigned:
		return state.addSigned(int64(raw), n)
	case schema.MeasureValueUnsigned:
		return state.addUnsigned(raw, n)
	case schema.MeasureValueFloat:
		return state.addFloat(math.Float64frombits(raw), n)
	default:
		return fmt.Errorf("%w: unsupported measure value kind", qexec.ErrInvalidQuery)
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
			return fmt.Errorf("%w: aggregate field %q has non-signed index value", qexec.ErrInvalidQuery, state.metric.field.name)
		}
		return state.addSigned(v, n)

	case aggregateValueUnsigned:
		v, ok := value.Uint()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-unsigned index value", qexec.ErrInvalidQuery, state.metric.field.name)
		}
		return state.addUnsigned(v, n)

	case aggregateValueFloat:
		v, ok := value.Float()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-float index value", qexec.ErrInvalidQuery, state.metric.field.name)
		}
		return state.addFloat(v, n)

	default:
		if state.metric.op == aggregateMetricMin || state.metric.op == aggregateMetricMax {
			state.addBest(value)
			return nil
		}
		return fmt.Errorf("%w: aggregate requires numeric field %q", qexec.ErrInvalidQuery, state.metric.field.name)
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
			return fmt.Errorf("%w: integer SUM overflow", qexec.ErrInvalidQuery)
		}
		add := int64(n) * v
		if v != 0 && add/v != int64(n) {
			return fmt.Errorf("%w: integer SUM overflow", qexec.ErrInvalidQuery)
		}
		if (add > 0 && state.intSum > math.MaxInt64-add) || (add < 0 && state.intSum < math.MinInt64-add) {
			return fmt.Errorf("%w: integer SUM overflow", qexec.ErrInvalidQuery)
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
			return fmt.Errorf("%w: unsigned SUM overflow", qexec.ErrInvalidQuery)
		}
		add := v * n
		if state.uintSum > math.MaxUint64-add {
			return fmt.Errorf("%w: unsigned SUM overflow", qexec.ErrInvalidQuery)
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
			switch state.metric.field.measure.Kind {
			case schema.MeasureValueSigned:
				return Value{num: uint64(state.intSum), any: ValueKindInt}
			case schema.MeasureValueUnsigned:
				return Value{num: state.uintSum, any: ValueKindUint}
			case schema.MeasureValueFloat:
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

func compareFloat64QuerySemantics(a, b float64) int {
	a = keycodec.CanonicalizeFloat64ForIndex(a)
	b = keycodec.CanonicalizeFloat64ForIndex(b)

	aNaN := math.IsNaN(a)
	bNaN := math.IsNaN(b)
	switch {
	case aNaN && bNaN:
		return 0
	case aNaN:
		return 1
	case bNaN:
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func aggregateValueFromIndexKey(f *schema.Field, key keycodec.IndexKey) Value {
	raw := key.U64()
	if !key.IsNumeric() {
		s := key.UnsafeString()
		if !f.UseVI && f.Kind == reflect.Bool {
			if s == "1" {
				return Value{num: 1, any: ValueKindBool}
			}
			return Value{num: 0, any: ValueKindBool}
		}
		return valueFromUnsafeString(s)
	}

	switch {
	case schema.IsNativeTimeField(f):
		return Value{num: uint64(keycodec.Int64FromOrderedKey(raw)), any: ValueKindInt}
	case isAggregateSignedKind(f.Kind):
		return Value{num: uint64(keycodec.Int64FromOrderedKey(raw)), any: ValueKindInt}
	case isAggregateUnsignedKind(f.Kind):
		return Value{num: raw, any: ValueKindUint}
	case isAggregateFloatKind(f.Kind):
		return Value{num: math.Float64bits(keycodec.Float64FromOrderedKey(raw)), any: ValueKindFloat}
	default:
		return Value{num: raw, any: ValueKindUint}
	}
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
	if (ak == ValueKindInt || ak == ValueKindUint || ak == ValueKindFloat) &&
		(bk == ValueKindInt || bk == ValueKindUint || bk == ValueKindFloat) {
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
	if (ak == ValueKindInt || ak == ValueKindUint || ak == ValueKindFloat) &&
		(bk == ValueKindInt || bk == ValueKindUint || bk == ValueKindFloat) {
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
	f = keycodec.CanonicalizeFloat64ForIndex(f)
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
	f = keycodec.CanonicalizeFloat64ForIndex(f)
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
