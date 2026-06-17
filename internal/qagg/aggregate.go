package qagg

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sort"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbitrace"
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

const aggregateGroupIDOrdinalMaxLen = 16 << 20
const aggregateFullScanMaxAvgBucketRows = 64

type aggregateGroupOrdinalMap struct {
	keys   []uint64
	values []uint32
	mask   uint64
}

func (m *aggregateGroupOrdinalMap) init(n int) {
	size := 1
	for size < n+n/3 {
		size <<= 1
	}
	if cap(m.keys) < size {
		m.keys = make([]uint64, size)
		m.values = make([]uint32, size)
	} else {
		m.keys = m.keys[:size]
		m.values = m.values[:size]
	}
	m.mask = uint64(size - 1)
}

func (m *aggregateGroupOrdinalMap) reset() {
	if cap(m.keys) > aggregateGroupOrdinalMapPoolMaxCap {
		m.keys = nil
		m.values = nil
		m.mask = 0
		return
	}
	clear(m.values)
}

func (m *aggregateGroupOrdinalMap) put(id uint64, group uint32) {
	slot := aggregateGroupOrdinalSlot(id, m.mask)
	for m.values[slot] != 0 {
		slot = (slot + 1) & m.mask
	}
	m.keys[slot] = id
	m.values[slot] = group
}

func (m *aggregateGroupOrdinalMap) get(id uint64) uint32 {
	slot := aggregateGroupOrdinalSlot(id, m.mask)
	for {
		group := m.values[slot]
		if group == 0 {
			return 0
		}
		if m.keys[slot] == id {
			return group
		}
		slot = (slot + 1) & m.mask
	}
}

func aggregateGroupOrdinalSlot(id uint64, mask uint64) uint64 {
	id ^= id >> 33
	id *= 0xff51afd7ed558ccd
	id ^= id >> 33
	return id & mask
}

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
	filter       *qir.Query
	groups       []aggregateFieldRef
	metrics      []aggregateMetric
	having       aggregateHavingExpr
	havingArgs   []aggregateHavingExpr
	havingValues []Value
	hasHaving    bool
	order        []aggregateOrder
	orderUnique  bool
	offset       uint64
	limit        uint64
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

func Execute(view *qexec.View, snap *snapshot.View, prepared *Query) (result Result, err error) {
	family := selectAggregateFamily(prepared)
	distinctWindowed := family == aggregateSelectorDistinct &&
		!prepared.hasHaving &&
		len(prepared.order) == 0 &&
		len(prepared.metrics) == 1 &&
		prepared.metrics[0].op == aggregateMetricDistinct
	exec := aggregateExecutor{snap: snap}
	var trace *qexec.Trace
	if view.TraceSamplingEnabled() {
		trace = view.BeginTrace(qir.NewShape(prepared.filter).WithWindow(prepared.offset, prepared.limit))
		if trace != nil {
			trace.SetPlan(rbitrace.PlanAggregate)
			defer func() {
				trace.Finish(uint64(len(result.Rows)), err)
			}()
		}
	}

	if family == aggregateSelectorCount {
		result, err = exec.executeCountAggregateFamily(view, prepared, trace)
	} else {
		ids, filterErr := view.Filter(prepared.filter)
		if filterErr != nil {
			return Result{}, filterErr
		}
		defer ids.Release()
		result, err = exec.executeAggregateFamily(prepared, ids, family, trace)
	}
	if err != nil {
		return Result{}, err
	}

	groupedRows := 0
	if family == aggregateSelectorGrouped {
		groupedRows = len(result.Rows)
	}
	if prepared.hasHaving {
		result = applyAggregateHaving(result, prepared.having)
	}
	if len(prepared.order) > 0 {
		result = applyAggregateOrder(result, prepared.order, prepared.orderUnique, prepared.offset, prepared.limit)
	}
	if !distinctWindowed {
		result = applyAggregateWindow(result, prepared.offset, prepared.limit)
	}
	if family == aggregateSelectorGrouped && shouldCompactAggregateRows(groupedRows, len(result.Rows)) {
		result = compactAggregateRows(result)
	}
	return result, nil
}

func (q *Query) Release() {
	aggregateQueryPool.Put(q)
}

func Prepare(src *qx.QX, s *schema.Schema, resolve qir.FieldResolver) (*Query, error) {
	if src == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if src.Reduction == nil || src.Reduction.IsEmpty() {
		return nil, fmt.Errorf("%w: aggregate query requires reduction", rbierrors.ErrInvalidQuery)
	}
	if len(src.Projection) > 0 {
		return nil, fmt.Errorf("%w: aggregate projection is not supported", rbierrors.ErrInvalidQuery)
	}
	filter, err := qir.PrepareQuery(&qx.QX{Filter: src.Filter}, resolve)
	if err != nil {
		return nil, err
	}

	out := aggregateQueryPool.Get()
	out.filter = filter
	out.offset = src.Window.Offset
	out.limit = src.Window.Limit
	if cap(out.groups) < len(src.Reduction.Group) {
		out.groups = make([]aggregateFieldRef, 0, len(src.Reduction.Group))
	}
	if cap(out.metrics) < len(src.Reduction.Metrics) {
		out.metrics = make([]aggregateMetric, 0, len(src.Reduction.Metrics))
	}
	outputPositions := aggregateOutputPositionMapPool.Get()
	defer aggregateOutputPositionMapPool.Put(outputPositions)

	for i := range src.Reduction.Group {
		group, err := prepareAggregateGroup(s, src.Reduction.Group[i])
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
		metric, err := prepareAggregateMetric(s, src.Reduction.Metrics[i])
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
			return nil, fmt.Errorf("%w: DISTINCT is supported only as a single ungrouped metric", rbierrors.ErrInvalidQuery)
		}
	}

	if len(out.metrics) == 0 && len(out.groups) == 0 {
		out.Release()
		return nil, fmt.Errorf("%w: aggregate query has no groups or metrics", rbierrors.ErrInvalidQuery)
	}

	if !src.Reduction.Having.IsZero() {
		havingArgs, havingValues, err := aggregateHavingStorageSize(src.Reduction.Having, outputPositions)
		if err != nil {
			out.Release()
			return nil, err
		}
		if cap(out.havingArgs) < havingArgs {
			out.havingArgs = make([]aggregateHavingExpr, 0, havingArgs)
		}
		if cap(out.havingValues) < havingValues {
			out.havingValues = make([]Value, 0, havingValues)
		}
		having, err := out.prepareAggregateHaving(src.Reduction.Having, outputPositions)
		if err != nil {
			out.Release()
			return nil, err
		}
		out.having = having
		out.hasHaving = true
	}

	if len(src.Order) > 0 {
		if cap(out.order) < len(src.Order) {
			out.order = make([]aggregateOrder, 0, len(src.Order))
		}
		for i := range src.Order {
			by := src.Order[i].By
			if by.Kind != qx.KindOUT || by.Name == "" {
				out.Release()
				return nil, fmt.Errorf("%w: aggregate ORDER supports only OUT references", rbierrors.ErrInvalidQuery)
			}
			if by.Value != nil {
				out.Release()
				return nil, fmt.Errorf("%w: aggregate ORDER output reference %q must not carry a value", rbierrors.ErrInvalidQuery, by.Name)
			}
			if len(by.Args) != 0 {
				out.Release()
				return nil, fmt.Errorf("%w: aggregate ORDER output reference %q must not have arguments", rbierrors.ErrInvalidQuery, by.Name)
			}
			pos, ok := outputPositions[by.Name]
			if !ok {
				out.Release()
				return nil, fmt.Errorf("%w: unknown aggregate output %q in ORDER", rbierrors.ErrInvalidQuery, by.Name)
			}
			out.order = append(out.order, aggregateOrder{index: pos, desc: src.Order[i].Desc})
		}
		if len(out.groups) > 0 {
			uniq := true
			for i := range out.groups {
				found := false
				for j := range out.order {
					if out.order[j].index == i {
						found = true
						break
					}
				}
				if !found {
					uniq = false
					break
				}
			}
			out.orderUnique = uniq
		} else if len(out.metrics) == 1 && out.metrics[0].op == aggregateMetricDistinct {
			out.orderUnique = true
		}
	}

	return out, nil
}

func reserveAggregateOutputName(seen map[string]int, name string, pos int) error {
	if _, ok := seen[name]; ok {
		return fmt.Errorf("%w: duplicate aggregate output %q", rbierrors.ErrInvalidQuery, name)
	}
	seen[name] = pos
	return nil
}

func aggregateHavingStorageSize(expr qx.Expr, outputs map[string]int) (int, int, error) {
	if expr.Kind != qx.KindOP {
		return 0, 0, fmt.Errorf("%w: aggregate HAVING root must be a predicate", rbierrors.ErrInvalidQuery)
	}
	if expr.Value != nil {
		return 0, 0, fmt.Errorf("%w: aggregate HAVING operation %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Name)
	}
	args := 0
	values := 0
	switch expr.Name {
	case qx.OpAND, qx.OpOR:
		if len(expr.Args) == 0 {
			if expr.Name == qx.OpAND {
				return 0, 0, fmt.Errorf("%w: aggregate HAVING empty AND expression", rbierrors.ErrInvalidQuery)
			}
			return 0, 0, fmt.Errorf("%w: aggregate HAVING empty OR expression", rbierrors.ErrInvalidQuery)
		}
		args += len(expr.Args)
		for i := range expr.Args {
			childArgs, childValues, err := aggregateHavingStorageSize(expr.Args[i], outputs)
			if err != nil {
				return 0, 0, err
			}
			args += childArgs
			values += childValues
		}
	case qx.OpNOT:
		if len(expr.Args) != 1 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING NOT expects one predicate", rbierrors.ErrInvalidQuery)
		}
		args++
		childArgs, childValues, err := aggregateHavingStorageSize(expr.Args[0], outputs)
		if err != nil {
			return 0, 0, err
		}
		args += childArgs
		values += childValues
	case qx.OpEXISTS, qx.OpISNULL:
		if len(expr.Args) != 1 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING %s expects one OUT reference", rbierrors.ErrInvalidQuery, expr.Name)
		}
		if _, err := prepareAggregateHavingOutput(expr.Args[0], outputs); err != nil {
			return 0, 0, err
		}
	case qx.OpEQ, qx.OpNE, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		if len(expr.Args) != 2 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING %s expects OUT and literal", rbierrors.ErrInvalidQuery, expr.Name)
		}
		if _, err := prepareAggregateHavingOutput(expr.Args[0], outputs); err != nil {
			return 0, 0, err
		}
		if expr.Args[1].Kind != qx.KindLIT {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING right side supports only literals", rbierrors.ErrInvalidQuery)
		}
		if expr.Args[1].Name != "" {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING literal must not define a name", rbierrors.ErrInvalidQuery)
		}
		if len(expr.Args[1].Args) != 0 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING literal must not have arguments", rbierrors.ErrInvalidQuery)
		}
	case qx.OpIN:
		if len(expr.Args) != 2 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN expects OUT and literal slice", rbierrors.ErrInvalidQuery)
		}
		if _, err := prepareAggregateHavingOutput(expr.Args[0], outputs); err != nil {
			return 0, 0, err
		}
		if expr.Args[1].Kind != qx.KindLIT {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", rbierrors.ErrInvalidQuery)
		}
		if expr.Args[1].Name != "" {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN literal slice must not define a name", rbierrors.ErrInvalidQuery)
		}
		if len(expr.Args[1].Args) != 0 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN literal slice must not have arguments", rbierrors.ErrInvalidQuery)
		}
		raw := reflect.ValueOf(expr.Args[1].Value)
		if !raw.IsValid() {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", rbierrors.ErrInvalidQuery)
		}
		raw, isNil := schema.UnwrapQueryValue(raw)
		if isNil || raw.Kind() != reflect.Slice {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", rbierrors.ErrInvalidQuery)
		}
		if raw.Len() == 0 {
			return 0, 0, fmt.Errorf("%w: aggregate HAVING IN: no values provided", rbierrors.ErrInvalidQuery)
		}
		values += raw.Len()
	default:
		return 0, 0, fmt.Errorf("%w: aggregate HAVING supports only simple OUT predicates", rbierrors.ErrInvalidQuery)
	}
	return args, values, nil
}

func (q *Query) prepareAggregateHaving(expr qx.Expr, outputs map[string]int) (aggregateHavingExpr, error) {
	if expr.Kind != qx.KindOP {
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING root must be a predicate", rbierrors.ErrInvalidQuery)
	}
	if expr.Value != nil {
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING operation %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Name)
	}
	switch expr.Name {

	case qx.OpAND, qx.OpOR:
		start := len(q.havingArgs)
		q.havingArgs = q.havingArgs[:start+len(expr.Args)]
		args := q.havingArgs[start : start+len(expr.Args)]
		for i := range expr.Args {
			arg, err := q.prepareAggregateHaving(expr.Args[i], outputs)
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
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING NOT expects one predicate", rbierrors.ErrInvalidQuery)
		}
		start := len(q.havingArgs)
		q.havingArgs = q.havingArgs[:start+1]
		args := q.havingArgs[start : start+1]
		arg, err := q.prepareAggregateHaving(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		args[0] = arg
		return aggregateHavingExpr{op: aggregateHavingNot, args: args}, nil

	case qx.OpEXISTS, qx.OpISNULL:
		if len(expr.Args) != 1 {
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING %s expects one OUT reference", rbierrors.ErrInvalidQuery, expr.Name)
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
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING %s expects OUT and literal", rbierrors.ErrInvalidQuery, expr.Name)
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
			return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING IN expects OUT and literal slice", rbierrors.ErrInvalidQuery)
		}
		pos, err := prepareAggregateHavingOutput(expr.Args[0], outputs)
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		values, err := q.aggregateLiteralValueSlice(expr.Args[1])
		if err != nil {
			return aggregateHavingExpr{}, err
		}
		return aggregateHavingExpr{op: aggregateHavingIN, index: pos, values: values}, nil

	default:
		return aggregateHavingExpr{}, fmt.Errorf("%w: aggregate HAVING supports only simple OUT predicates", rbierrors.ErrInvalidQuery)
	}
}

func prepareAggregateHavingOutput(expr qx.Expr, outputs map[string]int) (int, error) {
	if expr.Kind != qx.KindOUT || expr.Name == "" {
		return 0, fmt.Errorf("%w: aggregate HAVING left side supports only OUT references", rbierrors.ErrInvalidQuery)
	}
	if expr.Value != nil {
		return 0, fmt.Errorf("%w: aggregate HAVING output reference %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Name)
	}
	if len(expr.Args) != 0 {
		return 0, fmt.Errorf("%w: aggregate HAVING output reference %q must not have arguments", rbierrors.ErrInvalidQuery, expr.Name)
	}
	pos, ok := outputs[expr.Name]
	if !ok {
		return 0, fmt.Errorf("%w: unknown aggregate output %q in HAVING", rbierrors.ErrInvalidQuery, expr.Name)
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
		return Value{}, fmt.Errorf("%w: aggregate HAVING right side supports only literals", rbierrors.ErrInvalidQuery)
	}
	if expr.Name != "" {
		return Value{}, fmt.Errorf("%w: aggregate HAVING literal must not define a name", rbierrors.ErrInvalidQuery)
	}
	if len(expr.Args) != 0 {
		return Value{}, fmt.Errorf("%w: aggregate HAVING literal must not have arguments", rbierrors.ErrInvalidQuery)
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
		return Value{}, fmt.Errorf("%w: unsupported aggregate HAVING literal type %T", rbierrors.ErrInvalidQuery, raw)
	}
}

func (q *Query) aggregateLiteralValueSlice(expr qx.Expr) ([]Value, error) {
	if expr.Kind != qx.KindLIT {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", rbierrors.ErrInvalidQuery)
	}
	if expr.Name != "" {
		return nil, fmt.Errorf("%w: aggregate HAVING IN literal slice must not define a name", rbierrors.ErrInvalidQuery)
	}
	if len(expr.Args) != 0 {
		return nil, fmt.Errorf("%w: aggregate HAVING IN literal slice must not have arguments", rbierrors.ErrInvalidQuery)
	}
	raw := reflect.ValueOf(expr.Value)
	if !raw.IsValid() {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", rbierrors.ErrInvalidQuery)
	}
	raw, isNil := schema.UnwrapQueryValue(raw)
	if isNil || raw.Kind() != reflect.Slice {
		return nil, fmt.Errorf("%w: aggregate HAVING IN right side supports only literal slices", rbierrors.ErrInvalidQuery)
	}
	if raw.Len() == 0 {
		return nil, fmt.Errorf("%w: aggregate HAVING IN: no values provided", rbierrors.ErrInvalidQuery)
	}

	start := len(q.havingValues)
	q.havingValues = q.havingValues[:start+raw.Len()]
	values := q.havingValues[start : start+raw.Len()]
	for i := 0; i < raw.Len(); i++ {
		value, err := aggregateLiteralRawValue(raw.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func prepareAggregateGroup(s *schema.Schema, expr qx.Expr) (aggregateFieldRef, error) {
	if expr.Kind != qx.KindREF || expr.Name == "" {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY supports only field references", rbierrors.ErrInvalidQuery)
	}
	if expr.Value != nil {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY field reference %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Name)
	}
	if len(expr.Args) != 0 {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY field reference %q must not have arguments", rbierrors.ErrInvalidQuery, expr.Name)
	}
	if expr.Name == schema.ReservedKeyFieldName {
		return aggregateFieldRef{}, fmt.Errorf("%w: %v is not supported in GROUP BY", rbierrors.ErrInvalidQuery, schema.ReservedKeyFieldName)
	}

	acc, ok := s.IndexedByName[expr.Name]
	if !ok {
		if _, measure := s.MeasuresByName[expr.Name]; measure {
			return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY measure field %q is not supported", rbierrors.ErrInvalidQuery, expr.Name)
		}
		return aggregateFieldRef{}, fmt.Errorf("%w: no index for group field %q", rbierrors.ErrInvalidQuery, expr.Name)
	}

	if acc.Field.Slice {
		return aggregateFieldRef{}, fmt.Errorf("%w: GROUP BY slice field %q is not supported", rbierrors.ErrInvalidQuery, expr.Name)
	}

	out := expr.Alias
	if out == "" {
		out = expr.Name
	}

	return aggregateFieldRef{name: expr.Name, out: out, ordinary: acc, kind: aggregateFieldValueKind(acc.Field)}, nil
}

func prepareAggregateMetric(s *schema.Schema, expr qx.Expr) (aggregateMetric, error) {
	if expr.Kind != qx.KindOP {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric must be an operation", rbierrors.ErrInvalidQuery)
	}
	if expr.Value != nil {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric operation %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Name)
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
			if expr.Args[0].Value != nil {
				return aggregateMetric{}, fmt.Errorf("%w: COUNT(DISTINCT) operation must not carry a value", rbierrors.ErrInvalidQuery)
			}
			if len(expr.Args[0].Args) != 1 || expr.Args[0].Args[0].Kind != qx.KindREF || expr.Args[0].Args[0].Name == "" {
				return aggregateMetric{}, fmt.Errorf("%w: COUNT(DISTINCT) supports only direct field reference", rbierrors.ErrInvalidQuery)
			}
			if expr.Args[0].Args[0].Value != nil {
				return aggregateMetric{}, fmt.Errorf("%w: COUNT(DISTINCT) field reference %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Args[0].Args[0].Name)
			}
			if len(expr.Args[0].Args[0].Args) != 0 {
				return aggregateMetric{}, fmt.Errorf("%w: COUNT(DISTINCT) field reference %q must not have arguments", rbierrors.ErrInvalidQuery, expr.Args[0].Args[0].Name)
			}
			field, err := prepareAggregateMetricField(s, expr.Args[0].Args[0].Name, metric.op)
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
		return aggregateMetric{}, fmt.Errorf("%w: unsupported aggregate metric %q", rbierrors.ErrInvalidQuery, expr.Name)
	}

	if len(expr.Args) != 1 || expr.Args[0].Kind != qx.KindREF || expr.Args[0].Name == "" {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric %q supports only direct field reference", rbierrors.ErrInvalidQuery, expr.Name)
	}
	if expr.Args[0].Value != nil {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric %q field reference %q must not carry a value", rbierrors.ErrInvalidQuery, expr.Name, expr.Args[0].Name)
	}
	if len(expr.Args[0].Args) != 0 {
		return aggregateMetric{}, fmt.Errorf("%w: aggregate metric %q field reference %q must not have arguments", rbierrors.ErrInvalidQuery, expr.Name, expr.Args[0].Name)
	}

	f, err := prepareAggregateMetricField(s, expr.Args[0].Name, metric.op)
	if err != nil {
		return aggregateMetric{}, err
	}

	metric.field = f
	if metric.out == "" {
		metric.out = defaultAggregateMetricName(metric.op, f.name)
	}
	return metric, nil
}

func prepareAggregateMetricField(s *schema.Schema, name string, op aggregateMetricOp) (aggregateFieldRef, error) {
	if name == schema.ReservedKeyFieldName {
		return aggregateFieldRef{}, fmt.Errorf("%w: %v is not supported in aggregate metric", rbierrors.ErrInvalidQuery, schema.ReservedKeyFieldName)
	}
	if acc, ok := s.MeasuresByName[name]; ok {
		if op == aggregateMetricDistinct || op == aggregateMetricCountDistinct {
			return aggregateFieldRef{}, fmt.Errorf("%w: DISTINCT over measure field %q is not supported", rbierrors.ErrInvalidQuery, name)
		}
		return aggregateFieldRef{name: name, measure: acc, isMeasure: true, kind: aggregateMeasureValueKind(acc.Kind)}, nil
	}
	acc, ok := s.IndexedByName[name]
	if !ok {
		return aggregateFieldRef{}, fmt.Errorf("%w: no index for aggregate field %q", rbierrors.ErrInvalidQuery, name)
	}
	if acc.Field.Slice {
		return aggregateFieldRef{}, fmt.Errorf("%w: aggregate over slice field %q is not supported", rbierrors.ErrInvalidQuery, name)
	}

	kind := aggregateFieldValueKind(acc.Field)
	if op == aggregateMetricSum || op == aggregateMetricAvg {
		if acc.Field.UseVI ||
			(!isAggregateSignedKind(acc.Field.Kind) &&
				!isAggregateUnsignedKind(acc.Field.Kind) &&
				!isAggregateFloatKind(acc.Field.Kind)) {
			return aggregateFieldRef{}, fmt.Errorf("%w: %s requires numeric field %q", rbierrors.ErrInvalidQuery, aggregateMetricOpName(op), name)
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
	return ae.executeAggregateFamily(q, ids, selectAggregateFamily(q), nil)
}

func (ae *aggregateExecutor) executeAggregateFamily(q *Query, ids posting.List, family aggregateSelectorFamily, trace *qexec.Trace) (Result, error) {
	var decision aggregateRouteDecision
	switch family {
	case aggregateSelectorCount:
		decision = selectCountAggregate()
		count := ids.Cardinality()
		decision.expectedFilterRows = count
		decision.selectedCost = float64(count)
		if trace != nil {
			trace.SetAggregateRoute(decision.traceRoute())
		}
		return aggregateCountResult(q.metrics[0].out, count), nil
	case aggregateSelectorDistinct:
		decision = selectDistinctAggregate(ae.collectDistinctFacts(q, ids))
	case aggregateSelectorUngrouped:
		decision = selectUngroupedAggregate(ae.collectUngroupedFacts(q, ids))
	case aggregateSelectorGrouped:
		decision = selectGroupedAggregate(ae.collectGroupedFacts(q, ids))
	default:
		return Result{}, dispatchInvalidAggregateRoute(0)
	}
	if trace != nil {
		trace.SetAggregateRoute(decision.traceRoute())
	}
	return ae.dispatchAggregateRoute(q, ids, decision)
}

func (ae *aggregateExecutor) executeCountAggregateFamily(view *qexec.View, q *Query, trace *qexec.Trace) (Result, error) {
	count, err := Count(view, q.filter, false)
	if err != nil {
		return Result{}, err
	}
	decision := selectCountAggregate()
	decision.expectedFilterRows = count
	decision.selectedCost = float64(count)
	if trace != nil {
		trace.SetAggregateRoute(decision.traceRoute())
	}
	return aggregateCountResult(q.metrics[0].out, count), nil
}

func aggregateCountResult(out string, count uint64) Result {
	return Result{
		Layout: []string{out},
		Rows: []Row{{
			Value{num: count, any: ValueKindUint},
		}},
	}
}

func (ae *aggregateExecutor) dispatchAggregateRoute(q *Query, ids posting.List, decision aggregateRouteDecision) (Result, error) {
	switch decision.route {
	case aggregateRouteDistinctUngrouped:
		return ae.executeDistinctAggregate(q, ids)
	case aggregateRouteCountDistinctUngrouped:
		return ae.executeCountDistinctAggregate(q.metrics[0], ids)
	case aggregateRouteUngroupedOrdinary, aggregateRouteUngroupedMeasure, aggregateRouteUngroupedHybrid:
		return ae.executeUngroupedAggregate(q, ids, decision)
	case aggregateRouteGroupedRecursive:
		return ae.executeGroupedRecursiveAggregate(q, ids)
	case aggregateRouteGroupedMeasure, aggregateRouteGroupedHybrid:
		return ae.executeGroupedLookupAggregate(q, ids, decision)
	case aggregateRouteGroupedOrdinaryByID:
		return ae.executeGroupedOrdinaryByID(q, ids, decision)
	default:
		return Result{}, dispatchInvalidAggregateRoute(decision.route)
	}
}

func (ae *aggregateExecutor) executeUngroupedAggregate(q *Query, ids posting.List, decision aggregateRouteDecision) (Result, error) {
	layout := make([]string, len(q.metrics))
	states := aggregateMetricStateSlicePool.Get(len(q.metrics))[:len(q.metrics)]
	empty := ids.IsEmpty()
	for i := range q.metrics {
		layout[i] = q.metrics[i].out
		states[i].metric = q.metrics[i]
	}
	if !empty {
		err := ae.foldAggregateMetricStates(states, ids, decision.measureMode)
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

func appendGroupedRow(rows []Row, values []Value, groupValues []Value, metricCount int) ([]Row, []Value, Row) {
	width := len(groupValues) + metricCount
	if cap(values)-len(values) < width {
		newCap := cap(values) * 2
		if newCap < width {
			newCap = width
		}
		values = make([]Value, 0, newCap)
	}
	start := len(values)
	rowValues := values[:start+width]
	values = rowValues
	row := rowValues[start : start+width : start+width]
	copy(row, groupValues)
	rows = append(rows, row)
	return rows, values, row
}

func (ae *aggregateExecutor) executeDistinctAggregate(q *Query, ids posting.List) (Result, error) {
	metric := q.metrics[0]
	layout := []string{metric.out}
	offset := uint64(0)
	limit := uint64(0)
	if !q.hasHaving && len(q.order) == 0 {
		offset = q.offset
		limit = q.limit
	}
	window := offset != 0 || limit != 0
	if ids.IsEmpty() {
		if window {
			return Result{Layout: layout}, nil
		}
		return Result{Layout: layout, Rows: make([]Row, 0)}, nil
	}
	acc := metric.field.ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
	filterCardinality := ids.Cardinality()
	universe := ae.snap.Universe.Cardinality()
	keyCount := ov.KeyCount()
	if filterCardinality == universe {
		rowCount := keyCount
		if !nilIDs.IsEmpty() {
			rowCount++
		}
		if window {
			if offset >= uint64(rowCount) {
				return Result{Layout: layout}, nil
			}
			start := int(offset)
			end := rowCount
			if limit > 0 && limit < uint64(end-start) {
				end = start + int(limit)
			}
			rows := make([]Row, end-start)
			values := make([]Value, end-start)
			keyStart := start
			if keyStart > keyCount {
				keyStart = keyCount
			}
			keyEnd := end
			if keyEnd > keyCount {
				keyEnd = keyCount
			}
			row := 0
			cur := ov.NewCursor(ov.RangeByRanks(keyStart, keyEnd), false)
			for {
				key, _, ok := cur.Next()
				if !ok {
					break
				}
				values[row] = aggregateValueFromIndexKey(acc.Field, key)
				rows[row] = values[row : row+1 : row+1]
				row++
			}
			if !nilIDs.IsEmpty() && end > keyCount {
				rows[row] = values[row : row+1 : row+1]
			}
			return Result{Layout: layout, Rows: rows}, nil
		}
		rows := make([]Row, rowCount)
		values := make([]Value, rowCount)
		row := 0
		cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
		for {
			key, _, ok := cur.Next()
			if !ok {
				break
			}
			values[row] = aggregateValueFromIndexKey(acc.Field, key)
			rows[row] = values[row : row+1 : row+1]
			row++
		}
		if !nilIDs.IsEmpty() {
			rows[row] = values[row : row+1 : row+1]
		}
		return Result{Layout: layout, Rows: rows}, nil
	}

	rowCap := min(uint64(keyCount)+1, filterCardinality+1)
	if window && offset >= rowCap {
		return Result{Layout: layout}, nil
	}
	resultCap := rowCap
	if window {
		resultCap -= offset
		if limit > 0 && limit < resultCap {
			resultCap = limit
		}
	}
	singleBuckets := uint64(keyCount) > rowCap && ov.Rows() == uint64(keyCount)

	if resultCap < 64 {
		rows := make([]Row, 0)
		rows, _, matched := ae.appendDistinctRows(rows, nil, metric.field.ordinary, ids, ov, nilIDs, filterCardinality, universe, singleBuckets, offset, limit)
		if window && len(rows) == 0 && matched <= offset {
			rows = nil
		}
		return Result{Layout: layout, Rows: rows}, nil
	}

	rows := make([]Row, 0, int(resultCap))
	values := make([]Value, 0, int(resultCap))
	rows, _, matched := ae.appendDistinctRows(rows, values, metric.field.ordinary, ids, ov, nilIDs, filterCardinality, universe, singleBuckets, offset, limit)
	if window && len(rows) == 0 && matched <= offset {
		rows = nil
	}
	return Result{Layout: layout, Rows: rows}, nil
}

func (ae *aggregateExecutor) executeCountDistinctAggregate(metric aggregateMetric, ids posting.List) (Result, error) {
	count := uint64(0)
	if !ids.IsEmpty() {
		acc := metric.field.ordinary
		ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
		universe := ae.snap.Universe.Cardinality()
		filterCardinality := ids.Cardinality()
		if filterCardinality == universe {
			count = uint64(ov.KeyCount())
		} else if ov.Rows() == uint64(ov.KeyCount()) {
			count = filterCardinality
			nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
			if !nilIDs.IsEmpty() {
				count -= aggregateIntersectCardinalityKnown(ids, nilIDs, filterCardinality, universe)
			}
		} else {
			cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
			for {
				_, bucketIDs, ok := cur.Next()
				if !ok {
					break
				}
				if aggregateIntersectCardinalityKnown(ids, bucketIDs, filterCardinality, universe) > 0 {
					count++
				}
			}
		}
	}
	return aggregateCountResult(metric.out, count), nil
}

func (ae *aggregateExecutor) executeGroupedRecursiveAggregate(q *Query, ids posting.List) (Result, error) {
	layout := groupedAggregateLayout(q)
	if ids.IsEmpty() {
		return Result{Layout: layout, Rows: make([]Row, 0)}, nil
	}
	rows := make([]Row, 0)
	rowValues := make([]Value, 0)
	groupValues := make([]Value, len(q.groups))
	rowCountOnly := true
	for i := range q.metrics {
		if !q.metrics[i].rowCount {
			rowCountOnly = false
			break
		}
	}
	var err error
	rows, _, err = ae.executeGroupedRecursive(q, ids, 0, groupValues, rows, rowValues, rowCountOnly)
	if err != nil {
		return Result{}, err
	}
	return Result{Layout: layout, Rows: rows}, nil
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

func (ae *aggregateExecutor) executeGroupedOrdinaryByID(q *Query, ids posting.List, decision aggregateRouteDecision) (Result, error) {
	layout := groupedAggregateLayout(q)
	rows := make([]Row, 0)
	rowValues := make([]Value, 0)
	groupValues := make([]Value, len(q.groups))
	var states []aggregateMetricState

	var err error
	switch decision.groupLookup {
	case aggregateGroupLookupOrdinalSlice:
		groupByIDLen := int(decision.groupMapLen)
		groupByID := pooled.GetUint32Slice(groupByIDLen)[:groupByIDLen]
		clear(groupByID)
		rows, _, err = ae.buildGroupedIDMap(q, ids, 0, groupValues, rows, rowValues, groupByID, nil)
		if err == nil {
			states = initGroupedMetricStates(q, rows)
			err = ae.foldGroupedOrdinaryByID(q, rows, states, groupByID)
		}
		pooled.ReleaseUint32Slice(groupByID)

	case aggregateGroupLookupMap:
		groupByID := aggregateGroupOrdinalMapPool.Get()
		groupByID.init(int(decision.groupMapLen))
		rows, _, err = ae.buildGroupedIDMap(q, ids, 0, groupValues, rows, rowValues, nil, groupByID)
		if err == nil {
			states = initGroupedMetricStates(q, rows)
			err = ae.foldGroupedOrdinaryByIDMap(q, rows, states, groupByID)
		}
		aggregateGroupOrdinalMapPool.Put(groupByID)

	default:
		return Result{}, dispatchInvalidAggregateRoute(decision.route)
	}

	if err != nil {
		if states != nil {
			aggregateMetricStateSlicePool.Put(states)
		}
		return Result{}, err
	}
	finishGroupedMetricStates(q, rows, states)
	aggregateMetricStateSlicePool.Put(states)

	return Result{Layout: layout, Rows: rows}, nil
}

func (ae *aggregateExecutor) executeGroupedLookupAggregate(q *Query, ids posting.List, decision aggregateRouteDecision) (Result, error) {
	layout := groupedAggregateLayout(q)
	if ids.IsEmpty() {
		return Result{Layout: layout, Rows: make([]Row, 0)}, nil
	}

	rows := make([]Row, 0)
	rowValues := make([]Value, 0)
	groupValues := make([]Value, len(q.groups))
	var states []aggregateMetricState

	var err error
	switch decision.groupLookup {
	case aggregateGroupLookupOrdinalSlice:
		groupByIDLen := int(decision.groupMapLen)
		groupByID := pooled.GetUint32Slice(groupByIDLen)[:groupByIDLen]
		clear(groupByID)
		rows, _, err = ae.buildGroupedIDMap(q, ids, 0, groupValues, rows, rowValues, groupByID, nil)
		if err == nil {
			states = initGroupedMetricStates(q, rows)
			err = ae.foldGroupedAggregateByID(q, ids, rows, states, groupByID, decision.measureMode)
		}
		pooled.ReleaseUint32Slice(groupByID)

	default:
		return Result{}, dispatchInvalidAggregateRoute(decision.route)
	}

	if err != nil {
		if states != nil {
			aggregateMetricStateSlicePool.Put(states)
		}
		return Result{}, err
	}
	finishGroupedMetricStates(q, rows, states)
	aggregateMetricStateSlicePool.Put(states)

	return Result{Layout: layout, Rows: rows}, nil
}

func (ae *aggregateExecutor) buildGroupedIDMap(
	q *Query,
	current posting.List,
	level int,
	groupValues []Value,
	rows []Row,
	rowValues []Value,
	groupByID []uint32,
	groupByIDMap *aggregateGroupOrdinalMap,
) ([]Row, []Value, error) {
	if level == len(q.groups) {
		rowIndex := len(rows)
		if uint64(rowIndex) >= uint64(^uint32(0)) {
			return rows, rowValues, fmt.Errorf("%w: aggregate group count exceeds runtime limit", rbierrors.ErrInvalidQuery)
		}
		rows, rowValues, row := appendGroupedRow(rows, rowValues, groupValues, len(q.metrics))

		if len(q.metrics) != 0 {
			rowCount := current.Cardinality()
			for i := range q.metrics {
				if q.metrics[i].rowCount {
					row[len(q.groups)+i] = Value{num: rowCount, any: ValueKindUint}
				}
			}
		}

		groupOrdinal := uint32(rowIndex + 1)
		it := current.Iter()
		if groupByID != nil {
			for it.HasNext() {
				id := it.Next()
				groupByID[int(id)] = groupOrdinal
			}
		} else {
			for it.HasNext() {
				groupByIDMap.put(it.Next(), groupOrdinal)
			}
		}
		it.Release()
		return rows, rowValues, nil
	}

	acc := q.groups[level].ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	keyCount := ov.KeyCount()
	cur := ov.NewCursor(ov.RangeByRanks(0, keyCount), false)
	useSliceLeaf := groupByID != nil && level+1 == len(q.groups)
	if useSliceLeaf && level > 0 && keyCount > 0 && ov.Rows()/uint64(keyCount) < uint64(indexdata.FieldChunkTargetEntries) {
		useSliceLeaf = false
	}
	if useSliceLeaf {
		for {
			key, bucketIDs, ok := cur.Next()
			if !ok {
				break
			}
			var err error
			rows, rowValues, err = appendGroupedIDSliceLeaf(q, current, bucketIDs, groupValues, rows, rowValues, groupByID, acc.Field, key, true)
			if err != nil {
				return rows, rowValues, err
			}
		}

		nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
		if !nilIDs.IsEmpty() {
			groupValues[level] = Value{}
			var err error
			rows, rowValues, err = appendGroupedIDSliceLeaf(q, current, nilIDs, groupValues, rows, rowValues, groupByID, nil, keycodec.IndexKey{}, false)
			if err != nil {
				return rows, rowValues, err
			}
		}
		return rows, rowValues, nil
	}

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
		var err error
		rows, rowValues, err = ae.buildGroupedIDMap(q, next, level+1, groupValues, rows, rowValues, groupByID, groupByIDMap)
		if err != nil {
			next.Release()
			return rows, rowValues, err
		}
		next.Release()
	}

	nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
	if !nilIDs.IsEmpty() {
		groupValues[level] = Value{}
		if groupByID != nil && level+1 == len(q.groups) && nilIDs.Cardinality() >= uint64(indexdata.FieldChunkTargetEntries) {
			var err error
			rows, rowValues, err = appendGroupedIDSliceLeaf(q, current, nilIDs, groupValues, rows, rowValues, groupByID, nil, keycodec.IndexKey{}, false)
			if err != nil {
				return rows, rowValues, err
			}
			return rows, rowValues, nil
		}
		next := current.Borrow().BuildAnd(nilIDs)
		if !next.IsEmpty() {
			var err error
			rows, rowValues, err = ae.buildGroupedIDMap(q, next, level+1, groupValues, rows, rowValues, groupByID, groupByIDMap)
			if err != nil {
				next.Release()
				return rows, rowValues, err
			}
		}
		next.Release()
	}

	return rows, rowValues, nil
}

func appendGroupedIDSliceLeaf(
	q *Query,
	current posting.List,
	bucket posting.List,
	groupValues []Value,
	rows []Row,
	rowValues []Value,
	groupByID []uint32,
	field *schema.Field,
	key keycodec.IndexKey,
	hasKey bool,
) ([]Row, []Value, error) {
	rowIndex := -1
	groupOrdinal := uint32(0)
	rowCount := uint64(0)
	overflow := false

	current.ForEachIntersecting(bucket, func(id uint64) bool {
		if rowIndex < 0 {
			rowIndex = len(rows)
			if uint64(rowIndex) >= uint64(^uint32(0)) {
				overflow = true
				return true
			}
			if hasKey {
				groupValues[len(groupValues)-1] = aggregateValueFromIndexKey(field, key)
			}
			rows, rowValues, _ = appendGroupedRow(rows, rowValues, groupValues, len(q.metrics))
			groupOrdinal = uint32(rowIndex + 1)
		}
		rowCount++
		groupByID[int(id)] = groupOrdinal
		return false
	})
	if overflow {
		return rows, rowValues, fmt.Errorf("%w: aggregate group count exceeds runtime limit", rbierrors.ErrInvalidQuery)
	}
	if rowIndex >= 0 {
		row := rows[rowIndex]
		for i := range q.metrics {
			if q.metrics[i].rowCount {
				row[len(q.groups)+i] = Value{num: rowCount, any: ValueKindUint}
			}
		}
	}
	return rows, rowValues, nil
}

func initGroupedMetricStates(q *Query, rows []Row) []aggregateMetricState {
	states := aggregateMetricStateSlicePool.Get(len(rows) * len(q.metrics))[:len(rows)*len(q.metrics)]
	for rowIdx := range rows {
		base := rowIdx * len(q.metrics)
		for metricIdx := range q.metrics {
			states[base+metricIdx].metric = q.metrics[metricIdx]
		}
	}
	return states
}

func finishGroupedMetricStates(q *Query, rows []Row, states []aggregateMetricState) {
	for rowIdx := range rows {
		base := rowIdx * len(q.metrics)
		for metricIdx := range q.metrics {
			if q.metrics[metricIdx].rowCount {
				continue
			}
			rows[rowIdx][len(q.groups)+metricIdx] = states[base+metricIdx].finish()
		}
	}
}

func (ae *aggregateExecutor) foldGroupedAggregateByID(
	q *Query,
	ids posting.List,
	rows []Row,
	states []aggregateMetricState,
	groupByID []uint32,
	measureMode aggregateMeasureAccess,
) error {
	if err := ae.foldGroupedOrdinaryByID(q, rows, states, groupByID); err != nil {
		return err
	}
	return ae.foldGroupedMeasureByID(q, ids, states, groupByID, measureMode)
}

func (ae *aggregateExecutor) foldGroupedOrdinaryByID(
	q *Query,
	rows []Row,
	states []aggregateMetricState,
	groupByID []uint32,
) error {

	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount || metric.field.isMeasure {
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

func (ae *aggregateExecutor) foldGroupedOrdinaryByIDMap(
	q *Query,
	rows []Row,
	states []aggregateMetricState,
	groupByID *aggregateGroupOrdinalMap,
) error {

	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount || metric.field.isMeasure {
			continue
		}
		if hasPriorOrdinaryAggregateMetric(q.metrics, i) {
			continue
		}
		if err := ae.foldGroupedOrdinaryFieldByIDMap(q, rows, states, groupByID, i); err != nil {
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

func hasPriorMeasureAggregateMetric(metrics []aggregateMetric, pos int) bool {
	metric := metrics[pos]
	for i := 0; i < pos; i++ {
		if aggregateMetricsShareMeasureField(metrics[i], metric) {
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
		key, bucketIDs, id, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			break
		}
		if single {
			if id >= uint64(len(groupByID)) {
				continue
			}
			groupOrdinal := groupByID[int(id)]
			if groupOrdinal == 0 {
				continue
			}
			groupIndex := int(groupOrdinal - 1)
			stateBase := groupIndex * len(q.metrics)
			var value Value
			valueReady := false
		LOOP:
			for metricIdx := first; metricIdx < len(q.metrics); metricIdx++ {
				if !aggregateMetricsShareOrdinaryField(q.metrics[first], q.metrics[metricIdx]) {
					continue
				}
				state := &states[stateBase+metricIdx]

				switch state.metric.op {

				case aggregateMetricCount:
					state.count++
					state.seen = true

				case aggregateMetricCountDistinct:
					state.count++
					state.seen = true

				case aggregateMetricSum, aggregateMetricAvg:
					if err = state.addIndexKey(key, 1); err != nil {
						break LOOP
					}

				default:
					if !valueReady {
						value = aggregateValueFromIndexKey(acc.Field, key)
						valueReady = true
					}
					if err = state.addValue(value, 1); err != nil {
						break LOOP
					}
				}
			}
			if err != nil {
				break
			}
			continue
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

func (ae *aggregateExecutor) foldGroupedOrdinaryFieldByIDMap(
	q *Query,
	rows []Row,
	states []aggregateMetricState,
	groupByID *aggregateGroupOrdinalMap,
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
		key, bucketIDs, id, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			break
		}
		if single {
			groupOrdinal := groupByID.get(id)
			if groupOrdinal == 0 {
				continue
			}
			groupIndex := int(groupOrdinal - 1)
			stateBase := groupIndex * len(q.metrics)
			var value Value
			valueReady := false
		LOOP:
			for metricIdx := first; metricIdx < len(q.metrics); metricIdx++ {
				if !aggregateMetricsShareOrdinaryField(q.metrics[first], q.metrics[metricIdx]) {
					continue
				}
				state := &states[stateBase+metricIdx]

				switch state.metric.op {

				case aggregateMetricCount:
					state.count++
					state.seen = true

				case aggregateMetricCountDistinct:
					state.count++
					state.seen = true

				case aggregateMetricSum, aggregateMetricAvg:
					if err = state.addIndexKey(key, 1); err != nil {
						break LOOP
					}

				default:
					if !valueReady {
						value = aggregateValueFromIndexKey(acc.Field, key)
						valueReady = true
					}
					if err = state.addValue(value, 1); err != nil {
						break LOOP
					}
				}
			}
			if err != nil {
				break
			}
			continue
		}
		it := bucketIDs.Iter()
		for it.HasNext() {
			groupOrdinal := groupByID.get(it.Next())
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

func (ae *aggregateExecutor) foldGroupedMeasureByID(
	q *Query,
	ids posting.List,
	states []aggregateMetricState,
	groupByID []uint32,
	measureMode aggregateMeasureAccess,
) error {
	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount || !metric.field.isMeasure {
			continue
		}
		if hasPriorMeasureAggregateMetric(q.metrics, i) {
			continue
		}
		if err := ae.foldGroupedMeasureFieldByID(q, ids, states, groupByID, i, measureMode); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggregateExecutor) foldGroupedMeasureFieldByID(
	q *Query,
	ids posting.List,
	states []aggregateMetricState,
	groupByID []uint32,
	first int,
	measureMode aggregateMeasureAccess,
) error {
	acc := q.metrics[first].field.measure
	storage := ae.snap.Measure[acc.Ordinal]
	if storage.Rows() == 0 {
		return nil
	}
	if measureMode == aggregateMeasureAccessMixed {
		measureMode = selectAggregateMeasureAccess(ids.Cardinality(), ae.snap.Universe.Cardinality(), storage)
	}
	switch measureMode {
	case aggregateMeasureAccessNone:
		return nil
	case aggregateMeasureAccessLookup:
		it := ids.Iter()
		for it.HasNext() {
			id := it.Next()
			raw, ok := storage.Lookup(id)
			if !ok {
				continue
			}
			if err := addGroupedMeasureRaw(q, states, first, int(groupByID[int(id)]-1), raw, acc.Kind); err != nil {
				it.Release()
				return err
			}
		}
		it.Release()
		return nil
	}

	measureIDs, values, ok := storage.FlatSlices()
	if ok {
		return addGroupedMeasureSlicesByID(q, states, first, groupByID, measureIDs, values, acc.Kind)
	}
	for chunkPos := 0; chunkPos < storage.ChunkCount(); chunkPos++ {
		measureIDs, values = storage.ChunkSlices(chunkPos)
		if err := addGroupedMeasureSlicesByID(q, states, first, groupByID, measureIDs, values, acc.Kind); err != nil {
			return err
		}
	}
	return nil
}

func addGroupedMeasureSlicesByID(
	q *Query,
	states []aggregateMetricState,
	first int,
	groupByID []uint32,
	measureIDs []uint64,
	values []uint64,
	kind schema.MeasureValueKind,
) error {
	for i := range measureIDs {
		id := measureIDs[i]
		if id >= uint64(len(groupByID)) {
			continue
		}
		groupOrdinal := groupByID[int(id)]
		if groupOrdinal == 0 {
			continue
		}
		if err := addGroupedMeasureRaw(q, states, first, int(groupOrdinal-1), values[i], kind); err != nil {
			return err
		}
	}
	return nil
}

func addGroupedMeasureRaw(
	q *Query,
	states []aggregateMetricState,
	first int,
	groupIndex int,
	raw uint64,
	kind schema.MeasureValueKind,
) error {
	stateBase := groupIndex * len(q.metrics)

	if first+1 == len(q.metrics) {
		state := &states[stateBase+first]
		if state.metric.op == aggregateMetricSum {
			return state.addMeasureSumRaw(raw, kind)
		}
		return state.addRawMeasure(raw, kind, 1)
	}

	for metricIdx := first; metricIdx < len(q.metrics); metricIdx++ {
		if !aggregateMetricsShareMeasureField(q.metrics[first], q.metrics[metricIdx]) {
			continue
		}
		state := &states[stateBase+metricIdx]
		var err error
		if state.metric.op == aggregateMetricSum {
			err = state.addMeasureSumRaw(raw, kind)
		} else {
			err = state.addRawMeasure(raw, kind, 1)
		}
		if err != nil {
			return err
		}
	}
	return nil
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
			case aggregateMetricSum, aggregateMetricAvg:
				if err := state.addIndexKey(key, n); err != nil {
					return err
				}
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

func (ae *aggregateExecutor) executeGroupedRecursive(
	q *Query,
	current posting.List,
	level int,
	groupValues []Value,
	rows []Row,
	rowValues []Value,
	rowCountOnly bool,
) ([]Row, []Value, error) {

	if level == len(q.groups) {
		if rowCountOnly {
			rows, rowValues, row := appendGroupedRow(rows, rowValues, groupValues, len(q.metrics))
			if len(q.metrics) != 0 {
				rowCount := current.Cardinality()
				for i := range q.metrics {
					row[len(groupValues)+i] = Value{num: rowCount, any: ValueKindUint}
				}
			}
			return rows, rowValues, nil
		}
		states := aggregateMetricStateSlicePool.Get(len(q.metrics))[:len(q.metrics)]
		for i := range q.metrics {
			states[i].metric = q.metrics[i]
		}
		if err := ae.foldAggregateMetricStates(states, current, aggregateMeasureAccessMixed); err != nil {
			aggregateMetricStateSlicePool.Put(states)
			return rows, rowValues, err
		}
		rows, rowValues, row := appendGroupedRow(rows, rowValues, groupValues, len(states))
		for i := range states {
			row[len(groupValues)+i] = states[i].finish()
		}
		aggregateMetricStateSlicePool.Put(states)
		return rows, rowValues, nil
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
		var err error
		rows, rowValues, err = ae.executeGroupedRecursive(q, next, level+1, groupValues, rows, rowValues, rowCountOnly)
		if err != nil {
			next.Release()
			return rows, rowValues, err
		}
		next.Release()
	}

	nilIDs := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
	if !nilIDs.IsEmpty() {
		next := current.Borrow().BuildAnd(nilIDs)
		if !next.IsEmpty() {
			groupValues[level] = Value{}
			var err error
			rows, rowValues, err = ae.executeGroupedRecursive(q, next, level+1, groupValues, rows, rowValues, rowCountOnly)
			if err != nil {
				next.Release()
				return rows, rowValues, err
			}
		}
		next.Release()
	}

	return rows, rowValues, nil
}

func appendDistinctRow(rows []Row, values []Value, value Value) ([]Row, []Value) {
	if values == nil {
		return append(rows, Row{value}), values
	}
	row := len(values)
	values = append(values, value)
	return append(rows, values[row:row+1:row+1]), values
}

func (ae *aggregateExecutor) appendDistinctRows(
	rows []Row,
	values []Value,
	acc schema.IndexedFieldAccessor,
	ids posting.List,
	ov indexdata.FieldIndexView,
	nilIDs posting.List,
	filterCardinality uint64,
	universe uint64,
	singleBuckets bool,
	offset uint64,
	limit uint64,
) ([]Row, []Value, uint64) {
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	var contains posting.ContainsCursor
	if singleBuckets {
		contains.Reset(ids)
	}
	matched := uint64(0)

	if singleBuckets {
		for {
			key, _, id, _, ok := cur.NextKeyPostingOrSingle()
			if !ok {
				break
			}
			if !contains.Contains(id) {
				continue
			}
			if matched >= offset {
				rows, values = appendDistinctRow(rows, values, aggregateValueFromIndexKey(acc.Field, key))
				if limit > 0 && uint64(len(rows)) >= limit {
					matched++
					return rows, values, matched
				}
			}
			matched++
		}
	} else {
		for {
			key, bucketIDs, ok := cur.Next()
			if !ok {
				break
			}
			if aggregateIntersectCardinalityKnown(ids, bucketIDs, filterCardinality, universe) == 0 {
				continue
			}
			if matched >= offset {
				rows, values = appendDistinctRow(rows, values, aggregateValueFromIndexKey(acc.Field, key))
				if limit > 0 && uint64(len(rows)) >= limit {
					matched++
					return rows, values, matched
				}
			}
			matched++
		}
	}

	if aggregateIntersectCardinalityKnown(ids, nilIDs, filterCardinality, universe) > 0 {
		if matched >= offset {
			rows, values = appendDistinctRow(rows, values, Value{})
		}
		matched++
	}
	return rows, values, matched
}

func (ae *aggregateExecutor) foldAggregateMetricStates(states []aggregateMetricState, ids posting.List, measureMode aggregateMeasureAccess) error {
	for i := range states {
		metric := states[i].metric
		if metric.rowCount {
			states[i].count = ids.Cardinality()
			states[i].seen = true
			continue
		}
		if metric.field.isMeasure {
			prior := false
			for j := 0; j < i; j++ {
				if aggregateMetricsShareMeasureField(states[j].metric, metric) {
					prior = true
					break
				}
			}
			if prior {
				continue
			}
			if err := ae.foldMeasureMetricStates(states, ids, i, measureMode); err != nil {
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

func (ae *aggregateExecutor) foldMeasureMetricStates(states []aggregateMetricState, ids posting.List, first int, measureMode aggregateMeasureAccess) error {
	shared := false
	for i := first + 1; i < len(states); i++ {
		if aggregateMetricsShareMeasureField(states[first].metric, states[i].metric) {
			shared = true
			break
		}
	}
	if !shared {
		return ae.foldMeasureMetric(&states[first], ids, measureMode)
	}

	acc := states[first].metric.field.measure
	storage := ae.snap.Measure[acc.Ordinal]
	rows := storage.Rows()
	if rows == 0 {
		return nil
	}
	if measureMode == aggregateMeasureAccessMixed {
		measureMode = selectAggregateMeasureAccess(ids.Cardinality(), ae.snap.Universe.Cardinality(), storage)
	}
	switch measureMode {
	case aggregateMeasureAccessNone:
		return nil
	case aggregateMeasureAccessFullScan:
		for i := first; i < len(states); i++ {
			if !aggregateMetricsShareMeasureField(states[first].metric, states[i].metric) {
				continue
			}
			if err := ae.foldMeasureMetric(&states[i], ids, aggregateMeasureAccessFullScan); err != nil {
				return err
			}
		}
		return nil
	case aggregateMeasureAccessMergeScan:
		return addMeasureStorageIntersectStates(states, first, storage, acc.Kind, ids)
	}

	it := ids.Iter()
	for it.HasNext() {
		id := it.Next()
		raw, ok := storage.Lookup(id)
		if !ok {
			continue
		}
		if err := addMeasureRaw(states, first, raw, acc.Kind); err != nil {
			it.Release()
			return err
		}
	}
	it.Release()

	return nil
}

func addMeasureStorageIntersectStates(states []aggregateMetricState, first int, storage indexdata.MeasureStorage, kind schema.MeasureValueKind, ids posting.List) error {
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
				if err := addMeasureRaw(states, first, values[i], kind); err != nil {
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
				if err := addMeasureRaw(states, first, values[i], kind); err != nil {
					it.Release()
					return err
				}
			}
		}
	}
	it.Release()
	return nil
}

func addMeasureRaw(states []aggregateMetricState, first int, raw uint64, kind schema.MeasureValueKind) error {
	for i := first; i < len(states); i++ {
		if !aggregateMetricsShareMeasureField(states[first].metric, states[i].metric) {
			continue
		}
		state := &states[i]
		var err error
		if state.metric.op == aggregateMetricSum {
			err = state.addMeasureSumRaw(raw, kind)
		} else {
			err = state.addRawMeasure(raw, kind, 1)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggregateExecutor) foldMeasureMetric(state *aggregateMetricState, ids posting.List, measureMode aggregateMeasureAccess) error {
	acc := state.metric.field.measure
	storage := ae.snap.Measure[acc.Ordinal]
	rows := storage.Rows()
	if rows == 0 {
		return nil
	}
	if measureMode == aggregateMeasureAccessMixed {
		measureMode = selectAggregateMeasureAccess(ids.Cardinality(), ae.snap.Universe.Cardinality(), storage)
	}
	switch measureMode {
	case aggregateMeasureAccessNone:
		return nil
	case aggregateMeasureAccessFullScan:
		if state.metric.op == aggregateMetricCount {
			state.count += uint64(rows)
			state.seen = true
			return nil
		}
		return state.addMeasureStorageAll(storage, acc.Kind)
	case aggregateMeasureAccessMergeScan:
		return state.addMeasureStorageIntersect(storage, acc.Kind, ids)
	}

	it := ids.Iter()
	for it.HasNext() {
		id := it.Next()
		raw, ok := storage.Lookup(id)
		if !ok {
			continue
		}
		var err error
		if state.metric.op == aggregateMetricSum {
			err = state.addMeasureSumRaw(raw, acc.Kind)
		} else {
			err = state.addRawMeasure(raw, acc.Kind, 1)
		}
		if err != nil {
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
		if state.metric.op == aggregateMetricSum {
			return state.addMeasureSumValues(values, kind)
		}
		for i := range values {
			if err := state.addRawMeasure(values[i], kind, 1); err != nil {
				return err
			}
		}
		return nil
	}
	for chunkPos := 0; chunkPos < storage.ChunkCount(); chunkPos++ {
		_, chunkValues := storage.ChunkSlices(chunkPos)
		if state.metric.op == aggregateMetricSum {
			if err := state.addMeasureSumValues(chunkValues, kind); err != nil {
				return err
			}
			continue
		}
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
				var err error
				if state.metric.op == aggregateMetricSum {
					err = state.addMeasureSumRaw(values[i], kind)
				} else {
					err = state.addRawMeasure(values[i], kind, 1)
				}
				if err != nil {
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
				var err error
				if state.metric.op == aggregateMetricSum {
					err = state.addMeasureSumRaw(values[i], kind)
				} else {
					err = state.addRawMeasure(values[i], kind, 1)
				}
				if err != nil {
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

func aggregateMetricsShareMeasureField(a aggregateMetric, b aggregateMetric) bool {
	return !a.rowCount &&
		!b.rowCount &&
		a.field.isMeasure &&
		b.field.isMeasure &&
		a.field.measure.Ordinal == b.field.measure.Ordinal
}

func (ae *aggregateExecutor) foldOrdinaryMetricStates(states []aggregateMetricState, ids posting.List, first int) error {
	acc := states[first].metric.field.ordinary
	ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal])
	universe := ae.snap.Universe.Cardinality()
	filterCardinality := ids.Cardinality()
	keyCount := ov.KeyCount()
	if filterCardinality == universe {
		if keyCount > len(states)*aggregateFullScanMaxAvgBucketRows {
			br := ov.RangeByRanks(0, keyCount)
			_, rows := ov.RangeStats(br)
			if rows <= uint64(keyCount)*aggregateFullScanMaxAvgBucketRows {
				return ae.foldOrdinaryMetricStatesAll(states, first, ov, br, keyCount, rows)
			}
		}
	}
	if ov.Rows() == uint64(keyCount) {
		var contains posting.ContainsCursor
		contains.Reset(ids)
		cur := ov.NewCursor(ov.RangeByRanks(0, keyCount), false)
		for {
			key, _, id, _, ok := cur.NextKeyPostingOrSingle()
			if !ok {
				break
			}
			if !contains.Contains(id) {
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
					states[i].count++
					states[i].seen = true

				case aggregateMetricCountDistinct:
					states[i].count++
					states[i].seen = true

				case aggregateMetricSum, aggregateMetricAvg:
					if err := states[i].addIndexKey(key, 1); err != nil {
						return err
					}

				default:
					if !valueReady {
						value = aggregateValueFromIndexKey(acc.Field, key)
						valueReady = true
					}
					if err := states[i].addValue(value, 1); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	cur := ov.NewCursor(ov.RangeByRanks(0, keyCount), false)
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

			case aggregateMetricSum, aggregateMetricAvg:
				if err := states[i].addIndexKey(key, n); err != nil {
					return err
				}

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

func (ae *aggregateExecutor) foldOrdinaryMetricStatesAll(states []aggregateMetricState, first int, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, keyCount int, rows uint64) error {
	if rows == 0 {
		return nil
	}

	metric := states[first].metric
	firstValue := aggregateValueFromIndexKey(metric.field.ordinary.Field, ov.KeyAt(0))
	lastValue := firstValue
	if keyCount > 1 {
		lastValue = aggregateValueFromIndexKey(metric.field.ordinary.Field, ov.KeyAt(keyCount-1))
	}

	scanValues := false
	var scanIdx [8]int
	scanN := 0
	scanAll := false
	for i := first; i < len(states); i++ {
		if !aggregateMetricsShareOrdinaryField(metric, states[i].metric) {
			continue
		}
		switch states[i].metric.op {
		case aggregateMetricCount:
			states[i].count += rows
			states[i].seen = true
		case aggregateMetricCountDistinct:
			states[i].count += uint64(keyCount)
			states[i].seen = true
		case aggregateMetricMin:
			states[i].best = firstValue
			states[i].seen = true
		case aggregateMetricMax:
			states[i].best = lastValue
			states[i].seen = true
		default:
			scanValues = true
			if scanN < len(scanIdx) {
				scanIdx[scanN] = i
				scanN++
			} else {
				scanAll = true
			}
		}
	}
	if !scanValues {
		return nil
	}

	cur := ov.NewCursor(br, false)
	if !scanAll && (metric.field.kind == aggregateValueSigned || metric.field.kind == aggregateValueUnsigned) {
		for {
			key, bucketIDs, ok := cur.Next()
			if !ok {
				break
			}
			n := bucketIDs.Cardinality()
			if n == 0 {
				continue
			}
			for j := 0; j < scanN; j++ {
				if err := states[scanIdx[j]].addIndexKey(key, n); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if metric.field.kind == aggregateValueFloat && !scanAll {
		for {
			key, bucketIDs, ok := cur.Next()
			if !ok {
				break
			}
			n := bucketIDs.Cardinality()
			if n == 0 {
				continue
			}
			add := keycodec.Float64FromOrderedKey(key.U64()) * float64(n)
			for j := 0; j < scanN; j++ {
				state := &states[scanIdx[j]]
				state.count += n
				state.floatSum += add
				state.seen = true
			}
		}
		return nil
	}

	for {
		key, bucketIDs, ok := cur.Next()
		if !ok {
			break
		}
		n := bucketIDs.Cardinality()
		if n == 0 {
			continue
		}
		value := aggregateValueFromIndexKey(metric.field.ordinary.Field, key)
		if scanAll {
			for i := first; i < len(states); i++ {
				if !aggregateMetricsShareOrdinaryField(metric, states[i].metric) {
					continue
				}
				switch states[i].metric.op {
				case aggregateMetricSum, aggregateMetricAvg:
					if err := states[i].addValue(value, n); err != nil {
						return err
					}
				}
			}
			continue
		}
		for j := 0; j < scanN; j++ {
			if err := states[scanIdx[j]].addValue(value, n); err != nil {
				return err
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
		return fmt.Errorf("%w: unsupported measure value kind", rbierrors.ErrInvalidQuery)
	}
}

func (state *aggregateMetricState) addMeasureSumValues(values []uint64, kind schema.MeasureValueKind) error {
	if len(values) == 0 {
		return nil
	}
	switch kind {

	case schema.MeasureValueSigned:
		sum := state.intSum
		for i := range values {
			v := int64(values[i])
			if (v > 0 && sum > math.MaxInt64-v) || (v < 0 && sum < math.MinInt64-v) {
				return fmt.Errorf("%w: integer SUM overflow", rbierrors.ErrInvalidQuery)
			}
			sum += v
		}
		state.intSum = sum

	case schema.MeasureValueUnsigned:
		sum := state.uintSum
		for i := range values {
			v := values[i]
			if sum > math.MaxUint64-v {
				return fmt.Errorf("%w: unsigned SUM overflow", rbierrors.ErrInvalidQuery)
			}
			sum += v
		}
		state.uintSum = sum

	case schema.MeasureValueFloat:
		sum := state.floatSum
		for i := range values {
			sum += math.Float64frombits(values[i])
		}
		state.floatSum = sum

	default:
		return fmt.Errorf("%w: unsupported measure value kind", rbierrors.ErrInvalidQuery)
	}

	state.seen = true
	return nil
}

func (state *aggregateMetricState) addMeasureSumRaw(raw uint64, kind schema.MeasureValueKind) error {
	switch kind {

	case schema.MeasureValueSigned:
		v := int64(raw)
		if (v > 0 && state.intSum > math.MaxInt64-v) || (v < 0 && state.intSum < math.MinInt64-v) {
			return fmt.Errorf("%w: integer SUM overflow", rbierrors.ErrInvalidQuery)
		}
		state.intSum += v

	case schema.MeasureValueUnsigned:
		if state.uintSum > math.MaxUint64-raw {
			return fmt.Errorf("%w: unsigned SUM overflow", rbierrors.ErrInvalidQuery)
		}
		state.uintSum += raw

	case schema.MeasureValueFloat:
		state.floatSum += math.Float64frombits(raw)

	default:
		return fmt.Errorf("%w: unsupported measure value kind", rbierrors.ErrInvalidQuery)
	}

	state.seen = true
	return nil
}

func (state *aggregateMetricState) addIndexKey(key keycodec.IndexKey, n uint64) error {
	raw := key.U64()
	switch state.metric.field.kind {
	case aggregateValueSigned:
		return state.addSigned(keycodec.Int64FromOrderedKey(raw), n)
	case aggregateValueUnsigned:
		return state.addUnsigned(raw, n)
	case aggregateValueFloat:
		return state.addFloat(keycodec.Float64FromOrderedKey(raw), n)
	default:
		return fmt.Errorf("%w: aggregate requires numeric field %q", rbierrors.ErrInvalidQuery, state.metric.field.name)
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
			return fmt.Errorf("%w: aggregate field %q has non-signed index value", rbierrors.ErrInvalidQuery, state.metric.field.name)
		}
		return state.addSigned(v, n)

	case aggregateValueUnsigned:
		v, ok := value.Uint()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-unsigned index value", rbierrors.ErrInvalidQuery, state.metric.field.name)
		}
		return state.addUnsigned(v, n)

	case aggregateValueFloat:
		v, ok := value.Float()
		if !ok {
			return fmt.Errorf("%w: aggregate field %q has non-float index value", rbierrors.ErrInvalidQuery, state.metric.field.name)
		}
		return state.addFloat(v, n)

	default:
		if state.metric.op == aggregateMetricMin || state.metric.op == aggregateMetricMax {
			state.addBest(value)
			return nil
		}
		return fmt.Errorf("%w: aggregate requires numeric field %q", rbierrors.ErrInvalidQuery, state.metric.field.name)
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

	if state.metric.op == aggregateMetricAvg {
		state.count += n
		state.floatSum += float64(v) * float64(n)
	} else {
		if n > uint64(math.MaxInt64) {
			return fmt.Errorf("%w: integer SUM overflow", rbierrors.ErrInvalidQuery)
		}
		add := int64(n) * v
		if v != 0 && add/v != int64(n) {
			return fmt.Errorf("%w: integer SUM overflow", rbierrors.ErrInvalidQuery)
		}
		if (add > 0 && state.intSum > math.MaxInt64-add) || (add < 0 && state.intSum < math.MinInt64-add) {
			return fmt.Errorf("%w: integer SUM overflow", rbierrors.ErrInvalidQuery)
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

	if state.metric.op == aggregateMetricAvg {
		state.count += n
		state.floatSum += float64(v) * float64(n)
	} else {
		if n != 0 && v > math.MaxUint64/n {
			return fmt.Errorf("%w: unsigned SUM overflow", rbierrors.ErrInvalidQuery)
		}
		add := v * n
		if state.uintSum > math.MaxUint64-add {
			return fmt.Errorf("%w: unsigned SUM overflow", rbierrors.ErrInvalidQuery)
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
	if state.metric.op == aggregateMetricAvg {
		state.count += n
	}
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

func shouldCompactAggregateRows(before int, after int) bool {
	if after == 0 || after == before {
		return false
	}
	return before-after > after
}

func compactAggregateRows(result Result) Result {
	width := len(result.Rows[0])
	rows := make([]Row, len(result.Rows))
	values := make([]Value, len(result.Rows)*width)
	pos := 0
	for i := range result.Rows {
		row := values[pos : pos+width : pos+width]
		copy(row, result.Rows[i])
		rows[i] = row
		pos += width
	}
	result.Rows = rows
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
	return aggregateRowsLess(s.order, s.rows[i], s.rows[j])
}

func aggregateRowsLess(orders []aggregateOrder, left Row, right Row) bool {
	for k := range orders {
		order := orders[k]
		lv := left[order.index]
		rv := right[order.index]
		lk := lv.Kind()
		rk := rv.Kind()
		if lk == ValueKindNone || rk == ValueKindNone {
			if lk == rk {
				continue
			}
			return lk != ValueKindNone
		}
		cmp := aggregateCompareOrderValues(lv, rv, lk, rk)
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

func applyAggregateOrder(result Result, order []aggregateOrder, orderUnique bool, offset uint64, limit uint64) Result {
	if len(result.Rows) < 2 {
		return result
	}
	if limit == 0 {
		sort.Sort(aggregateRowSorter{rows: result.Rows, order: order})
		return result
	}
	if offset >= uint64(len(result.Rows)) {
		result.Rows = nil
		return result
	}
	need := offset + limit
	if need < offset || need >= uint64(len(result.Rows)) {
		sort.Sort(aggregateRowSorter{rows: result.Rows, order: order})
		return result
	}
	if !orderUnique {
		sort.Sort(aggregateRowSorter{rows: result.Rows, order: order})
		return result
	}
	k := int(need)
	n := len(result.Rows)
	nLog := bits.Len(uint(n)) - 1
	kLog := bits.Len(uint(k)) - 1
	if uint64(n)*uint64(nLog) <= (uint64(n)+uint64(k))*uint64(kLog) {
		sort.Sort(aggregateRowSorter{rows: result.Rows, order: order})
		return result
	}
	aggregateRowsTopK(result.Rows, order, k)
	sort.Sort(aggregateRowSorter{rows: result.Rows[:k], order: order})
	result.Rows = result.Rows[:k]
	return result
}

func aggregateRowsTopK(rows []Row, order []aggregateOrder, k int) {
	for i := k/2 - 1; i >= 0; i-- {
		aggregateRowsTopKDown(rows, order, i, k)
	}
	for i := k; i < len(rows); i++ {
		if aggregateRowsLess(order, rows[i], rows[0]) {
			rows[0], rows[i] = rows[i], rows[0]
			aggregateRowsTopKDown(rows, order, 0, k)
		}
	}
}

func aggregateRowsTopKDown(rows []Row, order []aggregateOrder, i int, n int) {
	for {
		child := i*2 + 1
		if child >= n {
			return
		}
		if child+1 < n && aggregateRowsLess(order, rows[child], rows[child+1]) {
			child++
		}
		if !aggregateRowsLess(order, rows[i], rows[child]) {
			return
		}
		rows[i], rows[child] = rows[child], rows[i]
		i = child
	}
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

func aggregateCompareOrderValues(a Value, b Value, ak ValueKind, bk ValueKind) int {
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
