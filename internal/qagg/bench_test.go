package qagg

import (
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const qaggBenchSeedBatchRows = 500_000

type qaggBenchRec struct {
	GroupLow  int `db:"group_low"  rbi:"index"`
	GroupMid  int `db:"group_mid"  rbi:"index"`
	GroupHigh int `db:"group_high" rbi:"index"`

	Tiny  int  `db:"tiny"  rbi:"index"`
	Mid   int  `db:"mid"   rbi:"index"`
	Broad bool `db:"broad" rbi:"index"`

	ValueLow  int64 `db:"value_low"  rbi:"index"`
	ValueHigh int64 `db:"value_high" rbi:"index"`

	DenseMeasure  int64  `db:"dense_measure"  rbi:"measure"`
	SparseMeasure *int64 `db:"sparse_measure" rbi:"measure"`
}

type qaggBenchScale struct {
	name string
	rows int
}

type qaggBenchIDLayout struct {
	name   string
	sparse bool
}

type qaggBenchDBKey struct {
	rows   int
	sparse bool
}

type qaggBenchDB struct {
	rt   *schema.Runtime
	exec *qexec.Runtime
	snap *snapshot.View
}

type qaggBenchCase struct {
	name        string
	route       string
	selectivity string
	storage     string
	build       func(qaggBenchScale) *qx.QX
}

var (
	qaggBenchResult   Result
	qaggBenchRows     int
	qaggBenchIDs      uint64
	qaggBenchGroups   int
	qaggBenchDecision aggregateRouteDecision

	qaggBenchSchemaMu sync.Mutex
	qaggBenchSchema   *schema.Runtime

	qaggBenchDBsMu sync.Mutex
	qaggBenchDBs   = make(map[qaggBenchDBKey]*qaggBenchDB)

	qaggBenchScales = [...]qaggBenchScale{
		{name: "Rows100K", rows: 100_000},
		{name: "Rows1M", rows: 1_000_000},
		{name: "Rows10M", rows: 10_000_000},
	}

	qaggBenchIDLayouts = [...]qaggBenchIDLayout{
		{name: "DenseIDs"},
		{name: "SparseIDs", sparse: true},
	}
)

func qaggBenchRuntime(b testing.TB) *schema.Runtime {
	b.Helper()

	qaggBenchSchemaMu.Lock()
	defer qaggBenchSchemaMu.Unlock()
	if qaggBenchSchema != nil {
		return qaggBenchSchema
	}

	rt, err := schema.Compile(reflect.TypeFor[qaggBenchRec](), schema.Config{})
	if err != nil {
		b.Fatalf("schema.Compile: %v", err)
	}
	qaggBenchSchema = rt
	return rt
}

func qaggBenchCachedDB(b testing.TB, scale qaggBenchScale, layout qaggBenchIDLayout) *qaggBenchDB {
	b.Helper()

	key := qaggBenchDBKey{rows: scale.rows, sparse: layout.sparse}
	qaggBenchDBsMu.Lock()
	db := qaggBenchDBs[key]
	if db != nil {
		qaggBenchDBsMu.Unlock()
		return db
	}
	qaggBenchDBsMu.Unlock()

	rt := qaggBenchRuntime(b)
	exec := qexec.NewRuntime(qexec.Config{Schema: rt})
	db = &qaggBenchDB{rt: rt, exec: exec}
	for base := 0; base < scale.rows; base += qaggBenchSeedBatchRows {
		n := scale.rows - base
		if n > qaggBenchSeedBatchRows {
			n = qaggBenchSeedBatchRows
		}
		vals := make([]qaggBenchRec, n)
		entries := make([]snapshot.BatchEntry, n)
		for i := 0; i < n; i++ {
			row := uint64(base + i + 1)
			vals[i] = qaggBenchRecord(row)
			if row&7 == 0 {
				vals[i].SparseMeasure = &vals[i].DenseMeasure
			}
			id := row
			if layout.sparse {
				id = row*16 + 1
			}
			entries[i] = snapshot.BatchEntry{ID: id, New: unsafe.Pointer(&vals[i])}
		}
		db.snap = snapshot.BuildPrepared(uint64(base/qaggBenchSeedBatchRows+1), db.snap, rt, snapshot.CacheConfig{
			MatPredMaxEntries: 256,
			MatPredMaxCard:    512 << 10,
		}, nil, nil, entries)
	}

	qaggBenchDBsMu.Lock()
	if existing := qaggBenchDBs[key]; existing != nil {
		qaggBenchDBsMu.Unlock()
		return existing
	}
	qaggBenchDBs[key] = db
	qaggBenchDBsMu.Unlock()
	return db
}

func qaggBenchRecord(row uint64) qaggBenchRec {
	return qaggBenchRec{
		GroupLow:     int(row & 7),
		GroupMid:     int(row & 1023),
		GroupHigh:    int(row >> 4),
		Tiny:         int(row % 10_000),
		Mid:          int(row % 100),
		Broad:        row%5 < 3,
		ValueLow:     int64(row & 127),
		ValueHigh:    int64(row),
		DenseMeasure: int64((row*17)%1_000_003) - 500_000,
	}
}

func qaggBenchRange(rows int, name string) (int64, int64) {
	width := rows
	switch name {
	case "Tiny":
		width = rows / 10_000
		if width < 64 {
			width = 64
		}
	case "Mid":
		width = rows / 100
		if width < 1024 {
			width = 1024
		}
	case "Broad":
		width = rows * 3 / 5
	}
	if width >= rows {
		return 1, int64(rows + 1)
	}
	start := (rows - width) / 2
	if start < 1 {
		start = 1
	}
	return int64(start), int64(start + width)
}

func qaggBenchScalarFilter(name string) []qx.Expr {
	switch name {
	case "Tiny":
		return []qx.Expr{qx.EQ("tiny", 0)}
	case "Mid":
		return []qx.Expr{qx.EQ("mid", 0)}
	case "Broad":
		return []qx.Expr{qx.EQ("broad", true)}
	default:
		return nil
	}
}

func qaggBenchRangeFilter(scale qaggBenchScale, name string) []qx.Expr {
	start, end := qaggBenchRange(scale.rows, name)
	return []qx.Expr{qx.GTE("value_high", start), qx.LT("value_high", end)}
}

func qaggBenchORFilter() []qx.Expr {
	return []qx.Expr{qx.OR(
		qx.EQ("tiny", 0),
		qx.EQ("mid", 1),
		qx.EQ("group_low", 3),
	)}
}

func qaggBenchQuery(filters []qx.Expr) *qx.QX {
	return qx.Query(filters...)
}

func qaggBenchCases() []qaggBenchCase {
	return []qaggBenchCase{
		{
			name:        "NoFilter",
			route:       "RowCount",
			selectivity: "All",
			storage:     "Filter",
			build: func(qaggBenchScale) *qx.QX {
				return qx.Aggregate(qx.ROWCOUNT().AS("rows"))
			},
		},
		{
			name:        "ScalarEQ",
			route:       "RowCount",
			selectivity: "Tiny",
			storage:     "Filter",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Tiny")).Metrics(qx.ROWCOUNT().AS("rows"))
			},
		},
		{
			name:        "Range",
			route:       "RowCount",
			selectivity: "Mid",
			storage:     "Filter",
			build: func(scale qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchRangeFilter(scale, "Mid")).Metrics(qx.ROWCOUNT().AS("rows"))
			},
		},
		{
			name:        "OR",
			route:       "RowCount",
			selectivity: "Broad",
			storage:     "Filter",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchORFilter()).Metrics(qx.ROWCOUNT().AS("rows"))
			},
		},
		{
			name:        "LowCardUnfiltered",
			route:       "Distinct",
			selectivity: "All",
			storage:     "OrdinaryLowCard",
			build: func(qaggBenchScale) *qx.QX {
				return qx.Aggregate(qx.DISTINCT("group_low").AS("v"))
			},
		},
		{
			name:        "LowCardFiltered",
			route:       "Distinct",
			selectivity: "Tiny",
			storage:     "OrdinaryLowCard",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Tiny")).Metrics(qx.DISTINCT("group_low").AS("v"))
			},
		},
		{
			name:        "HighCardUnfiltered",
			route:       "Distinct",
			selectivity: "All",
			storage:     "OrdinaryHighCard",
			build: func(qaggBenchScale) *qx.QX {
				return qx.Aggregate(qx.DISTINCT("value_high").AS("v"))
			},
		},
		{
			name:        "HighCardFiltered",
			route:       "Distinct",
			selectivity: "Tiny",
			storage:     "OrdinaryHighCard",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Tiny")).Metrics(qx.DISTINCT("value_high").AS("v"))
			},
		},
		{
			name:        "CountDistinctFiltered",
			route:       "Distinct",
			selectivity: "Broad",
			storage:     "OrdinaryHighCard",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).Metrics(qx.COUNT(qx.DISTINCT("value_high")).AS("v"))
			},
		},
		{
			name:        "SharedMetricsOneField",
			route:       "Ungrouped",
			selectivity: "Mid",
			storage:     "OrdinaryLowCard",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Mid")).Metrics(
					qx.COUNT("value_low").AS("cnt"),
					qx.SUM("value_low").AS("sum"),
					qx.AVG("value_low").AS("avg"),
					qx.MIN("value_low").AS("min"),
					qx.MAX("value_low").AS("max"),
				)
			},
		},
		{
			name:        "SeveralFields",
			route:       "Ungrouped",
			selectivity: "Mid",
			storage:     "OrdinaryMixedCard",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Mid")).Metrics(
					qx.SUM("value_low").AS("low_sum"),
					qx.SUM("value_high").AS("high_sum"),
				)
			},
		},
		{
			name:        "DenseMeasureFullScan",
			route:       "Ungrouped",
			selectivity: "All",
			storage:     "MeasureDense",
			build: func(qaggBenchScale) *qx.QX {
				return qx.Aggregate(qx.SUM("dense_measure").AS("sum"))
			},
		},
		{
			name:        "DenseMeasureMergeScan",
			route:       "Ungrouped",
			selectivity: "Broad",
			storage:     "MeasureDense",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).Metrics(qx.SUM("dense_measure").AS("sum"))
			},
		},
		{
			name:        "SparseMeasureLookup",
			route:       "Ungrouped",
			selectivity: "Tiny",
			storage:     "MeasureSparse",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Tiny")).Metrics(qx.SUM("sparse_measure").AS("sum"))
			},
		},
		{
			name:        "GroupLow",
			route:       "Grouped",
			selectivity: "Broad",
			storage:     "OrdinaryLowCard_GroupLow",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).
					Group("group_low").
					Metrics(qx.ROWCOUNT().AS("rows"), qx.SUM("value_low").AS("sum"))
			},
		},
		{
			name:        "GroupMid",
			route:       "Grouped",
			selectivity: "Mid",
			storage:     "OrdinaryHighCard_GroupMid",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Mid")).
					Group("group_mid").
					Metrics(qx.SUM("value_high").AS("sum"))
			},
		},
		{
			name:        "GroupLowMid",
			route:       "Grouped",
			selectivity: "Broad",
			storage:     "OrdinaryMixedCard_Group2",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).
					Group("group_low", "group_mid").
					Metrics(qx.ROWCOUNT().AS("rows"), qx.SUM("value_low").AS("sum"))
			},
		},
		{
			name:        "MeasureBroad",
			route:       "Grouped",
			selectivity: "Broad",
			storage:     "MeasureDense_GroupLow",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).
					Group("group_low").
					Metrics(qx.SUM("dense_measure").AS("sum"))
			},
		},
		{
			name:        "MeasureBroadGroup2",
			route:       "Grouped",
			selectivity: "Broad",
			storage:     "MeasureDense_Group2",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).
					Group("group_low", "group_mid").
					Metrics(qx.SUM("dense_measure").AS("sum"))
			},
		},
		{
			name:        "MeasureSelective",
			route:       "Grouped",
			selectivity: "Tiny",
			storage:     "MeasureSparse_GroupMid",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Tiny")).
					Group("group_mid").
					Metrics(qx.SUM("sparse_measure").AS("sum"))
			},
		},
		{
			name:        "MeasureSelectiveGroup2",
			route:       "Grouped",
			selectivity: "Tiny",
			storage:     "MeasureSparse_Group2",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Tiny")).
					Group("group_low", "group_mid").
					Metrics(qx.SUM("sparse_measure").AS("sum"))
			},
		},
		{
			name:        "HybridBroad",
			route:       "Grouped",
			selectivity: "Broad",
			storage:     "HybridOrdinaryMeasure_GroupLow",
			build: func(qaggBenchScale) *qx.QX {
				return qaggBenchQuery(qaggBenchScalarFilter("Broad")).
					Group("group_low").
					Metrics(qx.SUM("value_low").AS("low_sum"), qx.SUM("dense_measure").AS("measure_sum"))
			},
		},
	}
}

func qaggBenchName(c qaggBenchCase, scale qaggBenchScale, layout qaggBenchIDLayout) string {
	return c.route + "/" + scale.name + "/" + c.selectivity + "/" + c.storage + "/" + layout.name + "/" + c.name
}

func qaggBenchPrepareName(c qaggBenchCase, scale qaggBenchScale) string {
	return c.route + "/" + scale.name + "/" + c.selectivity + "/" + c.storage + "/" + c.name
}

func qaggBenchReportResult(b *testing.B, q *Query, ids uint64, result Result) {
	b.ReportMetric(float64(len(result.Rows)), "rows/op")
	if ids > 0 {
		b.ReportMetric(float64(ids), "ids/op")
	}
	if len(q.groups) > 0 {
		b.ReportMetric(float64(len(result.Rows)), "groups/op")
	}
	qaggBenchRows = len(result.Rows)
	qaggBenchIDs = ids
	qaggBenchGroups = 0
	if len(q.groups) > 0 {
		qaggBenchGroups = len(result.Rows)
	}
	qaggBenchResult = result
}

func BenchmarkAggregatePrepare(b *testing.B) {
	rt := qaggBenchRuntime(b)
	cases := qaggBenchCases()
	for si := range qaggBenchScales {
		scale := qaggBenchScales[si]
		for ci := range cases {
			tc := cases[ci]
			b.Run(qaggBenchPrepareName(tc, scale), func(b *testing.B) {
				src := tc.build(scale)
				var prepared *Query
				var err error
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prepared, err = Prepare(src, rt)
					if err != nil {
						b.Fatalf("Prepare: %v", err)
					}
					qaggBenchRows = len(prepared.metrics) + len(prepared.groups)
					prepared.Release()
				}
			})
		}
	}
}

func BenchmarkAggregateFilterOnly(b *testing.B) {
	cases := qaggBenchCases()
	for si := range qaggBenchScales {
		scale := qaggBenchScales[si]
		for li := range qaggBenchIDLayouts {
			layout := qaggBenchIDLayouts[li]
			for ci := range cases {
				tc := cases[ci]
				b.Run(qaggBenchName(tc, scale, layout), func(b *testing.B) {
					db := qaggBenchCachedDB(b, scale, layout)
					prepared, err := Prepare(tc.build(scale), db.rt)
					if err != nil {
						b.Fatalf("Prepare: %v", err)
					}
					defer prepared.Release()

					view := db.exec.AcquireView(db.snap)
					defer db.exec.ReleaseView(view)
					ids, err := view.Filter(prepared.filter)
					if err != nil {
						b.Fatalf("Filter warmup: %v", err)
					}
					qaggBenchIDs = ids.Cardinality()
					ids.Release()

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						ids, err = view.Filter(prepared.filter)
						if err != nil {
							b.Fatalf("Filter: %v", err)
						}
						qaggBenchIDs = ids.Cardinality()
						ids.Release()
					}
					b.ReportMetric(float64(qaggBenchIDs), "ids/op")
				})
			}
		}
	}
}

func BenchmarkAggregateSelectorOnly(b *testing.B) {
	cases := qaggBenchCases()
	for si := range qaggBenchScales {
		scale := qaggBenchScales[si]
		for li := range qaggBenchIDLayouts {
			layout := qaggBenchIDLayouts[li]
			for ci := range cases {
				tc := cases[ci]
				b.Run(qaggBenchName(tc, scale, layout), func(b *testing.B) {
					db := qaggBenchCachedDB(b, scale, layout)
					prepared, err := Prepare(tc.build(scale), db.rt)
					if err != nil {
						b.Fatalf("Prepare: %v", err)
					}
					defer prepared.Release()

					view := db.exec.AcquireView(db.snap)
					defer db.exec.ReleaseView(view)
					ids, err := view.Filter(prepared.filter)
					if err != nil {
						b.Fatalf("Filter: %v", err)
					}
					defer ids.Release()

					exec := aggregateExecutor{snap: db.snap}
					var decision aggregateRouteDecision
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch selectAggregateFamily(prepared) {
						case aggregateSelectorCount:
							decision = selectCountAggregate(aggregateCountFacts{})
						case aggregateSelectorDistinct:
							decision = selectDistinctAggregate(exec.collectDistinctFacts(prepared, ids))
						case aggregateSelectorUngrouped:
							decision = selectUngroupedAggregate(exec.collectUngroupedFacts(prepared, ids))
						default:
							decision = selectGroupedAggregate(exec.collectGroupedFacts(prepared, ids))
						}
					}
					qaggBenchDecision = decision
					b.ReportMetric(float64(ids.Cardinality()), "ids/op")
				})
			}
		}
	}
}

func BenchmarkAggregateOnly(b *testing.B) {
	cases := qaggBenchCases()
	for si := range qaggBenchScales {
		scale := qaggBenchScales[si]
		for li := range qaggBenchIDLayouts {
			layout := qaggBenchIDLayouts[li]
			for ci := range cases {
				tc := cases[ci]
				b.Run(qaggBenchName(tc, scale, layout), func(b *testing.B) {
					db := qaggBenchCachedDB(b, scale, layout)
					prepared, err := Prepare(tc.build(scale), db.rt)
					if err != nil {
						b.Fatalf("Prepare: %v", err)
					}
					defer prepared.Release()

					view := db.exec.AcquireView(db.snap)
					defer db.exec.ReleaseView(view)
					ids, err := view.Filter(prepared.filter)
					if err != nil {
						b.Fatalf("Filter: %v", err)
					}
					defer ids.Release()

					exec := aggregateExecutor{snap: db.snap}
					result, err := exec.executeAggregate(prepared, ids)
					if err != nil {
						b.Fatalf("Aggregate warmup: %v", err)
					}
					qaggBenchReportResult(b, prepared, ids.Cardinality(), result)

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result, err = exec.executeAggregate(prepared, ids)
						if err != nil {
							b.Fatalf("Aggregate: %v", err)
						}
						qaggBenchResult = result
					}
					qaggBenchReportResult(b, prepared, ids.Cardinality(), qaggBenchResult)
				})
			}
		}
	}
}

func BenchmarkAggregatePreparedEndToEnd(b *testing.B) {
	cases := qaggBenchCases()
	for si := range qaggBenchScales {
		scale := qaggBenchScales[si]
		for li := range qaggBenchIDLayouts {
			layout := qaggBenchIDLayouts[li]
			for ci := range cases {
				tc := cases[ci]
				b.Run(qaggBenchName(tc, scale, layout), func(b *testing.B) {
					db := qaggBenchCachedDB(b, scale, layout)
					prepared, err := Prepare(tc.build(scale), db.rt)
					if err != nil {
						b.Fatalf("Prepare: %v", err)
					}
					defer prepared.Release()

					view := db.exec.AcquireView(db.snap)
					defer db.exec.ReleaseView(view)
					ids, err := view.Filter(prepared.filter)
					if err != nil {
						b.Fatalf("Filter: %v", err)
					}
					idsCardinality := ids.Cardinality()
					ids.Release()

					result, err := Execute(view, db.snap, prepared)
					if err != nil {
						b.Fatalf("Execute warmup: %v", err)
					}
					qaggBenchReportResult(b, prepared, idsCardinality, result)

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result, err = Execute(view, db.snap, prepared)
						if err != nil {
							b.Fatalf("Execute: %v", err)
						}
						qaggBenchResult = result
					}
					qaggBenchReportResult(b, prepared, idsCardinality, qaggBenchResult)
				})
			}
		}
	}
}

func BenchmarkAggregatePostprocessOnly(b *testing.B) {
	for si := range qaggBenchScales {
		scale := qaggBenchScales[si]
		for li := range qaggBenchIDLayouts {
			layout := qaggBenchIDLayouts[li]
			name := "Postprocess/" + scale.name + "/All/OrdinaryHighCard_GroupHigh/" + layout.name + "/HavingOrderLimit"
			b.Run(name, func(b *testing.B) {
				db := qaggBenchCachedDB(b, scale, layout)
				src := qx.Group("group_high").
					Metrics(qx.ROWCOUNT().AS("rows")).
					Having(qx.GTE(qx.OUT("rows"), 1)).
					SortOut("rows", qx.DESC).
					Offset(8).
					Limit(128)
				prepared, err := Prepare(src, db.rt)
				if err != nil {
					b.Fatalf("Prepare: %v", err)
				}
				defer prepared.Release()

				view := db.exec.AcquireView(db.snap)
				defer db.exec.ReleaseView(view)
				ids, err := view.Filter(prepared.filter)
				if err != nil {
					b.Fatalf("Filter: %v", err)
				}
				defer ids.Release()

				exec := aggregateExecutor{snap: db.snap}
				base, err := exec.executeAggregate(prepared, ids)
				if err != nil {
					b.Fatalf("Aggregate: %v", err)
				}
				scratch := make([]Row, len(base.Rows))

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					copy(scratch, base.Rows)
					result := Result{Layout: base.Layout, Rows: scratch[:len(base.Rows)]}
					result = applyAggregateHaving(result, prepared.having)
					result = applyAggregateOrder(result, prepared.order, prepared.orderUnique, prepared.offset, prepared.limit)
					result = applyAggregateWindow(result, prepared.offset, prepared.limit)
					qaggBenchResult = result
				}
				b.ReportMetric(float64(len(qaggBenchResult.Rows)), "rows/op")
				b.ReportMetric(float64(ids.Cardinality()), "ids/op")
				b.ReportMetric(float64(len(base.Rows)), "groups/op")
				qaggBenchRows = len(qaggBenchResult.Rows)
				qaggBenchIDs = ids.Cardinality()
				qaggBenchGroups = len(base.Rows)
			})
		}
	}
}
