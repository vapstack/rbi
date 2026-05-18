package qexec

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/snapshot"
)

const (
	qexecBenchRows          = 100_000
	qexecBenchSeedBatchRows = 500_000
)

var (
	qexecBenchIDs         []uint64
	qexecBenchCount       uint64
	qexecBenchTraceEvents uint64
)

type qexecBenchScale struct {
	name string
	rows int
}

type qexecBenchSelectivity struct {
	name string
	num  uint64
	den  uint64
	min  uint64
}

type qexecBenchDBKey struct {
	rows                         int
	matPredCacheMaxEntries       int
	matPredCacheMaxCard          uint64
	numericRangeBucketSize       int
	numericRangeBucketMinKeys    int
	numericRangeBucketMinSpanKey int
}

var (
	qexecBenchAllScales = [...]qexecBenchScale{
		{name: "Rows100K", rows: 100_000},
		{name: "Rows1M", rows: 1_000_000},
		{name: "Rows10M", rows: 10_000_000},
	}
	qexecBenchAllSelectivities = [...]qexecBenchSelectivity{
		{name: "Tiny", num: 1, den: 10_000, min: 64},
		{name: "Mid", num: 1, den: 100, min: 1_024},
		{name: "Broad", num: 3, den: 5, min: 8_192},
	}

	qexecBenchDBsMu sync.Mutex
	qexecBenchDBs   = make(map[qexecBenchDBKey]*testDB)

	qexecBenchOptValues = [256]string{}
)

func init() {
	for i := range qexecBenchOptValues {
		qexecBenchOptValues[i] = "opt-" + strconv.FormatInt(int64(i), 10)
	}
}

func qexecBenchOptions() testOptions {
	return testOptions{
		MatPredCacheMaxEntries:         256,
		MatPredCacheMaxCard:            256 << 10,
		NumericRangeBucketSize:         256,
		NumericRangeBucketMinFieldKeys: 1024,
		NumericRangeBucketMinSpanKeys:  256,
	}
}

func qexecBenchScales() []qexecBenchScale {
	if testing.Short() {
		return qexecBenchAllScales[:1]
	}
	return qexecBenchAllScales[:]
}

func qexecBenchSelectivities() []qexecBenchSelectivity {
	return qexecBenchAllSelectivities[:]
}

func qexecBenchRange(rows int, sel qexecBenchSelectivity) (int, int) {
	n := uint64(rows)
	width := n * sel.num / sel.den
	if width < sel.min {
		width = sel.min
	}
	if width < 1 {
		width = 1
	}
	if width >= n {
		return 1, rows + 1
	}
	start := (n - width) / 2
	if width <= 4096 && n > 4096 {
		center := n / 2
		start = center - center%2048
	}
	if start < 1 {
		start = 1
	}
	end := start + width
	if end > n+1 {
		end = n + 1
	}
	return int(start), int(end)
}

func qexecBenchAgeRange(rows int, sel qexecBenchSelectivity) (qx.Expr, qx.Expr) {
	start, end := qexecBenchRange(rows, sel)
	return qx.GTE("age", start), qx.LT("age", end)
}

func qexecBenchScoreRange(rows int, sel qexecBenchSelectivity) (qx.Expr, qx.Expr) {
	start, end := qexecBenchRange(rows, sel)
	return qx.GTE("score", float64(start)), qx.LT("score", float64(end))
}

func qexecBenchPrefix(sel qexecBenchSelectivity) string {
	switch sel.name {
	case "Tiny":
		return "user-tiny"
	case "Mid":
		return "user-mid"
	default:
		return "user-broad"
	}
}

func qexecBenchTag(sel qexecBenchSelectivity) string {
	switch sel.name {
	case "Tiny":
		return "tag_tiny"
	case "Mid":
		return "tag_mid"
	default:
		return "tag_broad"
	}
}

func qexecBenchHighCardEmailPrefix(sel qexecBenchSelectivity) string {
	switch sel.name {
	case "Tiny":
		return "hct-"
	case "Mid":
		return "hcm-"
	default:
		return "hcb-"
	}
}

func qexecBenchHighCardEmail(id uint64) string {
	var buf [14]byte
	prefix := "hco-"
	switch {
	case id%2048 == 0:
		prefix = "hct-"
	case id%100 == 0:
		prefix = "hcm-"
	case id%2 == 0:
		prefix = "hcb-"
	}
	copy(buf[:4], prefix)
	for i := len(buf) - 1; i >= 4; i-- {
		buf[i] = byte('0' + id%10)
		id /= 10
	}
	return string(buf[:])
}

func qexecBenchRunScales(b *testing.B, fn func(*testing.B, *testDB, qexecBenchScale)) {
	b.Helper()
	scales := qexecBenchScales()
	for i := range scales {
		scale := scales[i]
		b.Run(scale.name, func(b *testing.B) {
			db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), scale.rows)
			fn(b, db, scale)
		})
	}
}

func qexecBenchRunScaleSelectivities(b *testing.B, fn func(*testing.B, *testDB, qexecBenchScale, qexecBenchSelectivity)) {
	b.Helper()
	scales := qexecBenchScales()
	sels := qexecBenchSelectivities()
	for i := range scales {
		scale := scales[i]
		b.Run(scale.name, func(b *testing.B) {
			db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), scale.rows)
			for j := range sels {
				sel := sels[j]
				b.Run(sel.name, func(b *testing.B) {
					fn(b, db, scale, sel)
				})
			}
		})
	}
}

func newQexecBenchDBWithOptionsAndRows(b *testing.B, opts testOptions, rows int) *testDB {
	b.Helper()
	if opts.TraceSink != nil {
		return buildQexecBenchDBWithOptionsAndRows(b, opts, rows)
	}

	key := qexecBenchDBKey{
		rows:                         rows,
		matPredCacheMaxEntries:       opts.MatPredCacheMaxEntries,
		matPredCacheMaxCard:          opts.MatPredCacheMaxCard,
		numericRangeBucketSize:       opts.NumericRangeBucketSize,
		numericRangeBucketMinKeys:    opts.NumericRangeBucketMinFieldKeys,
		numericRangeBucketMinSpanKey: opts.NumericRangeBucketMinSpanKeys,
	}

	qexecBenchDBsMu.Lock()
	db := qexecBenchDBs[key]
	qexecBenchDBsMu.Unlock()
	if db != nil {
		db.clearCurrentSnapshotCaches()
		return db
	}

	db = buildQexecBenchDBWithOptionsAndRows(b, opts, rows)
	qexecBenchDBsMu.Lock()
	if cached := qexecBenchDBs[key]; cached != nil {
		qexecBenchDBsMu.Unlock()
		cached.clearCurrentSnapshotCaches()
		return cached
	}
	qexecBenchDBs[key] = db
	qexecBenchDBsMu.Unlock()
	return db
}

func buildQexecBenchDBWithOptionsAndRows(b *testing.B, opts testOptions, rows int) *testDB {
	b.Helper()

	db := newTestDB(b, opts)
	countries := [...]string{"US", "DE", "NL", "PL", "BR", "JP", "IN", "CA"}
	names := [...]string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "mallory"}
	tagsTiny := []string{"tag_tiny", "go", "db"}
	tagsMid := []string{"tag_mid", "go", "ops"}
	tagsBroad := []string{"tag_broad", "go"}
	tagsDB := []string{"db"}
	tagsOther := []string{"java"}
	seedQexecBenchData(b, db, rows, func(id uint64) testRec {
		var opt *string
		if id%5 == 0 {
			opt = &qexecBenchOptValues[id&255]
		}
		tags := tagsOther
		switch {
		case id%2048 == 0:
			tags = tagsTiny
		case id%100 == 0:
			tags = tagsMid
		case id%2 == 0:
			tags = tagsBroad
		case id%5 == 0:
			tags = tagsDB
		}
		fullName := "user-other"
		switch {
		case id%2048 == 0:
			fullName = "user-tiny"
		case id%100 == 0:
			fullName = "user-mid"
		case id%2 == 0:
			fullName = "user-broad"
		}
		return testRec{
			Meta:     Meta{Country: countries[id&7]},
			Name:     names[(id>>3)&7],
			Email:    qexecBenchHighCardEmail(id),
			Age:      int(id),
			Score:    float64((id*7919)%uint64(rows)) + float64(id&15)/16,
			Active:   id&1 == 0,
			Tags:     tags,
			FullName: fullName,
			Opt:      opt,
		}
	})
	return db
}

func qexecBenchPublicOptions(opts testOptions) Options {
	return Options{
		AnalyzeInterval:                             -1,
		SnapshotMaterializedPredCacheMaxEntries:     opts.MatPredCacheMaxEntries,
		SnapshotMaterializedPredCacheMaxCardinality: int(opts.MatPredCacheMaxCard),
		NumericRangeBucketSize:                      opts.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys:              opts.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:               opts.NumericRangeBucketMinSpanKeys,
		TraceSink:                                   opts.TraceSink,
		TraceSampleEvery:                            opts.TraceSampleEvery,
	}
}

func newQexecBenchPublicDB(b *testing.B, rows int, options Options) *DB[uint64, testRec] {
	b.Helper()

	db, raw := openBoltAndNew[uint64, testRec](b, filepath.Join(b.TempDir(), "qexec_bench_public.db"), options)
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	countries := [...]string{"US", "DE", "NL", "PL", "BR", "JP", "IN", "CA"}
	names := [...]string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "mallory"}
	tagsTiny := []string{"tag_tiny", "go", "db"}
	tagsMid := []string{"tag_mid", "go", "ops"}
	tagsBroad := []string{"tag_broad", "go"}
	tagsDB := []string{"db"}
	tagsOther := []string{"java"}

	for base := 0; base < rows; base += qexecBenchSeedBatchRows {
		n := rows - base
		if n > qexecBenchSeedBatchRows {
			n = qexecBenchSeedBatchRows
		}
		ids := make([]uint64, n)
		vals := make([]testRec, n)
		ptrs := make([]*testRec, n)
		for i := 0; i < n; i++ {
			id := uint64(base + i + 1)
			var opt *string
			if id%5 == 0 {
				opt = &qexecBenchOptValues[id&255]
			}
			tags := tagsOther
			switch {
			case id%2048 == 0:
				tags = tagsTiny
			case id%100 == 0:
				tags = tagsMid
			case id%2 == 0:
				tags = tagsBroad
			case id%5 == 0:
				tags = tagsDB
			}
			fullName := "user-other"
			switch {
			case id%2048 == 0:
				fullName = "user-tiny"
			case id%100 == 0:
				fullName = "user-mid"
			case id%2 == 0:
				fullName = "user-broad"
			}
			ids[i] = id
			vals[i] = testRec{
				Meta:     Meta{Country: countries[id&7]},
				Name:     names[(id>>3)&7],
				Email:    qexecBenchHighCardEmail(id),
				Age:      int(id),
				Score:    float64((id*7919)%uint64(rows)) + float64(id&15)/16,
				Active:   id&1 == 0,
				Tags:     tags,
				FullName: fullName,
				Opt:      opt,
			}
			ptrs[i] = &vals[i]
		}
		if err := db.BatchSet(ids, ptrs); err != nil {
			b.Fatalf("BatchSet: %v", err)
		}
	}
	return db
}

func seedQexecBenchData(b *testing.B, db *testDB, rows int, gen func(uint64) testRec) {
	b.Helper()

	for base := 0; base < rows; base += qexecBenchSeedBatchRows {
		n := rows - base
		if n > qexecBenchSeedBatchRows {
			n = qexecBenchSeedBatchRows
		}
		vals := make([]testRec, n)
		entries := make([]snapshot.BatchEntry, n)
		for i := 0; i < n; i++ {
			id := uint64(base + i + 1)
			vals[i] = gen(id)
			entries[i] = snapshot.BatchEntry{ID: id, New: unsafe.Pointer(&vals[i])}
		}
		db.seq++
		db.snap = snapshot.BuildPrepared(db.seq, db.snap, db.rt, db.cfg, nil, nil, entries)
	}
}

func benchmarkPreparedQueryShape(b *testing.B, db *testDB, q *qx.QX) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	out, err := view.Query(&shape, false, false)
	if err != nil {
		b.Fatalf("Query warmup: %v", err)
	}
	qexecBenchIDs = out

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err = view.Query(&shape, false, false)
		if err != nil {
			b.Fatalf("Query: %v", err)
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkQueryPreparedShape(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		fixed := []struct {
			name string
			q    *qx.QX
		}{
			{name: "NoFilter_Limit100", q: qx.Query().Limit(100)},
			{name: "ScalarEQ_NoOrder", q: qx.Query(qx.EQ("country", "US"))},
			{name: "Order_NoFilter_Limit128", q: qx.Query().Sort("score", qx.DESC).Limit(128)},
		}
		for i := range fixed {
			tc := fixed[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkPreparedQueryShape(b, db, tc.q)
			})
		}

		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				scoreStart, scoreEnd := qexecBenchScoreRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				prefix := qexecBenchPrefix(sel)
				cases := []struct {
					name string
					q    *qx.QX
				}{
					{
						name: "AndMixed_NoOrder",
						q: qx.Query(
							qx.EQ("country", "US"),
							qx.EQ("active", true),
							ageStart,
							ageEnd,
							qx.HASANY("tags", []string{tag, "missing_tag"}),
						),
					},
					{
						name: "Range_NoOrder_Limit128",
						q:    qx.Query(ageStart, ageEnd).Limit(128),
					},
					{
						name: "Prefix_NoOrder_Limit128",
						q:    qx.Query(qx.PREFIX("full_name", prefix)).Limit(128),
					},
					{
						name: "Range_Order_Limit128",
						q:    qx.Query(ageStart, ageEnd).Sort("score", qx.DESC).Limit(128),
					},
					{
						name: "OrderBound_RangeResidual_Limit128",
						q:    qx.Query(ageStart, ageEnd, scoreStart).Sort("score", qx.DESC).Limit(128),
					},
					{
						name: "OR_NoOrder_Limit256",
						q: qx.Query(
							qx.OR(
								qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
								qx.AND(qx.EQ("country", "DE"), ageStart),
								qx.AND(scoreStart, scoreEnd, qx.HASANY("tags", []string{tag, "missing_tag"})),
							),
						).Limit(256),
					},
					{
						name: "OR_Order_Limit128",
						q: qx.Query(
							qx.OR(
								qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
								qx.AND(qx.EQ("country", "JP"), ageStart),
								qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
							),
						).Sort("score", qx.DESC).Limit(128),
					},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkPreparedQueryShape(b, db, tc.q)
					})
				}
			})
		}
	})
}

func BenchmarkQueryPreparedShapeOrderBoundLowCardResidual(b *testing.B) {
	db := newTestDB(b, qexecBenchOptions())
	countries := [...]string{"US", "DE", "NL", "PL", "BR", "JP", "IN", "CA"}
	names := [...]string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "mallory"}
	tags := []string{"tag_broad", "go"}
	seedQexecBenchData(b, db, qexecBenchRows, func(id uint64) testRec {
		return testRec{
			Meta:     Meta{Country: countries[id&7]},
			Name:     names[(id>>3)&7],
			Email:    qexecBenchHighCardEmail(id),
			Age:      18 + int(id%60),
			Score:    float64((id*7919)%10_000) + float64(id&15)/16,
			Active:   id&1 == 0,
			Tags:     tags,
			FullName: "user-broad",
		}
	})

	cases := []struct {
		name  string
		lo    int
		hi    int
		limit int
	}{
		{name: "Rows100K/OrderLimit/OrderBound_LowCardRangeResidual", lo: 25, hi: 40, limit: 100},
		{name: "Rows100K/OrderLimit/OrderBound_LowCardRangeResidual_BelowThreshold", lo: 25, hi: 27, limit: 100},
		{name: "Rows100K/OrderLimit/OrderBound_LowCardRangeResidual_AboveThreshold", lo: 25, hi: 28, limit: 100},
	}
	for i := range cases {
		tc := cases[i]
		b.Run(tc.name, func(b *testing.B) {
			q := qx.Query(
				qx.GTE("age", tc.lo),
				qx.LTE("age", tc.hi),
				qx.GT("score", 0.5),
			).Sort("score", qx.DESC).Limit(tc.limit)
			benchmarkPreparedQueryShape(b, db, q)
		})
	}
}

func BenchmarkQueryPreparedShapeOrderBoundHighCardResidualControl(b *testing.B) {
	db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	ageStart, ageEnd := qexecBenchAgeRange(qexecBenchRows, qexecBenchAllSelectivities[1])

	b.Run("Rows100K/OrderLimit/OrderBound_HighCardRangeResidual_Control", func(b *testing.B) {
		q := qx.Query(
			ageStart,
			ageEnd,
			qx.GTE("score", 1000.0),
		).Sort("score", qx.ASC).Limit(100)
		benchmarkPreparedQueryShape(b, db, q)
	})
}

func BenchmarkQueryPreparedShapeOrderBoundMixedResiduals(b *testing.B) {
	db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	ageStart, _ := qexecBenchAgeRange(qexecBenchRows, qexecBenchAllSelectivities[2])
	scoreStart, _ := qexecBenchScoreRange(qexecBenchRows, qexecBenchAllSelectivities[2])

	b.Run("Rows100K/OrderLimit/OrderBound_MixedResiduals_Limit50", func(b *testing.B) {
		q := qx.Query(
			qx.EQ("active", true),
			ageStart,
			scoreStart,
		).Sort("score", qx.DESC).Limit(50)
		benchmarkPreparedQueryShape(b, db, q)
	})
}

func BenchmarkQueryPreparedShapeOrderBoundNegativeOffset(b *testing.B) {
	db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	_, end := qexecBenchRange(qexecBenchRows, qexecBenchAllSelectivities[2])

	b.Run("Rows100K/OrderLimit/OrderBound_NegativeResiduals_Offset", func(b *testing.B) {
		q := qx.Query(
			qx.LT("age", end),
			qx.NOTIN("country", []string{"DE", "PL"}),
			qx.NOTIN("name", []string{"alice", "bob"}),
			qx.LT("score", float64(end)),
		).Sort("age", qx.ASC).Offset(2500).Limit(100)
		benchmarkPreparedQueryShape(b, db, q)
	})
}

func BenchmarkQueryPreparedShapePrefixOrderLimit(b *testing.B) {
	db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)

	b.Run("Rows100K/OrderLimit/PrefixOrderField_EQResidual_Limit12", func(b *testing.B) {
		q := qx.Query(
			qx.PREFIX("email", qexecBenchHighCardEmailPrefix(qexecBenchAllSelectivities[2])),
			qx.EQ("active", true),
		).Sort("email", qx.ASC).Limit(12)
		benchmarkPreparedQueryShape(b, db, q)
	})
}

func BenchmarkQueryPreparedShapeNoOrderBroadRangeLead(b *testing.B) {
	db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	broadStart, broadEnd := qexecBenchRange(qexecBenchRows, qexecBenchAllSelectivities[2])
	narrowStart, narrowEnd := qexecBenchRange(qexecBenchRows, qexecBenchAllSelectivities[0])
	gateLimit := 128
	gateWidth := gateLimit * limitRangeNoOrderBroadResidualRowsPerNeed
	gateStart := (qexecBenchRows - gateWidth) / 2
	gateBelowEnd := gateStart + gateWidth
	gateAboveEnd := gateBelowEnd + 1

	cases := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "Rows100K/NoOrderLimit/BroadRangeLead_ThreeSelectiveResiduals_BelowThreshold_Control",
			q: qx.Query(
				qx.GTE("age", gateStart),
				qx.LT("age", gateBelowEnd),
				qx.EQ("country", "US"),
				qx.EQ("name", "alice"),
				qx.HASANY("tags", []string{"tag_tiny", "missing_tag"}),
			).Limit(gateLimit),
		},
		{
			name: "Rows100K/NoOrderLimit/BroadRangeLead_ThreeSelectiveResiduals_AboveThreshold",
			q: qx.Query(
				qx.GTE("age", gateStart),
				qx.LT("age", gateAboveEnd),
				qx.EQ("country", "US"),
				qx.EQ("name", "alice"),
				qx.HASANY("tags", []string{"tag_tiny", "missing_tag"}),
			).Limit(gateLimit),
		},
		{
			name: "Rows100K/NoOrderLimit/BroadRangeLead_ThreeSelectiveResiduals",
			q: qx.Query(
				qx.GTE("age", broadStart),
				qx.LT("age", broadEnd),
				qx.EQ("country", "US"),
				qx.EQ("name", "alice"),
				qx.HASANY("tags", []string{"tag_tiny", "missing_tag"}),
			).Limit(128),
		},
		{
			name: "Rows100K/NoOrderLimit/BroadRangeLead_ThreeWeakResiduals_Control",
			q: qx.Query(
				qx.GTE("age", broadStart),
				qx.LT("age", broadEnd),
				qx.EQ("active", true),
				qx.HASANY("tags", []string{"tag_broad", "go"}),
				qx.PREFIX("full_name", "user-broad"),
			).Limit(128),
		},
		{
			name: "Rows100K/NoOrderLimit/NarrowRangeLead_ThreeResiduals_Control",
			q: qx.Query(
				qx.GTE("age", narrowStart),
				qx.LT("age", narrowEnd),
				qx.EQ("active", true),
				qx.HASANY("tags", []string{"tag_broad", "go"}),
				qx.PREFIX("full_name", "user-broad"),
			).Limit(32),
		},
		{
			name: "Rows100K/NoOrderLimit/BroadRangeLead_ThreeSelectiveResiduals_None",
			q: qx.Query(
				qx.GTE("age", broadStart),
				qx.LT("age", broadEnd),
				qx.EQ("country", "US"),
				qx.EQ("name", "mallory"),
				qx.HASANY("tags", []string{"tag_tiny", "missing_tag"}),
			).Limit(128),
		},
		{
			name: "Rows100K/NoOrderLimit/MissingEqualityResidual",
			q: qx.Query(
				qx.GTE("age", broadStart),
				qx.LT("age", broadEnd),
				qx.EQ("country", "ZZ"),
			).Limit(128),
		},
	}
	for i := range cases {
		tc := cases[i]
		b.Run(tc.name, func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, tc.q)
		})
	}
}

func BenchmarkQueryPreparedShapeLowCardOrderBucket(b *testing.B) {
	db := newTestDB(b, qexecBenchOptions())
	earlyTags := []string{"bucket_early", "mix"}
	lateTags := []string{"bucket_late", "mix"}
	sparseTags := []string{"bucket_sparse", "mix"}
	otherTags := []string{"other"}

	seedQexecBenchData(b, db, qexecBenchRows, func(id uint64) testRec {
		bucket := int((id - 1) % 100)
		pos := int((id - 1) / 100)
		country := "CA"
		name := "other"
		tags := otherTags

		// Score has 100 order buckets, so residual checks dominate inside tied buckets.
		if bucket < 8 {
			country = "US"
			name = "early"
			tags = earlyTags
		} else if bucket >= 92 {
			country = "DE"
			name = "late"
			tags = lateTags
		}
		if pos == 10 || pos == 20 {
			country = "NL"
			name = "sparse"
			tags = sparseTags
		}

		return testRec{
			Meta:     Meta{Country: country},
			Name:     name,
			Email:    qexecBenchHighCardEmail(id),
			Age:      int(id),
			Score:    float64(bucket),
			Active:   pos&1 == 0,
			Tags:     tags,
			FullName: "low-card-order",
		}
	})

	cases := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "Rows100K/OrderLimit/LowCardOrderBucket_MixedResiduals_Early",
			q: qx.Query(
				qx.EQ("country", "US"),
				qx.HASANY("tags", []string{"bucket_early", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/LowCardOrderBucket_MixedResiduals_Late",
			q: qx.Query(
				qx.EQ("country", "DE"),
				qx.HASANY("tags", []string{"bucket_late", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/LowCardOrderBucket_MixedResiduals_Sparse",
			q: qx.Query(
				qx.EQ("country", "NL"),
				qx.HASANY("tags", []string{"bucket_sparse", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/LowCardOrderBucket_MixedResiduals_None",
			q: qx.Query(
				qx.EQ("country", "BR"),
				qx.HASANY("tags", []string{"bucket_none", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
	}
	for i := range cases {
		tc := cases[i]
		b.Run(tc.name, func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, tc.q)
		})
	}
}

func BenchmarkQueryPreparedShapeUniqueOrderCorrelation(b *testing.B) {
	db := newTestDB(b, qexecBenchOptions())
	earlyTags := []string{"corr_early", "mix"}
	lateTags := []string{"corr_late", "mix"}
	sparseTags := []string{"corr_sparse", "mix"}
	otherTags := []string{"other"}

	seedQexecBenchData(b, db, qexecBenchRows, func(id uint64) testRec {
		country := "CA"
		name := "other"
		tags := otherTags

		// Early, late, and sparse variants keep roughly 2K residual matches each.
		switch {
		case id <= 2048:
			country = "US"
			name = "corr_early"
			tags = earlyTags
		case id > qexecBenchRows-2048:
			country = "DE"
			name = "corr_late"
			tags = lateTags
		case id%48 == 0:
			country = "NL"
			name = "corr_sparse"
			tags = sparseTags
		}

		return testRec{
			Meta:     Meta{Country: country},
			Name:     name,
			Email:    qexecBenchHighCardEmail(id),
			Age:      int(id),
			Score:    float64(id),
			Active:   id&1 == 0,
			Tags:     tags,
			FullName: "unique-order",
		}
	})

	cases := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "Rows100K/OrderLimit/UniqueOrder_Residuals_CorrelatedEarly",
			q: qx.Query(
				qx.EQ("name", "corr_early"),
				qx.HASANY("tags", []string{"corr_early", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/UniqueOrder_Residuals_AntiCorrelatedLate",
			q: qx.Query(
				qx.EQ("name", "corr_late"),
				qx.HASANY("tags", []string{"corr_late", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/UniqueOrder_Residuals_Sparse",
			q: qx.Query(
				qx.EQ("name", "corr_sparse"),
				qx.HASANY("tags", []string{"corr_sparse", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/UniqueOrder_Residuals_None",
			q: qx.Query(
				qx.EQ("name", "corr_none"),
				qx.HASANY("tags", []string{"corr_none", "missing_tag"}),
			).Sort("score", qx.ASC).Limit(128),
		},
	}
	for i := range cases {
		tc := cases[i]
		b.Run(tc.name, func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, tc.q)
		})
	}
}

func BenchmarkQueryPreparedShapeOrderBoundMissingResidual(b *testing.B) {
	db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	cases := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "Rows100K/OrderLimit/OrderBound_MissingResidual",
			q: qx.Query(
				qx.GTE("score", 1000.0),
				qx.LT("score", 90_000.0),
				qx.EQ("country", "ZZ"),
			).Sort("score", qx.ASC).Limit(128),
		},
		{
			name: "Rows100K/OrderLimit/OrderBound_RangeResidual_ExactAbsent",
			q: qx.Query(
				qx.GTE("score", 1000.0),
				qx.LT("score", 90_000.0),
				qx.GTE("age", 20_000),
				qx.LT("age", 80_000),
				qx.EQ("country", "ZZ"),
			).Sort("score", qx.ASC).Limit(128),
		},
	}
	for i := range cases {
		tc := cases[i]
		b.Run(tc.name, func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, tc.q)
		})
	}
}

func BenchmarkPlannerStats(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, _ qexecBenchScale) {
		b.Run("BuildPlannerStatsSnapshot", func(b *testing.B) {
			var out *PlannerStatsSnapshot
			snap := db.snap
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out = db.exec.BuildPlannerStatsSnapshot(snap, uint64(i+1))
				qexecBenchCount = uint64(len(out.Fields))
			}
		})

		b.Run("RefreshPlannerStatsFull", func(b *testing.B) {
			snap := db.snap
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.exec.RefreshPlannerStatsOnSnapshot(snap, 0, false)
			}
			if cur := db.exec.Stats.Load(); cur != nil {
				qexecBenchCount = uint64(len(cur.Fields))
			}
		})

		b.Run("RefreshPlannerStatsCursorBudget", func(b *testing.B) {
			snap := db.snap
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.exec.RefreshPlannerStatsOnSnapshot(snap, time.Nanosecond, true)
			}
			if cur := db.exec.Stats.Load(); cur != nil {
				qexecBenchCount = uint64(len(cur.Fields))
			}
		})
	})

	b.Run("Rows100K/FieldStatsUniqueFastPath", func(b *testing.B) {
		db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
		view := db.view()
		ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
		if !ov.HasData() {
			b.Fatal("missing age index")
		}
		var stats PlannerFieldStats
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stats = plannerFieldIndexViewStats(ov)
		}
		qexecBenchCount = stats.TotalBucketCard
	})

	b.Run("Rows100K/FieldStatsNonUniqueScan", func(b *testing.B) {
		db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
		view := db.view()
		ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "country")
		if !ov.HasData() {
			b.Fatal("missing country index")
		}
		var stats PlannerFieldStats
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stats = plannerFieldIndexViewStats(ov)
		}
		qexecBenchCount = stats.TotalBucketCard
	})

}

func benchmarkPreparedCardinality(b *testing.B, db *testDB, expr qx.Expr) {
	b.Helper()

	prepared, err := qir.PrepareCountExprsResolved(db.rt.IndexedByName, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprsResolved: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	cnt, err := view.exactExprCardinality(prepared.Expr)
	if err != nil {
		b.Fatalf("exactExprCardinality warmup: %v", err)
	}
	qexecBenchCount = cnt

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cnt, err = view.exactExprCardinality(prepared.Expr)
		if err != nil {
			b.Fatalf("exactExprCardinality: %v", err)
		}
		qexecBenchCount = cnt
	}
	b.ReportMetric(float64(qexecBenchCount), "rows/op")
}

func BenchmarkCardinalityPreparedExpr(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		b.Run("ScalarEQ", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, qx.EQ("country", "US"))
		})

		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				prefix := qexecBenchPrefix(sel)
				cases := []struct {
					name string
					expr qx.Expr
				}{
					{
						name: "SliceHASANY",
						expr: qx.HASANY("tags", []string{tag, "missing_tag"}),
					},
					{
						name: "ScalarINSplit",
						expr: qx.AND(
							qx.IN("country", []string{"US", "DE", "NL"}),
							ageStart,
							ageEnd,
						),
					},
					{
						name: "AndPredicates",
						expr: qx.AND(
							qx.EQ("active", true),
							ageStart,
							ageEnd,
							qx.HASANY("tags", []string{tag, "missing_tag"}),
						),
					},
					{
						name: "ORPredicates",
						expr: qx.OR(
							qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
							qx.AND(qx.EQ("country", "JP"), ageStart),
							qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
						),
					},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkPreparedCardinality(b, db, tc.expr)
					})
				}
			})
		}
	})
}

type qexecBenchCardinalityRoute uint8

const (
	qexecBenchCardinalityScalarLookup qexecBenchCardinalityRoute = iota
	qexecBenchCardinalitySliceLookup
	qexecBenchCardinalityUniqueEq
	qexecBenchCardinalityScalarInSplit
	qexecBenchCardinalityPreparedReordered
	qexecBenchCardinalityPredicates
	qexecBenchCardinalityORPredicates
	qexecBenchCardinalityMaterialized
)

type qexecBenchUniqueRec struct {
	Email    string   `db:"email"     rbi:"unique"`
	Country  string   `db:"country"   rbi:"index"`
	Score    int      `db:"score"     rbi:"index"`
	Active   bool     `db:"active"    rbi:"index"`
	Tags     []string `db:"tags"      rbi:"index"`
	FullName string   `db:"full_name" rbi:"index"`
}

func newQexecBenchUniqueDB(b *testing.B) *DB[uint64, qexecBenchUniqueRec] {
	b.Helper()

	db, raw := openBoltAndNew[uint64, qexecBenchUniqueRec](b, b.TempDir()+"/qexec_bench_unique.db", Options{})
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	ids := make([]uint64, qexecBenchRows)
	rows := make([]qexecBenchUniqueRec, qexecBenchRows)
	vals := make([]*qexecBenchUniqueRec, qexecBenchRows)
	countries := [...]string{"US", "DE", "NL", "PL"}
	for i := 0; i < qexecBenchRows; i++ {
		id := uint64(i + 1)
		tags := []string{"java"}
		switch id & 3 {
		case 0:
			tags = []string{"go", "db"}
		case 1:
			tags = []string{"ops"}
		case 2:
			tags = []string{"security", "go"}
		}
		ids[i] = id
		rows[i] = qexecBenchUniqueRec{
			Email:    "unique" + strconv.FormatUint(id, 10) + "@example.test",
			Country:  countries[id&3],
			Score:    int(id),
			Active:   id&1 == 0,
			Tags:     tags,
			FullName: "unique-" + strconv.FormatUint(id+1_000_000, 10)[1:],
		}
		vals[i] = &rows[i]
	}
	if err := db.BatchSet(ids, vals); err != nil {
		b.Fatalf("BatchSet: %v", err)
	}
	return db
}

func runQexecBenchCardinalityRoute(view *View, expr qir.Expr, route qexecBenchCardinalityRoute, trace *Trace) (uint64, bool, error) {
	switch route {
	case qexecBenchCardinalityScalarLookup:
		return view.TryFilterCardinalityByScalarLookup(expr, trace)
	case qexecBenchCardinalitySliceLookup:
		return view.TryFilterCardinalityBySliceLookup(expr, trace)
	case qexecBenchCardinalityUniqueEq:
		return view.TryFilterCardinalityByUniqueEq(expr, trace)
	case qexecBenchCardinalityScalarInSplit:
		return view.TryFilterCardinalityByScalarInSplit(expr, trace)
	case qexecBenchCardinalityPreparedReordered:
		return view.TryFilterCardinalityPreparedAndReordered(expr)
	case qexecBenchCardinalityPredicates:
		return view.TryFilterCardinalityByPredicates(expr, trace)
	case qexecBenchCardinalityORPredicates:
		return view.TryFilterCardinalityORByPredicates(expr, trace)
	case qexecBenchCardinalityMaterialized:
		cnt, err := view.FilterCardinalityByMaterializedExpr(expr, trace)
		return cnt, true, err
	default:
		return 0, false, nil
	}
}

func benchmarkCardinalityRoute(b *testing.B, view *View, resolve qir.FieldResolver, expr qx.Expr, route qexecBenchCardinalityRoute, wantPlan PlanName) {
	b.Helper()

	prepared, err := qir.PrepareCountExprsResolved(resolve, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprsResolved: %v", err)
	}
	defer prepared.Release()

	view.snap.ClearRuntimeCaches()
	cnt, ok, err := runQexecBenchCardinalityRoute(view, prepared.Expr, route, nil)
	if err != nil {
		b.Fatalf("cardinality route warmup: %v", err)
	}
	if !ok {
		b.Fatalf("cardinality route warmup was not used")
	}
	if wantPlan != "" {
		trace := &Trace{}
		cnt, ok, err = runQexecBenchCardinalityRoute(view, prepared.Expr, route, trace)
		if err != nil {
			b.Fatalf("cardinality route trace warmup: %v", err)
		}
		if !ok {
			b.Fatalf("cardinality route trace warmup was not used")
		}
		if got := trace.Event().Plan; got != string(wantPlan) {
			b.Fatalf("trace plan=%q, want %q", got, wantPlan)
		}
	}
	qexecBenchCount = cnt

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cnt, ok, err = runQexecBenchCardinalityRoute(view, prepared.Expr, route, nil)
		if err != nil {
			b.Fatalf("cardinality route: %v", err)
		}
		if !ok {
			b.Fatalf("cardinality route was not used")
		}
		qexecBenchCount = cnt
	}
	b.ReportMetric(float64(qexecBenchCount), "rows/op")
}

func BenchmarkCardinalityRoutes(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		view := db.view()
		fixed := []struct {
			name  string
			expr  qx.Expr
			route qexecBenchCardinalityRoute
			plan  PlanName
		}{
			{name: "ScalarLookup_EQ", expr: qx.EQ("country", "US"), route: qexecBenchCardinalityScalarLookup, plan: planFilterCardinalityScalarLookup},
			{name: "ScalarLookup_IN", expr: qx.IN("country", []string{"US", "DE", "NL", "PL"}), route: qexecBenchCardinalityScalarLookup, plan: planFilterCardinalityScalarLookup},
			{name: "ScalarLookup_EQNil", expr: qx.EQ("opt", nil), route: qexecBenchCardinalityScalarLookup, plan: planFilterCardinalityScalarLookup},
		}
		for i := range fixed {
			tc := fixed[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkCardinalityRoute(b, view, db.rt.IndexedByName, tc.expr, tc.route, tc.plan)
			})
		}

		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				scoreStart, scoreEnd := qexecBenchScoreRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				prefix := qexecBenchPrefix(sel)
				cases := []struct {
					name  string
					expr  qx.Expr
					route qexecBenchCardinalityRoute
					plan  PlanName
				}{
					{name: "SliceLookup_HASANY2", expr: qx.HASANY("tags", []string{tag, "missing_tag"}), route: qexecBenchCardinalitySliceLookup, plan: planFilterCardinalityScalarLookup},
					{name: "SliceLookup_HASALL", expr: qx.HASALL("tags", []string{tag, "go"}), route: qexecBenchCardinalitySliceLookup, plan: planFilterCardinalityScalarLookup},
					{
						name: "ScalarInSplit_Filtered",
						expr: qx.AND(
							qx.IN("country", []string{"US", "DE", "NL"}),
							ageStart,
							ageEnd,
						),
						route: qexecBenchCardinalityScalarInSplit,
						plan:  planFilterCardinalityScalarInSplit,
					},
					{
						name: "PreparedReordered_SetRange",
						expr: qx.AND(
							qx.HASANY("tags", []string{tag, "missing_tag"}),
							ageStart,
							ageEnd,
						),
						route: qexecBenchCardinalityPreparedReordered,
					},
					{
						name: "Predicates_LeadPosting",
						expr: qx.AND(
							qx.EQ("country", "US"),
							qx.HASANY("tags", []string{tag, "missing_tag"}),
							ageStart,
							ageEnd,
						),
						route: qexecBenchCardinalityPredicates,
						plan:  planFilterCardinalityPredicates,
					},
					{
						name: "ORPredicates_BranchScans",
						expr: qx.OR(
							qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("country", "US")),
							qx.AND(qx.EQ("country", "DE"), ageStart),
							qx.AND(qx.EQ("country", "JP"), scoreStart, scoreEnd),
							qx.AND(qx.EQ("country", "NL"), qx.EQ("active", true)),
						),
						route: qexecBenchCardinalityORPredicates,
						plan:  planFilterCardinalityORPredicates,
					},
					{
						name: "Materialized_AND",
						expr: qx.AND(
							qx.EQ("active", true),
							qx.HASANY("tags", []string{tag, "missing_tag"}),
							ageStart,
							ageEnd,
						),
						route: qexecBenchCardinalityMaterialized,
						plan:  planFilterCardinalityMaterialized,
					},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkCardinalityRoute(b, view, db.rt.IndexedByName, tc.expr, tc.route, tc.plan)
					})
				}
			})
		}
	})

	hybridDB := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	hybridView := hybridDB.view()
	ageStart, _ := qexecBenchRange(qexecBenchRows, qexecBenchAllSelectivities[1])
	_, scoreEnd := qexecBenchRange(qexecBenchRows, qexecBenchAllSelectivities[1])
	b.Run("Rows100K/ORPredicates_HybridSpill", func(b *testing.B) {
		benchmarkCardinalityRoute(b, hybridView, hybridDB.rt.IndexedByName, qx.OR(
			qx.AND(qx.PREFIX("full_name", "user-tiny"), qx.EQ("country", "US")),
			qx.HASANY("tags", []string{"tag_broad", "tag_mid"}),
			qx.AND(qx.EQ("country", "DE"), qx.GTE("age", ageStart)),
			qx.AND(qx.EQ("country", "JP"), qx.LT("score", float64(scoreEnd))),
		), qexecBenchCardinalityORPredicates, planFilterCardinalityORHybrid)
	})
	b.Run("Rows100K/ORPredicates_DisjointScalarEQ", func(b *testing.B) {
		benchmarkCardinalityRoute(b, hybridView, hybridDB.rt.IndexedByName, qx.OR(
			qx.AND(qx.PREFIX("full_name", "user-tiny"), qx.EQ("country", "US")),
			qx.AND(qx.EQ("country", "DE"), qx.GTE("age", ageStart)),
			qx.AND(qx.EQ("country", "NL"), qx.EQ("active", true)),
		), qexecBenchCardinalityORPredicates, planFilterCardinalityORPredicates)
	})
	b.Run("Rows100K/ORPredicates_OverlappingScalarEQ", func(b *testing.B) {
		benchmarkCardinalityRoute(b, hybridView, hybridDB.rt.IndexedByName, qx.OR(
			qx.AND(qx.PREFIX("full_name", "user-tiny"), qx.EQ("country", "US")),
			qx.AND(qx.EQ("country", "US"), qx.GTE("age", ageStart)),
			qx.AND(qx.EQ("country", "DE"), qx.EQ("active", true)),
			qx.AND(qx.EQ("country", "JP"), qx.LT("score", float64(scoreEnd))),
		), qexecBenchCardinalityORPredicates, "")
	})

	uniqueDB := newQexecBenchUniqueDB(b)
	uniqueView := uniqueDB.engine.currentQueryViewForTests()
	b.Run("Rows100K/UniqueEq_ANDResiduals", func(b *testing.B) {
		benchmarkCardinalityRoute(
			b,
			uniqueView,
			uniqueDB.engine.schema.IndexedByName,
			qx.AND(
				qx.EQ("email", "unique32768@example.test"),
				qx.EQ("active", true),
				qx.GTE("score", 20_000),
				qx.LT("score", 50_000),
			),
			qexecBenchCardinalityUniqueEq,
			planFilterCardinalityUniqueEq,
		)
	})
}

type qexecBenchCardinalityPredicateExecutor uint8

const (
	qexecBenchCardinalityLeadBuckets qexecBenchCardinalityPredicateExecutor = iota
	qexecBenchCardinalityLeadPostings
)

func benchmarkCardinalityPredicateExecutor(b *testing.B, db *testDB, expr qx.Expr, route qexecBenchCardinalityPredicateExecutor) {
	b.Helper()

	prepared, err := qir.PrepareCountExprsResolved(db.rt.IndexedByName, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprsResolved: %v", err)
	}
	defer prepared.Release()

	leaves, ok := qir.CollectAndLeaves(prepared.Expr, qir.LeafModeCollect)
	if !ok {
		b.Fatalf("CollectAndLeaves failed")
	}

	view := db.view()
	preds, ok := view.buildCardinalityPredicatesWithMode(leaves, false)
	if !ok {
		b.Fatalf("buildCardinalityPredicatesWithMode failed")
	}
	defer preds.Release()

	universe := view.snap.Universe.Cardinality()
	leadIdx, leadEst, _ := view.pickCardinalityLeadPredicate(preds.owner, universe)
	if leadIdx < 0 {
		b.Fatal("cardinality lead was not selected")
	}
	if route == qexecBenchCardinalityLeadPostings {
		for i := 0; i < preds.Len(); i++ {
			p := preds.owner[i]
			if p.alwaysTrue || p.covered || p.alwaysFalse || p.expr.Not || p.expr.Op != qir.OpEQ || !p.hasIter() || p.estCard == 0 {
				continue
			}
			leadIdx = i
			leadEst = p.estCard
			break
		}
	}
	if err = view.prepareCardinalityPredicate((&preds.owner[leadIdx]), leadEst, universe); err != nil {
		b.Fatalf("prepareCardinalityPredicate lead: %v", err)
	}
	if preds.owner[leadIdx].estCard > 0 {
		leadEst = preds.owner[leadIdx].estCard
	}
	leadNeedsCheck := (&preds.owner[leadIdx]).leadIterNeedsContainsCheck()

	var activeBuf [cardinalityPredicateScanMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < preds.Len(); i++ {
		if i == leadIdx && !leadNeedsCheck {
			continue
		}
		p := preds.owner[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			b.Fatal("unexpected always-false predicate")
		}
		active = append(active, i)
	}
	for _, pi := range active {
		if err = view.prepareCardinalityPredicate((&preds.owner[pi]), leadEst, universe); err != nil {
			b.Fatalf("prepareCardinalityPredicate residual: %v", err)
		}
	}

	write := 0
	for read := 0; read < len(active); read++ {
		pi := active[read]
		p := preds.owner[pi]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			b.Fatal("unexpected always-false predicate after prepare")
		}
		if !p.hasContains() {
			b.Fatal("active predicate has no contains path")
		}
		active[write] = pi
		write++
	}
	active = active[:write]
	sortActivePredicatesReader(active, preds.owner)

	var cnt uint64
	var examined uint64
	switch route {
	case qexecBenchCardinalityLeadBuckets:
		cnt, examined, ok = view.TryFilterCardinalityByPredicatesLeadBuckets(preds, leadIdx, active)
	case qexecBenchCardinalityLeadPostings:
		cnt, examined, ok = view.TryFilterCardinalityByPredicatesLeadPostings(preds, leadIdx, active)
	}
	if !ok {
		b.Fatal("cardinality predicate executor warmup was not used")
	}
	qexecBenchCount = cnt
	qexecBenchTraceEvents = examined

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch route {
		case qexecBenchCardinalityLeadBuckets:
			cnt, examined, ok = view.TryFilterCardinalityByPredicatesLeadBuckets(preds, leadIdx, active)
		case qexecBenchCardinalityLeadPostings:
			cnt, examined, ok = view.TryFilterCardinalityByPredicatesLeadPostings(preds, leadIdx, active)
		default:
			ok = false
		}
		if !ok {
			b.Fatal("cardinality predicate executor was not used")
		}
		qexecBenchCount = cnt
		qexecBenchTraceEvents = examined
	}
	b.ReportMetric(float64(qexecBenchCount), "rows/op")
	b.ReportMetric(float64(qexecBenchTraceEvents), "examined/op")
}

func BenchmarkCardinalityPredicateExecutors(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		scoreStart, scoreEnd := qexecBenchScoreRange(scale.rows, sel)
		tag := qexecBenchTag(sel)

		b.Run("LeadBuckets_RangeResidual", func(b *testing.B) {
			benchmarkCardinalityPredicateExecutor(b, db, qx.AND(
				ageStart,
				ageEnd,
				scoreStart,
				scoreEnd,
			), qexecBenchCardinalityLeadBuckets)
		})
		b.Run("LeadBuckets_RangeResidualExactControl", func(b *testing.B) {
			benchmarkCardinalityPredicateExecutor(b, db, qx.AND(
				ageStart,
				ageEnd,
				scoreStart,
				scoreEnd,
				qx.HASANY("tags", []string{"go", "java"}),
			), qexecBenchCardinalityLeadBuckets)
		})
		b.Run("LeadPostings_EQResiduals", func(b *testing.B) {
			benchmarkCardinalityPredicateExecutor(b, db, qx.AND(
				qx.EQ("country", "US"),
				qx.HASANY("tags", []string{tag, "missing_tag"}),
				ageStart,
				ageEnd,
			), qexecBenchCardinalityLeadPostings)
		})
		b.Run("LeadPostings_ExactResidualFilters", func(b *testing.B) {
			benchmarkCardinalityPredicateExecutor(b, db, qx.AND(
				qx.EQ("country", "US"),
				qx.EQ("name", "alice"),
				qx.EQ("active", true),
				qx.HASANY("tags", []string{tag, "missing_tag"}),
				ageStart,
				ageEnd,
			), qexecBenchCardinalityLeadPostings)
		})
	})
}

func benchmarkEvalPreparedExpr(b *testing.B, db *testDB, expr qx.Expr, clearCaches bool) {
	b.Helper()

	prepared, err := qir.PrepareCountExprsResolved(db.rt.IndexedByName, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprsResolved: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	res, err := view.evalExpr(prepared.Expr)
	if err != nil {
		b.Fatalf("evalExpr warmup: %v", err)
	}
	qexecBenchCount = view.postingResultCardinality(res)
	res.ids.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if clearCaches {
			b.StopTimer()
			db.clearCurrentSnapshotCaches()
			b.StartTimer()
		}
		res, err = view.evalExpr(prepared.Expr)
		if err != nil {
			b.Fatalf("evalExpr: %v", err)
		}
		qexecBenchCount = view.postingResultCardinality(res)
		res.ids.Release()
	}
	b.ReportMetric(float64(qexecBenchCount), "rows/op")
}

func BenchmarkEvalPreparedExpr(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		fixed := []struct {
			name string
			expr qx.Expr
		}{
			{name: "ScalarEQ", expr: qx.EQ("country", "US")},
			{name: "ScalarIN", expr: qx.IN("country", []string{"US", "DE", "NL", "PL"})},
		}
		for i := range fixed {
			tc := fixed[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkEvalPreparedExpr(b, db, tc.expr, false)
			})
		}

		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				scoreStart, _ := qexecBenchScoreRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				prefix := qexecBenchPrefix(sel)
				cases := []struct {
					name string
					expr qx.Expr
				}{
					{name: "SliceHASALL", expr: qx.HASALL("tags", []string{tag, "go"})},
					{name: "RangeNumeric", expr: qx.AND(ageStart, ageEnd)},
					{name: "RangeNumericMergeControl", expr: qx.AND(ageStart, scoreStart)},
					{name: "Prefix", expr: qx.PREFIX("full_name", prefix)},
					{
						name: "ANDMixed",
						expr: qx.AND(
							qx.EQ("active", true),
							ageStart,
							ageEnd,
							qx.HASANY("tags", []string{tag, "missing_tag"}),
						),
					},
					{
						name: "ORMixed",
						expr: qx.OR(
							qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
							qx.AND(qx.EQ("country", "JP"), ageStart),
							qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
						),
					},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkEvalPreparedExpr(b, db, tc.expr, false)
					})
				}
			})
		}
	})
}

func BenchmarkStringIndexHighCardinality(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, _ qexecBenchScale, sel qexecBenchSelectivity) {
		prefix := qexecBenchHighCardEmailPrefix(sel)
		rangeEnd := prefix[:len(prefix)-1] + "."
		prefixExpr := qx.PREFIX("email", prefix)
		rangeExpr := qx.AND(qx.GTE("email", prefix), qx.LT("email", rangeEnd))

		b.Run("QueryPrefix_NoOrder_Limit128", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(prefixExpr).Limit(128))
		})
		b.Run("QueryRange_NoOrder_Limit128", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(rangeExpr).Limit(128))
		})
		b.Run("QueryPrefix_OrderScore_Limit128", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(prefixExpr).Sort("score", qx.DESC).Limit(128))
		})
		b.Run("QueryRange_OrderScore_Limit128", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(rangeExpr).Sort("score", qx.DESC).Limit(128))
		})
		b.Run("CardinalityPrefix", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, prefixExpr)
		})
		b.Run("CardinalityRange", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, rangeExpr)
		})
		b.Run("EvalPrefix", func(b *testing.B) {
			benchmarkEvalPreparedExpr(b, db, prefixExpr, false)
		})
		b.Run("EvalRange", func(b *testing.B) {
			benchmarkEvalPreparedExpr(b, db, rangeExpr, false)
		})
	})
}

func BenchmarkEvalPreparedExprColdCaches(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		scoreStart, _ := qexecBenchScoreRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		prefix := qexecBenchPrefix(sel)
		cases := []struct {
			name string
			expr qx.Expr
		}{
			{name: "RangeNumeric", expr: qx.AND(ageStart, ageEnd)},
			{name: "RangeNumericMergeControl", expr: qx.AND(ageStart, scoreStart)},
			{name: "Prefix", expr: qx.PREFIX("full_name", prefix)},
			{
				name: "ORMixed",
				expr: qx.OR(
					qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
					qx.AND(qx.EQ("country", "JP"), ageStart),
					qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
				),
			},
		}
		for i := range cases {
			tc := cases[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkEvalPreparedExpr(b, db, tc.expr, true)
			})
		}
	})
}

func benchmarkBuildPredicates(b *testing.B, db *testDB, expr qx.Expr, kind string) {
	b.Helper()

	prepared, err := qir.PrepareCountExprsResolved(db.rt.IndexedByName, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprsResolved: %v", err)
	}
	defer prepared.Release()

	leaves, ok := qir.CollectAndLeaves(prepared.Expr, qir.LeafModeCollect)
	if !ok {
		b.Fatalf("CollectAndLeaves failed")
	}

	db.clearCurrentSnapshotCaches()
	view := db.view()
	var predCount int

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var preds predicateSet
		switch kind {
		case "ordered":
			preds, ok = view.buildPredicatesOrdered(leaves, "score")
		case "cardinality":
			preds, ok = view.buildCardinalityPredicatesWithMode(leaves, true)
		default:
			preds, ok = view.buildPredicates(leaves)
		}
		if !ok {
			b.Fatalf("buildPredicates(%s) failed", kind)
		}
		predCount = preds.Len()
		preds.Release()
	}
	b.ReportMetric(float64(predCount), "preds/op")
}

func BenchmarkBuildPredicates(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		scoreStart, scoreEnd := qexecBenchScoreRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		unordered := qx.AND(
			qx.EQ("country", "US"),
			qx.EQ("active", true),
			ageStart,
			ageEnd,
			qx.HASANY("tags", []string{tag, "missing_tag"}),
		)
		ordered := qx.AND(
			scoreStart,
			scoreEnd,
			qx.EQ("country", "US"),
			qx.HASANY("tags", []string{tag, "missing_tag"}),
		)

		b.Run("Query_Unordered", func(b *testing.B) {
			benchmarkBuildPredicates(b, db, unordered, "")
		})
		b.Run("Query_Unordered_RangeMergeControl", func(b *testing.B) {
			benchmarkBuildPredicates(b, db, qx.AND(
				qx.EQ("country", "US"),
				qx.EQ("active", true),
				ageStart,
				scoreStart,
				qx.HASANY("tags", []string{tag, "missing_tag"}),
			), "")
		})
		b.Run("Query_Ordered", func(b *testing.B) {
			benchmarkBuildPredicates(b, db, ordered, "ordered")
		})
		b.Run("Cardinality", func(b *testing.B) {
			benchmarkBuildPredicates(b, db, unordered, "cardinality")
		})
	})
}

func benchmarkBuildORBranches(b *testing.B, db *testDB, q *qx.QX, ordered bool) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()
	if shape.Expr.Op != qir.OpOR {
		b.Fatalf("expected OR query")
	}

	window, _ := orderWindow(&shape)
	orderField := ""
	if shape.HasOrder {
		orderField = db.exec.FieldNameByOrdinal(shape.Order.FieldOrdinal)
	}

	db.clearCurrentSnapshotCaches()
	view := db.view()
	var branchCount int

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var branches plannerORBranches
		var alwaysFalse bool
		var ok bool
		if ordered {
			branches, alwaysFalse, ok = view.buildORBranchesOrdered(shape.Expr.Operands, orderField, window, shape.Offset)
		} else {
			branches, alwaysFalse, ok = view.buildORBranches(shape.Expr.Operands)
		}
		if !ok {
			b.Fatalf("buildORBranches failed")
		}
		if alwaysFalse {
			branchCount = 0
		} else {
			branchCount = branches.Len()
			branches.Release()
		}
	}
	b.ReportMetric(float64(branchCount), "branches/op")
}

func BenchmarkBuildORBranches(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		prefix := qexecBenchPrefix(sel)
		noOrder := qx.Query(
			qx.OR(
				qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
				qx.AND(qx.EQ("country", "DE"), ageStart),
				qx.AND(ageStart, ageEnd),
			),
		).Limit(256)
		ordered := qx.Query(
			qx.OR(
				qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
				qx.AND(qx.EQ("country", "JP"), ageStart),
				qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
			),
		).Sort("score", qx.DESC).Limit(128)

		b.Run("NoOrder", func(b *testing.B) {
			benchmarkBuildORBranches(b, db, noOrder, false)
		})
		b.Run("Ordered", func(b *testing.B) {
			benchmarkBuildORBranches(b, db, ordered, true)
		})
	})
}

func benchmarkORPlannerDecision(b *testing.B, db *testDB, q *qx.QX, ordered bool) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()
	if shape.Expr.Op != qir.OpOR {
		b.Fatalf("expected OR query")
	}

	view := db.view()
	window, _ := orderWindow(&shape)
	orderField := ""
	if shape.HasOrder {
		orderField = db.exec.FieldNameByOrdinal(shape.Order.FieldOrdinal)
	}

	var branches plannerORBranches
	var alwaysFalse bool
	var ok bool
	if ordered {
		branches, alwaysFalse, ok = view.buildORBranchesOrdered(shape.Expr.Operands, orderField, window, shape.Offset)
	} else {
		branches, alwaysFalse, ok = view.buildORBranches(shape.Expr.Operands)
	}
	if !ok {
		b.Fatalf("buildORBranches failed")
	}
	if alwaysFalse {
		b.Fatalf("unexpected always-false OR")
	}
	defer branches.Release()

	var decisionRows uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ordered {
			d := view.decidePlanOROrder(&shape, branches)
			decisionRows = d.expectedRows
		} else {
			d := view.decidePlanORNoOrder(&shape, branches)
			decisionRows = d.expectedRows
		}
	}
	qexecBenchCount = decisionRows
}

func BenchmarkORPlannerDecision(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		prefix := qexecBenchPrefix(sel)
		noOrder := qx.Query(
			qx.OR(
				qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
				qx.AND(qx.EQ("country", "DE"), ageStart),
				qx.AND(ageStart, ageEnd),
			),
		).Limit(256)
		ordered := qx.Query(
			qx.OR(
				qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
				qx.AND(qx.EQ("country", "JP"), ageStart),
				qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
			),
		).Sort("score", qx.DESC).Limit(128)

		b.Run("NoOrder", func(b *testing.B) {
			benchmarkORPlannerDecision(b, db, noOrder, false)
		})
		b.Run("Ordered", func(b *testing.B) {
			benchmarkORPlannerDecision(b, db, ordered, true)
		})
	})
}

func benchmarkTryPlanOrdered(b *testing.B, db *testDB, q *qx.QX, wantPlan PlanName) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	trace := &Trace{}
	out, ok, err := view.tryPlanOrdered(&shape, trace)
	if err != nil {
		b.Fatalf("tryPlanOrdered warmup: %v", err)
	}
	if !ok {
		b.Skip("ordered plan path is not used for this shape")
	}
	if wantPlan != "" && trace.Event().Plan != string(wantPlan) {
		b.Skip("requested ordered plan variant is not used for this shape")
	}
	qexecBenchIDs = out

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok, err = view.tryPlanOrdered(&shape, nil)
		if err != nil {
			b.Fatalf("tryPlanOrdered: %v", err)
		}
		if !ok {
			b.Fatal("ordered plan path was not used")
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func benchmarkExecPlanOrderedBasicReader(b *testing.B, db *testDB, q *qx.QX, wantPlan PlanName) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	leaves, ok := qir.CollectAndLeaves(shape.Expr, qir.LeafModeCollect)
	if !ok {
		b.Fatalf("CollectAndLeaves failed")
	}
	view := db.view()
	orderField := db.exec.FieldNameByOrdinal(shape.Order.FieldOrdinal)
	preds, ok := view.buildPredicatesOrdered(leaves, orderField)
	if !ok {
		b.Fatalf("buildPredicatesOrdered failed")
	}
	defer preds.Release()

	var inline [plannerPredicateFastPathMaxLeaves]predicate
	var work []predicate
	if preds.Len() <= len(inline) {
		work = inline[:preds.Len()]
	} else {
		work = make([]predicate, preds.Len())
	}
	copy(work, preds.owner)
	trace := &Trace{}
	out, ok := view.execPlanOrderedBasicReader(&shape, work, trace)
	if !ok {
		b.Skip("ordered basic reader path is not used for this shape")
	}
	if wantPlan != "" && trace.Event().Plan != string(wantPlan) {
		b.Skip("requested ordered basic reader variant is not used for this shape")
	}
	qexecBenchIDs = out

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(work, preds.owner)
		out, ok = view.execPlanOrderedBasicReader(&shape, work, nil)
		if !ok {
			b.Fatal("ordered basic reader path was not used")
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkOrderedPlannerInternals(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		noOrder := qx.Query(
			qx.EQ("active", true),
			qx.NOTIN("country", []string{"NL", "DE"}),
			qx.IN("name", []string{"alice", "bob", "carol"}),
			ageStart,
			ageEnd,
		).Limit(90)
		basic := qx.Query(
			qx.EQ("country", "US"),
			ageStart,
			ageEnd,
		).Sort("score", qx.DESC).Limit(128)
		anchored := qx.Query(
			qx.EQ("country", "US"),
			qx.EQ("active", true),
			qx.HASANY("tags", []string{tag, "go"}),
			ageStart,
			ageEnd,
		).Sort("score", qx.DESC).Limit(128)

		b.Run("TryPlanOrdered_NoOrderLeadScan", func(b *testing.B) {
			benchmarkTryPlanOrdered(b, db, noOrder, PlanOrderedNoOrder)
		})
		b.Run("ExecBasicReader", func(b *testing.B) {
			benchmarkExecPlanOrderedBasicReader(b, db, basic, "")
		})
		b.Run("ExecBasicReader_Anchor", func(b *testing.B) {
			benchmarkExecPlanOrderedBasicReader(b, db, anchored, PlanOrderedAnchor)
		})
	})

	qexecBenchRunScales(b, func(b *testing.B, db *testDB, _ qexecBenchScale) {
		nullable := qx.Query(
			qx.EQ("country", "US"),
			qx.EQ("active", true),
		).Sort("opt", qx.ASC).Limit(128)
		b.Run("NullableFallbackExactNilTail", func(b *testing.B) {
			benchmarkExecPlanOrderedBasicReader(b, db, nullable, "")
		})
	})
}

func benchmarkPreparedQueryShapeColdCaches(b *testing.B, db *testDB, q *qx.QX) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	var out []uint64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.clearCurrentSnapshotCaches()
		b.StartTimer()
		out, err = view.Query(&shape, false, false)
		if err != nil {
			b.Fatalf("Query: %v", err)
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkQueryPreparedShapeColdCaches(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		_, end := qexecBenchRange(scale.rows, sel)
		scoreStart, _ := qexecBenchScoreRange(scale.rows, sel)
		emailPrefix := qexecBenchHighCardEmailPrefix(sel)
		tag := qexecBenchTag(sel)
		prefix := qexecBenchPrefix(sel)
		cases := []struct {
			name string
			q    *qx.QX
		}{
			{name: "Range_NoOrder_Limit128", q: qx.Query(ageStart, ageEnd).Limit(128)},
			{name: "Range_Order_Limit128", q: qx.Query(ageStart, ageEnd).Sort("score", qx.DESC).Limit(128)},
			{
				name: "OrderBound_MixedResiduals_Limit50",
				q: qx.Query(
					qx.EQ("active", true),
					ageStart,
					scoreStart,
				).Sort("score", qx.DESC).Limit(50),
			},
			{
				name: "OrderBound_NegativeResiduals_Offset",
				q: qx.Query(
					qx.LT("age", end),
					qx.NOTIN("country", []string{"DE", "PL"}),
					qx.NOTIN("name", []string{"alice", "bob"}),
					qx.LT("score", float64(end)),
				).Sort("age", qx.ASC).Offset(2500).Limit(100),
			},
			{
				name: "PrefixOrderField_EQResidual_Limit12",
				q: qx.Query(
					qx.PREFIX("email", emailPrefix),
					qx.EQ("active", true),
				).Sort("email", qx.ASC).Limit(12),
			},
			{
				name: "OR_Order_Limit128",
				q: qx.Query(
					qx.OR(
						qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
						qx.AND(qx.EQ("country", "JP"), ageStart),
						qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
					),
				).Sort("score", qx.DESC).Limit(128),
			},
		}
		for i := range cases {
			tc := cases[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkPreparedQueryShapeColdCaches(b, db, tc.q)
			})
		}
	})
}

func BenchmarkQueryPreparedArrayOrder(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		fixed := []struct {
			name string
			q    *qx.QX
		}{
			{name: "TagsLEN_DESC_Limit128", q: qx.Query(qx.EQ("active", true)).SortBy(qx.LEN("tags"), qx.DESC).Limit(128)},
			{name: "CountryPOS_ASC_Limit128", q: qx.Query(qx.EQ("active", true)).SortBy(qx.POS("country", []string{"DE", "US", "JP", "NL"}), qx.ASC).Limit(128)},
			{name: "TagsPOS_ActiveFilter_Limit128", q: qx.Query(qx.EQ("active", true)).SortBy(qx.POS("tags", []string{"tag_mid", "go", "db"}), qx.ASC).Limit(128)},
		}
		for i := range fixed {
			tc := fixed[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkPreparedQueryShape(b, db, tc.q)
			})
		}
		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				tag := qexecBenchTag(sel)
				b.Run("TagsPOS_ASC_Limit128", func(b *testing.B) {
					q := qx.Query(
						qx.HASANY("tags", []string{tag, "go"}),
					).SortBy(qx.POS("tags", []string{tag, "go", "db"}), qx.ASC).Limit(128)
					benchmarkPreparedQueryShape(b, db, q)
				})
			})
		}
	})
}

func BenchmarkQueryWindowShapes(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		start, end := qexecBenchRange(scale.rows, sel)
		offset := (end - start) / 2
		if offset > 1_000_000 {
			offset = 1_000_000
		}
		if offset < 1 {
			offset = 1
		}
		tag := qexecBenchTag(sel)

		cases := []struct {
			name string
			q    *qx.QX
		}{
			{
				name: "OffsetHeavy_OrderScore_Limit128",
				q:    qx.Query(ageStart, ageEnd).Sort("score", qx.DESC).Offset(offset).Limit(128),
			},
			{
				name: "UnboundedRange_NoOrder",
				q:    qx.Query(ageStart, ageEnd),
			},
			{
				name: "ArrayOrderOffset_Limit128",
				q: qx.Query(
					qx.HASANY("tags", []string{tag, "missing_tag"}),
				).SortBy(qx.POS("tags", []string{tag, "go", "db"}), qx.ASC).Offset(offset).Limit(128),
			},
			{
				name: "ArrayOrderUnbounded",
				q: qx.Query(
					qx.HASANY("tags", []string{tag, "missing_tag"}),
				).SortBy(qx.POS("tags", []string{tag, "go", "db"}), qx.ASC),
			},
		}
		for i := range cases {
			tc := cases[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkPreparedQueryShape(b, db, tc.q)
			})
		}
	})
}

func BenchmarkQueryCacheModes(b *testing.B) {
	modes := []struct {
		name string
		opts testOptions
	}{
		{name: "CacheDisabled", opts: qexecBenchOptions()},
		{name: "CacheTiny", opts: qexecBenchOptions()},
	}
	modes[0].opts.MatPredCacheMaxEntries = -1
	modes[0].opts.MatPredCacheMaxCard = 1
	modes[1].opts.MatPredCacheMaxEntries = 1
	modes[1].opts.MatPredCacheMaxCard = 1024

	scales := qexecBenchScales()
	sels := qexecBenchSelectivities()
	for i := range modes {
		mode := modes[i]
		b.Run(mode.name, func(b *testing.B) {
			for j := range scales {
				scale := scales[j]
				b.Run(scale.name, func(b *testing.B) {
					db := newQexecBenchDBWithOptionsAndRows(b, mode.opts, scale.rows)
					for k := range sels {
						sel := sels[k]
						b.Run(sel.name, func(b *testing.B) {
							ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
							tag := qexecBenchTag(sel)
							prefix := qexecBenchPrefix(sel)
							b.Run("Range_Order_Limit128", func(b *testing.B) {
								benchmarkPreparedQueryShape(b, db, qx.Query(ageStart, ageEnd).Sort("score", qx.DESC).Limit(128))
							})
							b.Run("OR_Order_Limit128", func(b *testing.B) {
								benchmarkPreparedQueryShape(b, db, qx.Query(
									qx.OR(
										qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
										qx.AND(qx.EQ("country", "JP"), ageStart),
										qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
									),
								).Sort("score", qx.DESC).Limit(128))
							})
						})
					}
				})
			}
		})
	}
}

func qexecBenchTraceSink(TraceEvent) {
	qexecBenchTraceEvents++
}

func benchmarkPreparedQueryShapeTrace(b *testing.B, db *testDB, q *qx.QX) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	out, err := view.Query(&shape, true, false)
	if err != nil {
		b.Fatalf("Query warmup: %v", err)
	}
	qexecBenchIDs = out

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err = view.Query(&shape, true, false)
		if err != nil {
			b.Fatalf("Query: %v", err)
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkQueryPreparedShapeTrace(b *testing.B) {
	opts := qexecBenchOptions()
	opts.TraceSink = qexecBenchTraceSink
	opts.TraceSampleEvery = 1
	db := newQexecBenchDBWithOptionsAndRows(b, opts, qexecBenchRows)
	sel := qexecBenchAllSelectivities[1]
	ageStart, ageEnd := qexecBenchAgeRange(qexecBenchRows, sel)
	tag := qexecBenchTag(sel)
	prefix := qexecBenchPrefix(sel)

	cases := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "NoFilter_Limit100",
			q:    qx.Query().Limit(100),
		},
		{
			name: "ANDMixed_NoOrder",
			q: qx.Query(
				qx.EQ("country", "US"),
				qx.EQ("active", true),
				ageStart,
				ageEnd,
				qx.HASANY("tags", []string{tag, "missing_tag"}),
			),
		},
		{
			name: "OR_Order_Limit128",
			q: qx.Query(
				qx.OR(
					qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
					qx.AND(qx.EQ("country", "JP"), ageStart),
					qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
				),
			).Sort("score", qx.DESC).Limit(128),
		},
	}

	for i := range cases {
		tc := cases[i]
		b.Run("Rows100K/"+tc.name, func(b *testing.B) {
			benchmarkPreparedQueryShapeTrace(b, db, tc.q)
		})
	}
}

type qexecBenchRoute uint8

const (
	qexecBenchRouteNoFilterNoOrder qexecBenchRoute = iota
	qexecBenchRouteLimit
	qexecBenchRouteOrderBasicLimit
	qexecBenchRouteOrderPrefixLimit
	qexecBenchRouteRangeNoOrderLimit
	qexecBenchRoutePrefixNoOrderLimit
	qexecBenchRoutePlanCandidate
	qexecBenchRoutePlanORMerge
)

func runQexecBenchRoute(view *View, shape *qir.Shape, route qexecBenchRoute) ([]uint64, bool, error) {
	switch route {
	case qexecBenchRouteNoFilterNoOrder:
		return view.tryNoFilterNoOrderWithLimit(shape, nil)
	case qexecBenchRouteLimit:
		out, ok, _, err := view.tryLimitQuery(shape, nil)
		return out, ok, err
	case qexecBenchRouteOrderBasicLimit:
		return view.tryQueryOrderBasicWithLimit(shape, nil)
	case qexecBenchRouteOrderPrefixLimit:
		return view.tryQueryOrderPrefixWithLimit(shape, nil)
	case qexecBenchRouteRangeNoOrderLimit:
		return view.tryQueryRangeNoOrderWithLimit(shape, nil)
	case qexecBenchRoutePrefixNoOrderLimit:
		return view.tryQueryPrefixNoOrderWithLimit(shape, nil)
	case qexecBenchRoutePlanCandidate:
		return view.tryPlanCandidate(shape, nil)
	case qexecBenchRoutePlanORMerge:
		return view.tryPlanORMergeMode(shape, nil)
	default:
		return nil, false, nil
	}
}

func benchmarkQueryRoute(b *testing.B, db *testDB, q *qx.QX, route qexecBenchRoute) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	out, ok, err := runQexecBenchRoute(view, &shape, route)
	if err != nil {
		b.Fatalf("route warmup: %v", err)
	}
	if !ok {
		b.Fatalf("route warmup was not used")
	}
	qexecBenchIDs = out

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok, err = runQexecBenchRoute(view, &shape, route)
		if err != nil {
			b.Fatalf("route: %v", err)
		}
		if !ok {
			b.Fatalf("route was not used")
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkQueryRoutes(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		_, end := qexecBenchRange(scale.rows, qexecBenchAllSelectivities[2])
		scoreStart, _ := qexecBenchScoreRange(scale.rows, qexecBenchAllSelectivities[2])
		emailPrefix := qexecBenchHighCardEmailPrefix(qexecBenchAllSelectivities[2])
		fixed := []struct {
			name  string
			q     *qx.QX
			route qexecBenchRoute
		}{
			{name: "NoFilterNoOrderLimit", q: qx.Query().Limit(100), route: qexecBenchRouteNoFilterNoOrder},
			{name: "LimitPlanner_Filtered", q: qx.Query(qx.EQ("active", true), qx.EQ("country", "US")).Limit(128), route: qexecBenchRouteLimit},
			{name: "OrderBasicLimit_Filtered", q: qx.Query(qx.EQ("active", true)).Sort("score", qx.DESC).Limit(128), route: qexecBenchRouteOrderBasicLimit},
			{
				name: "OrderBasicLimit_OrderBoundMixedResiduals",
				q: qx.Query(
					qx.EQ("active", true),
					qx.GTE("age", end/2),
					scoreStart,
				).Sort("score", qx.DESC).Limit(50),
				route: qexecBenchRouteOrderBasicLimit,
			},
			{
				name: "OrderBasicLimit_OrderBoundNegativeOffset",
				q: qx.Query(
					qx.LT("age", end),
					qx.NOTIN("country", []string{"DE", "PL"}),
					qx.NOTIN("name", []string{"alice", "bob"}),
					qx.LT("score", float64(end)),
				).Sort("age", qx.ASC).Offset(2500).Limit(100),
				route: qexecBenchRouteOrderBasicLimit,
			},
			{
				name: "OrderPrefixLimit_EQResidual",
				q: qx.Query(
					qx.PREFIX("email", emailPrefix),
					qx.EQ("active", true),
				).Sort("email", qx.ASC).Limit(12),
				route: qexecBenchRouteOrderPrefixLimit,
			},
		}
		for i := range fixed {
			tc := fixed[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkQueryRoute(b, db, tc.q, tc.route)
			})
		}

		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				prefix := qexecBenchPrefix(sel)
				cases := []struct {
					name  string
					q     *qx.QX
					route qexecBenchRoute
				}{
					{name: "RangeNoOrderLimit", q: qx.Query(ageStart).Limit(128), route: qexecBenchRouteRangeNoOrderLimit},
					{name: "PrefixNoOrderLimit", q: qx.Query(qx.PREFIX("full_name", prefix)).Limit(128), route: qexecBenchRoutePrefixNoOrderLimit},
					{
						name: "PlanCandidate_NoOrder",
						q: qx.Query(
							qx.EQ("country", "US"),
							qx.EQ("active", true),
							ageStart,
							ageEnd,
							qx.HASANY("tags", []string{tag, "missing_tag"}),
						).Limit(256),
						route: qexecBenchRoutePlanCandidate,
					},
					{
						name: "PlanORMerge_NoOrder",
						q: qx.Query(
							qx.OR(
								qx.EQ("country", "US"),
								qx.AND(qx.EQ("country", "DE"), qx.HASANY("tags", []string{tag, "missing_tag"})),
								qx.AND(ageStart, ageEnd),
							),
						).Limit(256),
						route: qexecBenchRoutePlanORMerge,
					},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkQueryRoute(b, db, tc.q, tc.route)
					})
				}
			})
		}
	})
}

type qexecBenchORExecRoute uint8

const (
	qexecBenchORExecNoOrderAdaptive qexecBenchORExecRoute = iota
	qexecBenchORExecNoOrderBaseline
	qexecBenchORExecOrderKWay
	qexecBenchORExecOrderBasic
	qexecBenchORExecOrderFallback
)

func benchmarkORExecutor(b *testing.B, db *testDB, q *qx.QX, route qexecBenchORExecRoute) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()
	if shape.Expr.Op != qir.OpOR {
		b.Fatalf("expected OR query")
	}

	view := db.view()
	window, _ := orderWindow(&shape)
	orderField := ""
	if shape.HasOrder {
		orderField = db.exec.FieldNameByOrdinal(shape.Order.FieldOrdinal)
	}

	var branches plannerORBranches
	var alwaysFalse bool
	var ok bool
	switch route {
	case qexecBenchORExecOrderKWay, qexecBenchORExecOrderBasic, qexecBenchORExecOrderFallback:
		branches, alwaysFalse, ok = view.buildORBranchesOrdered(shape.Expr.Operands, orderField, window, shape.Offset)
	default:
		branches, alwaysFalse, ok = view.buildORBranches(shape.Expr.Operands)
	}
	if !ok {
		b.Fatalf("buildORBranches failed")
	}
	if alwaysFalse {
		b.Fatalf("unexpected always-false OR")
	}
	defer branches.Release()

	var analysis plannerOROrderAnalysis
	hasAnalysis := false
	if route == qexecBenchORExecOrderKWay {
		analysis, hasAnalysis = view.buildOROrderAnalysis(&shape, branches)
		if !hasAnalysis {
			b.Fatalf("buildOROrderAnalysis failed")
		}
		defer analysis.release()
	}

	var out []uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch route {
		case qexecBenchORExecNoOrderAdaptive:
			out, ok = view.execPlanORNoOrderAdaptive(&shape, branches, nil)
			err = nil
		case qexecBenchORExecNoOrderBaseline:
			out, ok = view.execPlanORNoOrderBaseline(&shape, branches, nil)
			err = nil
		case qexecBenchORExecOrderKWay:
			out, ok, err = view.execPlanOROrderKWay(&shape, branches, &analysis, nil)
		case qexecBenchORExecOrderBasic:
			out, ok = view.execPlanOROrderBasic(&shape, branches, nil, nil, nil)
			err = nil
		case qexecBenchORExecOrderFallback:
			out, ok, err = view.execPlanOROrderMergeFallback(&shape, branches, nil)
		}
		if err != nil {
			b.Fatalf("OR executor: %v", err)
		}
		if !ok {
			b.Fatalf("OR executor was not used")
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkORExecutors(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		prefix := qexecBenchPrefix(sel)
		noOrder := qx.Query(
			qx.OR(
				qx.EQ("country", "US"),
				qx.AND(qx.EQ("country", "DE"), qx.HASANY("tags", []string{tag, "missing_tag"})),
				qx.AND(ageStart, ageEnd),
			),
		).Limit(256)
		ordered := qx.Query(
			qx.OR(
				qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
				qx.AND(qx.EQ("country", "JP"), ageStart),
				qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
			),
		).Sort("score", qx.DESC).Limit(128)

		b.Run("NoOrder_Adaptive", func(b *testing.B) {
			benchmarkORExecutor(b, db, noOrder, qexecBenchORExecNoOrderAdaptive)
		})
		b.Run("NoOrder_Baseline", func(b *testing.B) {
			benchmarkORExecutor(b, db, noOrder, qexecBenchORExecNoOrderBaseline)
		})
		b.Run("Order_KWay", func(b *testing.B) {
			benchmarkORExecutor(b, db, ordered, qexecBenchORExecOrderKWay)
		})
		b.Run("Order_Basic", func(b *testing.B) {
			benchmarkORExecutor(b, db, ordered, qexecBenchORExecOrderBasic)
		})
		b.Run("Order_Fallback", func(b *testing.B) {
			benchmarkORExecutor(b, db, ordered, qexecBenchORExecOrderFallback)
		})
	})
}

func benchmarkPreparedFilter(b *testing.B, db *testDB, expr qx.Expr) {
	b.Helper()

	prepared, err := qir.PrepareCountExprsResolved(db.rt.IndexedByName, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprsResolved: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	ids, err := view.Filter(prepared)
	if err != nil {
		b.Fatalf("Filter warmup: %v", err)
	}
	qexecBenchCount = ids.Cardinality()
	ids.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, err = view.Filter(prepared)
		if err != nil {
			b.Fatalf("Filter: %v", err)
		}
		qexecBenchCount = ids.Cardinality()
		ids.Release()
	}
	b.ReportMetric(float64(qexecBenchCount), "rows/op")
}

func BenchmarkFilterPreparedExpr(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		b.Run("ScalarEQ", func(b *testing.B) {
			benchmarkPreparedFilter(b, db, qx.EQ("country", "US"))
		})
		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				prefix := qexecBenchPrefix(sel)
				cases := []struct {
					name string
					expr qx.Expr
				}{
					{
						name: "ANDMixed",
						expr: qx.AND(
							qx.EQ("active", true),
							ageStart,
							ageEnd,
							qx.HASANY("tags", []string{tag, "missing_tag"}),
						),
					},
					{
						name: "ORMixed",
						expr: qx.OR(
							qx.AND(qx.EQ("country", "US"), qx.HASANY("tags", []string{tag, "missing_tag"})),
							qx.AND(qx.EQ("country", "JP"), ageStart),
							qx.AND(qx.PREFIX("full_name", prefix), qx.EQ("active", true)),
						),
					},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkPreparedFilter(b, db, tc.expr)
					})
				}
			})
		}
	})
}

func BenchmarkNegativeNilComplementShapes(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, _ qexecBenchScale) {
		b.Run("Query_EQNil_NoOrder_Limit256", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(qx.EQ("opt", nil)).Limit(256))
		})
		b.Run("Cardinality_EQNil", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, qx.EQ("opt", nil))
		})
		b.Run("Cardinality_OptNotNilComplement", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, qx.NOT(qx.EQ("opt", nil)))
		})
		b.Run("Query_NullableOrderNilTail", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(qx.NOT(qx.EQ("active", false))).Sort("opt", qx.ASC).Offset(1).Limit(128))
		})
		b.Run("Query_NOTIN_NoRange_Limit256", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(qx.NOTIN("country", []string{"DE", "PL"})).Limit(256))
		})
	})

	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		tag := qexecBenchTag(sel)
		start, _ := qexecBenchRange(scale.rows, sel)

		b.Run("Query_NOT_EQ_Order_Limit128", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(
				qx.NOT(qx.EQ("active", false)),
				ageStart,
				ageEnd,
			).Sort("score", qx.DESC).Limit(128))
		})
		b.Run("Query_NOTIN_NoOrder_Limit256", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(
				qx.NOTIN("country", []string{"DE", "PL"}),
				ageStart,
				ageEnd,
			).Limit(256))
		})
		b.Run("Query_HASNONE_NoOrder_Limit256", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(
				qx.HASNONE("tags", []string{tag, "missing_tag"}),
				ageStart,
				ageEnd,
			).Limit(256))
		})
		b.Run("Cardinality_NOTIN", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, qx.AND(
				qx.NOTIN("country", []string{"DE", "PL"}),
				ageStart,
				ageEnd,
			))
		})
		b.Run("Cardinality_HASNONE", func(b *testing.B) {
			benchmarkPreparedCardinality(b, db, qx.AND(
				qx.HASNONE("tags", []string{tag, "missing_tag"}),
				ageStart,
				ageEnd,
			))
		})
		b.Run("Query_PositiveRangeComplementCandidate", func(b *testing.B) {
			benchmarkPreparedQueryShape(b, db, qx.Query(qx.GTE("age", start)).Sort("score", qx.DESC).Limit(128))
		})
	})

	b.Run("Rows1M/Cardinality_NOTIN_RangeResidual_NonTrigger", func(b *testing.B) {
		scale := qexecBenchScale{name: "Rows1M", rows: 1_000_000}
		db := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), scale.rows)
		sel := qexecBenchAllSelectivities[2]
		ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
		scoreStart, scoreEnd := qexecBenchScoreRange(scale.rows, sel)
		benchmarkPreparedCardinality(b, db, qx.AND(
			qx.NOTIN("country", []string{"DE", "PL"}),
			ageStart,
			ageEnd,
			scoreStart,
			scoreEnd,
		))
	})
}

func benchmarkPreparedQueryMethod(b *testing.B, db *testDB, q *qx.QX) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	out, err := view.PreparedQuery(&shape)
	if err != nil {
		b.Fatalf("PreparedQuery warmup: %v", err)
	}
	qexecBenchIDs = out

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err = view.PreparedQuery(&shape)
		if err != nil {
			b.Fatalf("PreparedQuery: %v", err)
		}
		qexecBenchIDs = out
	}
	b.ReportMetric(float64(len(qexecBenchIDs)), "rows/op")
}

func BenchmarkPreparedQueryMethod(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		b.Run("NoFilter_Limit100", func(b *testing.B) {
			benchmarkPreparedQueryMethod(b, db, qx.Query().Limit(100))
		})
		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				ageStart, ageEnd := qexecBenchAgeRange(scale.rows, sel)
				tag := qexecBenchTag(sel)
				cases := []struct {
					name string
					q    *qx.QX
				}{
					{
						name: "ANDMixed_NoOrder",
						q: qx.Query(
							qx.EQ("country", "US"),
							qx.EQ("active", true),
							ageStart,
							ageEnd,
							qx.HASANY("tags", []string{tag, "missing_tag"}),
						),
					},
					{name: "Range_Order_Limit128", q: qx.Query(ageStart, ageEnd).Sort("score", qx.DESC).Limit(128)},
				}
				for j := range cases {
					tc := cases[j]
					b.Run(tc.name, func(b *testing.B) {
						benchmarkPreparedQueryMethod(b, db, tc.q)
					})
				}
			})
		}
	})
}

func BenchmarkEndToEndWrappers(b *testing.B) {
	opts := qexecBenchPublicOptions(qexecBenchOptions())
	db := newQexecBenchPublicDB(b, qexecBenchRows, opts)
	sel := qexecBenchAllSelectivities[1]
	ageStart, ageEnd := qexecBenchAgeRange(qexecBenchRows, sel)
	tag := qexecBenchTag(sel)
	q := qx.Query(
		qx.EQ("country", "US"),
		qx.EQ("active", true),
		ageStart,
		ageEnd,
		qx.HASANY("tags", []string{tag, "missing_tag"}),
	).Limit(128)

	b.Run("Rows100K/DB.QueryKeys", func(b *testing.B) {
		ids, err := db.QueryKeys(q)
		if err != nil {
			b.Fatalf("QueryKeys warmup: %v", err)
		}
		qexecBenchCount = uint64(len(ids))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ids, err = db.QueryKeys(q)
			if err != nil {
				b.Fatalf("QueryKeys: %v", err)
			}
			qexecBenchCount = uint64(len(ids))
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	b.Run("Rows100K/DB.Query", func(b *testing.B) {
		out, err := db.Query(q)
		if err != nil {
			b.Fatalf("Query warmup: %v", err)
		}
		qexecBenchCount = uint64(len(out))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out, err = db.Query(q)
			if err != nil {
				b.Fatalf("Query: %v", err)
			}
			qexecBenchCount = uint64(len(out))
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	b.Run("Rows100K/DB.Count", func(b *testing.B) {
		cnt, err := db.Count(
			qx.EQ("country", "US"),
			qx.EQ("active", true),
			ageStart,
			ageEnd,
			qx.HASANY("tags", []string{tag, "missing_tag"}),
		)
		if err != nil {
			b.Fatalf("Count warmup: %v", err)
		}
		qexecBenchCount = cnt

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cnt, err = db.Count(
				qx.EQ("country", "US"),
				qx.EQ("active", true),
				ageStart,
				ageEnd,
				qx.HASANY("tags", []string{tag, "missing_tag"}),
			)
			if err != nil {
				b.Fatalf("Count: %v", err)
			}
			qexecBenchCount = cnt
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	internalDB := newQexecBenchDBWithOptionsAndRows(b, qexecBenchOptions(), qexecBenchRows)
	b.Run("Rows100K/PrepareExecuteCombined", func(b *testing.B) {
		view := internalDB.view()
		prepared, shape, err := internalDB.prepareQuery(q)
		if err != nil {
			b.Fatalf("prepareQuery warmup: %v", err)
		}
		out, err := view.Query(&shape, false, false)
		prepared.Release()
		if err != nil {
			b.Fatalf("Query warmup: %v", err)
		}
		qexecBenchCount = uint64(len(out))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			prepared, shape, err = internalDB.prepareQuery(q)
			if err != nil {
				b.Fatalf("prepareQuery: %v", err)
			}
			out, err = view.Query(&shape, false, false)
			prepared.Release()
			if err != nil {
				b.Fatalf("Query: %v", err)
			}
			qexecBenchCount = uint64(len(out))
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})
}

func benchmarkTryQueryEmpty(b *testing.B, db *testDB, q *qx.QX) {
	b.Helper()

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		b.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	empty, err := view.TryQueryEmptyOnSnapshot(&shape)
	if err != nil {
		b.Fatalf("TryQueryEmptyOnSnapshot warmup: %v", err)
	}
	if !empty {
		b.Fatalf("query was not empty")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		empty, err = view.TryQueryEmptyOnSnapshot(&shape)
		if err != nil {
			b.Fatalf("TryQueryEmptyOnSnapshot: %v", err)
		}
		if !empty {
			b.Fatalf("query was not empty")
		}
	}
}

func BenchmarkTryQueryEmptyOnSnapshot(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		cases := []struct {
			name string
			q    *qx.QX
		}{
			{name: "ScalarEQMissing", q: qx.Query(qx.EQ("email", "missing@example.test"))},
			{name: "RangePastEnd", q: qx.Query(qx.GT("age", scale.rows+1))},
			{name: "PrefixMissing", q: qx.Query(qx.PREFIX("full_name", "missing"))},
		}
		for i := range cases {
			tc := cases[i]
			b.Run(tc.name, func(b *testing.B) {
				benchmarkTryQueryEmpty(b, db, tc.q)
			})
		}
	})
}

func BenchmarkNumericRangeBuckets(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		view := db.view()
		fm := db.rt.Fields["age"]
		if fm == nil {
			b.Fatal("missing age field metadata")
		}
		ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
		if !ov.HasData() {
			b.Fatal("missing age index")
		}
		start, end := qexecBenchRange(scale.rows, sel)
		br := ov.RangeByRanks(start, end)
		probe, ok := view.tryEvalNumericRangeBuckets("age", fm, ov, br)
		if !ok {
			b.Skip("numeric range bucket path is not used for this selectivity")
		}
		probe.ids.Release()
		db.clearCurrentSnapshotCaches()

		b.Run("EvalColdBuild", func(b *testing.B) {
			var res postingResult
			var ok bool
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db.clearCurrentSnapshotCaches()
				b.StartTimer()
				res, ok = view.tryEvalNumericRangeBuckets("age", fm, ov, br)
				if !ok {
					b.Fatal("expected numeric range bucket eval path")
				}
				qexecBenchCount = res.ids.Cardinality()
				res.ids.Release()
			}
			b.ReportMetric(float64(qexecBenchCount), "rows/op")
		})

		warm, ok := view.tryEvalNumericRangeBuckets("age", fm, ov, br)
		if !ok {
			b.Fatal("expected numeric range bucket warmup path")
		}
		warm.ids.Release()

		loadable := false
		loaded, ok := view.tryLoadNumericRangeBuckets("age", fm, ov, br)
		if ok {
			loaded.ids.Release()
			loadable = true
		}

		b.Run("EvalWarm", func(b *testing.B) {
			var res postingResult
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, ok = view.tryEvalNumericRangeBuckets("age", fm, ov, br)
				if !ok {
					b.Fatal("expected numeric range bucket eval path")
				}
				qexecBenchCount = res.ids.Cardinality()
				res.ids.Release()
			}
			b.ReportMetric(float64(qexecBenchCount), "rows/op")
		})

		b.Run("LoadWarm", func(b *testing.B) {
			if !loadable {
				b.Skip("numeric range bucket full-span cache guard rejected this span")
			}
			var res postingResult
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, ok = view.tryLoadNumericRangeBuckets("age", fm, ov, br)
				if !ok {
					b.Fatal("expected numeric range bucket load path")
				}
				qexecBenchCount = res.ids.Cardinality()
				res.ids.Release()
			}
			b.ReportMetric(float64(qexecBenchCount), "rows/op")
		})

		reuseEnd := end + qexecBenchOptions().NumericRangeBucketSize
		if reuseEnd > scale.rows+1 {
			reuseEnd = scale.rows + 1
		}
		b.Run("EvalExtendedReuse", func(b *testing.B) {
			if reuseEnd <= end {
				b.Skip("range cannot be extended")
			}
			reuseBR := ov.RangeByRanks(start, reuseEnd)
			res, ok := view.tryEvalNumericRangeBuckets("age", fm, ov, reuseBR)
			if !ok {
				b.Skip("numeric range bucket reuse path is not used for this span")
			}
			qexecBenchCount = res.ids.Cardinality()
			res.ids.Release()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, ok = view.tryEvalNumericRangeBuckets("age", fm, ov, reuseBR)
				if !ok {
					b.Fatal("expected numeric range bucket extended eval path")
				}
				qexecBenchCount = res.ids.Cardinality()
				res.ids.Release()
			}
			b.ReportMetric(float64(qexecBenchCount), "rows/op")
		})

		b.Run("SnapshotCardinality", func(b *testing.B) {
			var ok bool
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				qexecBenchCount, ok = view.trySnapshotNumericRangeCardinality("age", fm, ov, start, end)
				if !ok {
					b.Fatal("expected numeric range cardinality path")
				}
			}
			b.ReportMetric(float64(qexecBenchCount), "rows/op")
		})
	})
}

func BenchmarkBroadComplementMaterialization(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		view := db.view()
		start, _ := qexecBenchRange(scale.rows, qexecBenchAllSelectivities[2])
		resolved, err := qir.PrepareCountExprResolved(db.rt.IndexedByName, qx.GTE("age", start))
		if err != nil {
			b.Fatalf("PrepareCountExprResolved: %v", err)
		}
		defer resolved.Release()

		b.Run("ColdMaterialize", func(b *testing.B) {
			universe := view.snap.UniverseCardinality()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db.clearCurrentSnapshotCaches()
				b.StartTimer()
				p, ok := view.buildPredicateWithMode(resolved.Expr, false)
				if !ok {
					b.Fatal("buildPredicateWithMode failed")
				}
				route := view.orderedPredicateScalarRangeRouting(p, 128, 0, universe)
				if !route.useComplement && !route.broadComplement {
					releasePredicateOwnedState(&p)
					b.Skip("ordered complement route is not used")
				}
				ok = view.tryMaterializeBroadRangeComplementPredicateForOrdered(&p, route.broadComplement, universe, 128, false)
				if !ok {
					releasePredicateOwnedState(&p)
					b.Skip("broad complement materialization is not used")
				}
				qexecBenchCount = p.estCard
				releasePredicateOwnedState(&p)
			}
		})

		p, ok := view.buildPredicateWithMode(resolved.Expr, false)
		if !ok {
			b.Fatal("buildPredicateWithMode warmup failed")
		}
		route := view.orderedPredicateScalarRangeRouting(p, 128, 0, view.snap.UniverseCardinality())
		ok = view.tryMaterializeBroadRangeComplementPredicateForOrdered(&p, route.broadComplement, view.snap.UniverseCardinality(), 128, true)
		releasePredicateOwnedState(&p)
		if !ok {
			b.Skip("broad complement warm materialization is not used")
		}

		b.Run("WarmLoad", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p, ok := view.buildPredicateWithMode(resolved.Expr, false)
				if !ok {
					b.Fatal("buildPredicateWithMode failed")
				}
				if !view.loadWarmComplementPredicateForOrdered(&p, route.useComplement) {
					releasePredicateOwnedState(&p)
					b.Fatal("expected warm complement load path")
				}
				qexecBenchCount = p.estCard
				releasePredicateOwnedState(&p)
			}
		})
	})
}

func benchmarkRangePredicateState(b *testing.B, db *testDB, rows int, expr qx.Expr, mode string) {
	b.Helper()

	resolved, err := qir.PrepareCountExprResolved(db.rt.IndexedByName, expr)
	if err != nil {
		b.Fatalf("PrepareCountExprResolved: %v", err)
	}
	defer resolved.Release()

	db.clearCurrentSnapshotCaches()
	view := db.view()
	p, ok := view.buildPredicateWithMode(resolved.Expr, false)
	if !ok {
		b.Fatal("buildPredicateWithMode failed")
	}
	defer releasePredicateOwnedState(&p)
	if p.fieldIndexRangeState == nil {
		b.Fatal("expected field index range state")
	}

	state := p.fieldIndexRangeState
	switch mode {
	case "materialized_matches", "materialized_count", "materialized_apply":
		ids := state.materializeRange()
		if ids.IsEmpty() {
			b.Fatal("expected materialized range")
		}
		if mode == "materialized_apply" {
			state.probePostingFilter = false
			state.rangeMaterializeAt = 1
		}
	default:
		state.materializeAfter = 0
	}

	universe := view.snap.Universe.Borrow()
	defer universe.Release()

	b.ReportAllocs()
	b.ResetTimer()
	switch mode {
	case "matches":
		for i := 0; i < b.N; i++ {
			if state.matches(uint64((i % rows) + 1)) {
				qexecBenchCount++
			}
		}
	case "materialized_matches":
		for i := 0; i < b.N; i++ {
			if state.matches(uint64((i % rows) + 1)) {
				qexecBenchCount++
			}
		}
	case "count":
		for i := 0; i < b.N; i++ {
			cnt, ok := state.countBucket(universe)
			if !ok {
				b.Fatal("countBucket failed")
			}
			qexecBenchCount = cnt
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	case "materialized_count":
		for i := 0; i < b.N; i++ {
			cnt, ok := state.countBucket(universe)
			if !ok {
				b.Fatal("countBucket failed")
			}
			qexecBenchCount = cnt
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	case "materialized_apply":
		for i := 0; i < b.N; i++ {
			dst := universe.Borrow()
			out, ok := state.applyToPosting(dst)
			if !ok {
				dst.Release()
				b.Fatal("applyToPosting failed")
			}
			qexecBenchCount = out.Cardinality()
			out.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	default:
		b.Fatalf("unknown mode %q", mode)
	}
}

func BenchmarkFieldIndexRangePredicateState(b *testing.B) {
	qexecBenchRunScales(b, func(b *testing.B, db *testDB, scale qexecBenchScale) {
		b.Run("SmallLinearMatches", func(b *testing.B) {
			benchmarkRangePredicateState(b, db, scale.rows, qx.GTE("age", scale.rows-16), "matches")
		})
		sels := qexecBenchSelectivities()
		for i := range sels {
			sel := sels[i]
			b.Run(sel.name, func(b *testing.B) {
				start, end := qexecBenchRange(scale.rows, sel)
				expr := qx.LTE("age", end-start)
				b.Run("ProbeCountBucket", func(b *testing.B) {
					benchmarkRangePredicateState(b, db, scale.rows, expr, "count")
				})
				b.Run("MaterializedMatches", func(b *testing.B) {
					benchmarkRangePredicateState(b, db, scale.rows, expr, "materialized_matches")
				})
				b.Run("MaterializedCountBucket", func(b *testing.B) {
					benchmarkRangePredicateState(b, db, scale.rows, expr, "materialized_count")
				})
				b.Run("MaterializedApply", func(b *testing.B) {
					benchmarkRangePredicateState(b, db, scale.rows, expr, "materialized_apply")
				})
			})
		}
	})
}

func benchmarkPostsAnyState(b *testing.B, db *testDB, rows int, tag string, mode string) {
	b.Helper()

	view := db.view()
	acc, ok := db.rt.IndexedByName["tags"]
	if !ok {
		b.Fatal("missing tags index")
	}
	keys := []string{tag, "go"}
	if mode == "count_direct" {
		keys = []string{tag}
	}
	posts, _ := view.scalarLookupPostings("tags", acc.Ordinal, keys, false)
	defer posting.ReleaseSlice(posts)
	if len(posts) == 0 {
		b.Fatal("expected tag postings")
	}

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = posts
	state.containsMaterializeAt = 0
	defer postsAnyFilterStatePool.Put(state)

	if mode == "materialized_matches" || mode == "count_materialized" || mode == "apply_materialized" {
		ids := state.materialize()
		if ids.IsEmpty() {
			b.Fatal("expected materialized postsAny state")
		}
	}

	universe := view.snap.Universe.Borrow()
	defer universe.Release()

	b.ReportAllocs()
	b.ResetTimer()
	switch mode {
	case "matches":
		for i := 0; i < b.N; i++ {
			if state.matches(uint64((i % rows) + 1)) {
				qexecBenchCount++
			}
		}
	case "materialized_matches":
		for i := 0; i < b.N; i++ {
			if state.matches(uint64((i % rows) + 1)) {
				qexecBenchCount++
			}
		}
	case "count_direct":
		for i := 0; i < b.N; i++ {
			cnt, ok := state.countBucket(universe)
			if !ok {
				b.Fatal("countBucket failed")
			}
			qexecBenchCount = cnt
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	case "count_materialized":
		for i := 0; i < b.N; i++ {
			cnt, ok := state.countBucket(universe)
			if !ok {
				b.Fatal("countBucket failed")
			}
			qexecBenchCount = cnt
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	case "apply_adaptive":
		for i := 0; i < b.N; i++ {
			dst := universe.Borrow()
			out, ok := state.apply(dst)
			if !ok {
				dst.Release()
				b.Fatal("apply failed")
			}
			qexecBenchCount = out.Cardinality()
			out.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	case "apply_materialized":
		for i := 0; i < b.N; i++ {
			dst := universe.Borrow()
			out, ok := state.apply(dst)
			if !ok {
				dst.Release()
				b.Fatal("apply failed")
			}
			qexecBenchCount = out.Cardinality()
			out.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	default:
		b.Fatalf("unknown mode %q", mode)
	}
}

func BenchmarkPostsAnyFilterState(b *testing.B) {
	qexecBenchRunScaleSelectivities(b, func(b *testing.B, db *testDB, scale qexecBenchScale, sel qexecBenchSelectivity) {
		tag := qexecBenchTag(sel)
		b.Run("MatchesDirect", func(b *testing.B) {
			benchmarkPostsAnyState(b, db, scale.rows, tag, "matches")
		})
		b.Run("MatchesMaterialized", func(b *testing.B) {
			benchmarkPostsAnyState(b, db, scale.rows, tag, "materialized_matches")
		})
		b.Run("CountDirect", func(b *testing.B) {
			benchmarkPostsAnyState(b, db, scale.rows, tag, "count_direct")
		})
		b.Run("CountMaterialized", func(b *testing.B) {
			benchmarkPostsAnyState(b, db, scale.rows, tag, "count_materialized")
		})
		b.Run("ApplyAdaptive", func(b *testing.B) {
			benchmarkPostsAnyState(b, db, scale.rows, tag, "apply_adaptive")
		})
		b.Run("ApplyMaterialized", func(b *testing.B) {
			benchmarkPostsAnyState(b, db, scale.rows, tag, "apply_materialized")
		})
	})
}

func BenchmarkPostingBuilders(b *testing.B) {
	b.Run("UnionSingles", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := newPostingUnionBuilder(true)
			for j := 0; j < 4096; j++ {
				builder.addSingle(uint64(j + 1))
			}
			ids := builder.finish(true)
			qexecBenchCount = ids.Cardinality()
			ids.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	b.Run("SetDedupSingles", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := newPostingSetBuilder(4096, true)
			for j := 0; j < 8192; j++ {
				builder.addChecked(uint64((j & 4095) + 1))
			}
			ids := builder.finish(true)
			qexecBenchCount = ids.Cardinality()
			ids.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	b.Run("LazySetDedupSingles", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := newPostingLazySetBuilder(4096)
			for j := 0; j < 8192; j++ {
				builder.addChecked(uint64((j & 4095) + 1))
			}
			ids := builder.finish(true)
			qexecBenchCount = ids.Cardinality()
			ids.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	largeVals := make([]uint64, 65536)
	subsetVals := make([]uint64, 0, 4096)
	controlVals := make([]uint64, 0, 4096)
	for i := range largeVals {
		id := uint64(i*2 + 2)
		largeVals[i] = id
		if i&15 == 0 {
			subsetVals = append(subsetVals, id)
			controlVals = append(controlVals, id-1)
		}
	}
	large := posting.BuildFromSorted(largeVals)
	subset := posting.BuildFromSorted(subsetVals)
	control := posting.BuildFromSorted(controlVals)
	defer large.Release()
	defer subset.Release()
	defer control.Release()

	b.Run("UnionPairSubset", func(b *testing.B) {
		posts := [...]posting.List{subset, large}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ids := materializePostingUnionBufOwned(posts[:])
			qexecBenchCount = ids.Cardinality()
			ids.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})

	b.Run("UnionPairOverlapControl", func(b *testing.B) {
		posts := [...]posting.List{control, large}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ids := materializePostingUnionBufOwned(posts[:])
			qexecBenchCount = ids.Cardinality()
			ids.Release()
		}
		b.ReportMetric(float64(qexecBenchCount), "rows/op")
	})
}

func qexecBenchPostingLists() [4]posting.List {
	var out [4]posting.List
	for i := 0; i < len(out); i++ {
		vals := make([]uint64, 1024)
		base := uint64(i*512 + 1)
		for j := range vals {
			vals[j] = base + uint64(j*4)
		}
		out[i] = posting.BuildFromSorted(vals)
	}
	return out
}

func BenchmarkPostingIters(b *testing.B) {
	lists := qexecBenchPostingLists()
	for i := range lists {
		defer lists[i].Release()
	}
	posts := lists[:]

	b.Run("Concat", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := newPostingConcatBufIter(posts)
			var n uint64
			for it.HasNext() {
				n += it.Next() & 1
			}
			it.Release()
			qexecBenchCount = n
		}
	})

	b.Run("Union", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := newPostingUnionIter(posts)
			var n uint64
			for it.HasNext() {
				n += it.Next() & 1
			}
			it.Release()
			qexecBenchCount = n
		}
	})
}
