package qexec

import "testing"

func TestP2Quantile_WarmupKeepsExactPercentileAtFiveSamples(t *testing.T) {
	q := newP2Quantile(0.95)
	for _, v := range []uint64{1, 2, 3, 4, 100} {
		q.observe(v)
	}
	if got := q.value(); got != 100 {
		t.Fatalf("unexpected p95 during warmup: got=%d want=100", got)
	}
}

func TestPlannerStatsCollector_PeriodicBudgetAdvancesCursor(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 3_000)

	fieldCount := len(db.engine.schema.Indexed)
	if fieldCount < 2 {
		t.Skip("not enough indexed fields for cursor progression test")
	}

	db.engine.exec.Analyzer.Lock()
	db.engine.exec.Analyzer.Cursor = 0
	db.engine.exec.Analyzer.SoftBudget = 1 // 1ns, effectively one heavy field per cycle
	db.engine.exec.Analyzer.Unlock()

	prevVersion := uint64(0)
	prevCursor := 0

	cycles := fieldCount + 3
	for i := 0; i < cycles; i++ {
		db.engine.exec.Analyzer.Lock()
		db.engine.exec.RefreshPlannerStatsOnSnapshot(db.engine.snapshot.Current(), db.engine.exec.Analyzer.SoftBudget, true)
		curCursor := db.engine.exec.Analyzer.Cursor
		db.engine.exec.Analyzer.Unlock()

		wantCursor := (prevCursor + 1) % fieldCount
		if curCursor != wantCursor {
			t.Fatalf("cursor mismatch at cycle %d: got=%d want=%d", i, curCursor, wantCursor)
		}
		prevCursor = curCursor

		s := db.engine.exec.Stats.Load()
		if s == nil || s.Version <= prevVersion {
			t.Fatalf("version did not advance at cycle %d: got=%v prev=%d", i, s, prevVersion)
		}
		prevVersion = s.Version
	}
}
