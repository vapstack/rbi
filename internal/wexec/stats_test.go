package wexec

import "testing"

func TestStatsTrackBatchSizeDistribution(t *testing.T) {
	stats := statsCounters{Enabled: true}
	stats.recordExecuted(1)
	stats.recordExecuted(4)
	stats.recordExecuted(8)
	stats.recordExecuted(9)

	st := stats.snapshot(0, 16, 64, 0, 0, false, false)
	if st.BatchSize1 != 1 || st.BatchSize2To4 != 1 || st.BatchSize5To8 != 1 || st.BatchSize9Plus != 1 {
		t.Fatalf("batch size buckets = 1:%d 2..4:%d 5..8:%d 9+:%d", st.BatchSize1, st.BatchSize2To4, st.BatchSize5To8, st.BatchSize9Plus)
	}
	if st.ExecutedBatches != 4 || st.MultiRequestBatches != 3 || st.MultiRequestOps != 21 || st.MaxBatchSeen != 9 {
		t.Fatalf("stats snapshot = %+v", st)
	}
}
