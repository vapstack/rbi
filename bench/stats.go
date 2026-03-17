package main

import "github.com/vapstack/rbi"

type benchmarkDiagnostics struct {
	Index       rbi.IndexStats[uint64] `json:"index"`
	Snapshot    rbi.SnapshotStats      `json:"snapshot"`
	Planner     rbi.PlannerStats       `json:"planner"`
	Calibration rbi.CalibrationStats   `json:"calibration"`
	AutoBatch   rbi.AutoBatchStats     `json:"auto_batch"`
}

func collectBenchDiagnostics(db *rbi.DB[uint64, UserBench]) benchmarkDiagnostics {
	return benchmarkDiagnostics{
		Index:       db.IndexStats(),
		Snapshot:    db.SnapshotStats(),
		Planner:     db.PlannerStats(),
		Calibration: db.CalibrationStats(),
		AutoBatch:   db.AutoBatchStats(),
	}
}
