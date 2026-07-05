package wexec

import "sync/atomic"

type ExecutorStats struct {
	CallbackOps    uint64
	UniqueRejected uint64
	TxOpErrors     uint64
	CallbackErrors uint64
}

type executorStatsCounters struct {
	Enabled bool

	CallbackOps    atomic.Uint64
	UniqueRejected atomic.Uint64
	TxOpErrors     atomic.Uint64
	CallbackErrors atomic.Uint64
}
