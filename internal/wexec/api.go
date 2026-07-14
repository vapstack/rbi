package wexec

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

type Executor struct {
	stats executorStatsCounters

	dataBucket   []byte
	strmapBucket []byte

	bucketFillPercent float64

	strKey      bool
	indexed     bool
	ops         *RecordOps
	schema      *schema.Schema
	unique      UniqueContext
	snapshotOps SnapshotOps
}

type Config struct {
	StatsEnabled bool

	DataBucket   []byte
	StrMapBucket []byte
	StrKey       bool

	BucketFillPercent float64

	Indexed     bool
	Ops         *RecordOps
	Schema      *schema.Schema
	Unique      UniqueContext
	SnapshotOps SnapshotOps
}

func NewExecutor(cfg Config) *Executor {
	ex := &Executor{
		stats: executorStatsCounters{Enabled: cfg.StatsEnabled},

		dataBucket:        cfg.DataBucket,
		bucketFillPercent: cfg.BucketFillPercent,
		strKey:            cfg.StrKey,
		strmapBucket:      cfg.StrMapBucket,
		indexed:           cfg.Indexed,
		ops:               cfg.Ops,
		schema:            cfg.Schema,
		unique:            cfg.Unique,
		snapshotOps:       cfg.SnapshotOps,
	}
	return ex
}

func (b *Executor) ExecutorStats() ExecutorStats {
	if !b.stats.Enabled {
		return ExecutorStats{}
	}
	return ExecutorStats{
		CallbackOps:    b.stats.CallbackOps.Load(),
		UniqueRejected: b.stats.UniqueRejected.Load(),
		TxOpErrors:     b.stats.TxOpErrors.Load(),
		CallbackErrors: b.stats.CallbackErrors.Load(),
	}
}

type (
	OnChangeHook func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error
)

type RecordOps struct {
	Encode        func(unsafe.Pointer, []byte) ([]byte, error)
	Decode        func([]byte) (unsafe.Pointer, error)
	Acquire       func() unsafe.Pointer
	CloneInto     func(unsafe.Pointer, unsafe.Pointer) error
	Release       func(unsafe.Pointer)
	ValidateIndex func(unsafe.Pointer) error
}

type SnapshotOps struct {
	Current     func() *snapshot.View
	Schema      *schema.Schema
	CacheConfig snapshot.CacheConfig
	PatchFields map[string]*schema.Field
	StrKeyIndex bool
}
