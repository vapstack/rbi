package rebuild

import (
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

type DecodeFunc func([]byte) (unsafe.Pointer, error)

type ReleaseFunc func(unsafe.Pointer)

type State struct {
	Index             []indexdata.FieldStorage
	NilIndex          []indexdata.FieldStorage
	LenIndex          []indexdata.FieldStorage
	LenZeroComplement []bool
	Measure           []indexdata.MeasureStorage
	Universe          posting.List
	LenLoaded         bool
}

type Config struct {
	Bolt              *bbolt.DB
	Bucket            []byte
	Schema            *schema.Schema
	Current           *snapshot.View
	StrKey            bool
	StrMap            *strmap.Mapper
	SkipFields        map[string]struct{}
	SkipMeasureFields map[string]struct{}
	Decode            DecodeFunc
	Release           ReleaseFunc
}

type Result struct {
	Storage   snapshot.Storage
	Publish   bool
	Stats     bool
	BuildTime time.Duration
	BuildRPS  int
	LenLoaded bool
}

func Build(cfg Config, state State) (Result, error) {
	active := make([]buildField, 0, len(cfg.Schema.Indexed))
	for _, acc := range cfg.Schema.Indexed {
		if _, skip := cfg.SkipFields[acc.Name]; skip {
			continue
		}
		active = append(active, buildField{
			acc:          acc,
			write:        acc.WriteBuild,
			writeChecked: acc.WriteBuildChecked,
			slice:        acc.Field.Slice,
			numeric:      acc.Field.KeyKind == schema.FieldWriteKeysOrderedU64,
		})
	}
	activeMeasures := make([]schema.MeasureFieldAccessor, 0, len(cfg.Schema.Measures))
	for _, acc := range cfg.Schema.Measures {
		if _, skip := cfg.SkipMeasureFields[acc.Name]; skip {
			continue
		}
		activeMeasures = append(activeMeasures, acc)
	}

	if len(active) == 0 && len(activeMeasures) == 0 {
		return buildNoActive(cfg, state)
	}

	start := time.Now()
	build, err := scan(cfg, active, activeMeasures)
	if err != nil {
		return Result{}, err
	}

	result := materialize(cfg, state, active, activeMeasures, build, start)
	build.ok = true
	cleanupMemory(true)
	return result, nil
}
