package rebuild

import (
	"fmt"
	"runtime"
	"time"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

func buildKeyOnly(cfg Config, state State) (Result, error) {
	start := time.Now()
	builder := indexdata.SortedUniqueStringFieldStorageBuilder{}
	builder.Init(0)
	buildUniverse := state.Universe.IsEmpty()
	var scannedUniverse posting.List
	var keyIndex indexdata.FieldStorage
	maxStringIdx := uint64(0)
	scanned := uint64(0)

	err := cfg.Bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(cfg.DataBucket)
		if b == nil {
			keyIndex = builder.Finish()
			return nil
		}
		stringMap := tx.Bucket(cfg.StrMapBucket)
		if stringMap == nil {
			return fmt.Errorf("string storage format: missing string map bucket %q", cfg.StrMapBucket)
		}

		nextGCAt := uint64(indexBuildGCStride)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(v) < 8 {
				return fmt.Errorf(
					"invalid string value scan_pos=%d id=%q idx=%d value_len=%d value_prefix_hex=%s: string storage format: value shorter than %d bytes",
					scanned+1,
					string(k),
					uint64(0),
					len(v),
					formatDiagnosticBytesPrefix(v, 32),
					8,
				)
			}
			idx := keycodec.U64FromBytes(v[:8])
			if idx == 0 {
				return fmt.Errorf(
					"invalid string value scan_pos=%d id=%q idx=%d value_len=%d value_prefix_hex=%s: string storage format: zero string id",
					scanned+1,
					string(k),
					idx,
					len(v),
					formatDiagnosticBytesPrefix(v, 32),
				)
			}
			// bbolt data keys are capped at 32 KiB, below FieldStringRefMax.
			builder.AppendBytes(k, idx)
			if buildUniverse {
				scannedUniverse = scannedUniverse.BuildAdded(idx)
			}
			if idx > maxStringIdx {
				maxStringIdx = idx
			}
			scanned++

			if scanned >= nextGCAt {
				runtime.GC()
				nextGCAt += indexBuildGCStride
			}
		}
		if stringMap.Sequence() < maxStringIdx {
			return fmt.Errorf("string storage format: string map sequence %d lower than max live idx %d", stringMap.Sequence(), maxStringIdx)
		}
		keyIndex = builder.Finish()
		return nil
	})
	if err != nil {
		builder.Release()
		scannedUniverse.Release()
		return Result{}, fmt.Errorf("scan error: db=%q bucket=%q: %w", cfg.Bolt.Path(), string(cfg.DataBucket), err)
	}

	slotCount := len(cfg.Schema.Indexed)
	nextUniverse := state.Universe
	if buildUniverse {
		nextUniverse = scannedUniverse
		scannedUniverse = posting.List{}
	}
	var nextLenIndex []indexdata.FieldStorage
	var nextLenZeroComplement []bool
	if state.LenLoaded {
		nextLenIndex = indexdata.CloneFieldStorageSlots(state.LenIndex, slotCount)
		nextLenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
		clear(nextLenZeroComplement)
		copy(nextLenZeroComplement, state.LenZeroComplement[:min(slotCount, len(state.LenZeroComplement))])
	} else {
		nextLenIndex, nextLenZeroComplement = buildLenIndexStorage(cfg.Schema, state.Index, nextUniverse)
	}

	buildTime := time.Since(start)
	scannedUniverse.Release()
	recordCount := uint64(scanned)
	return Result{
		Storage: snapshot.Storage{
			Index:             indexdata.CloneFieldStorageSlots(state.Index, slotCount),
			KeyIndex:          keyIndex,
			NilIndex:          indexdata.CloneFieldStorageSlots(state.NilIndex, slotCount),
			LenIndex:          nextLenIndex,
			LenZeroComplement: nextLenZeroComplement,
			Measure:           indexdata.CloneMeasureStorageSlots(state.Measure, len(cfg.Schema.Measures)),
			Universe:          nextUniverse,
		},
		Publish:        true,
		Stats:          true,
		BuildTime:      buildTime,
		BuildRPS:       int(float64(recordCount) / max(buildTime.Seconds(), 1)),
		LenLoaded:      state.LenLoaded,
		KeyIndexLoaded: true,
	}, nil
}
