package rebuild

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"go.etcd.io/bbolt"
)

type buildField struct {
	acc          schema.IndexedFieldAccessor
	write        schema.BuildFieldWriteAccessorFn
	writeChecked schema.BuildFieldWriteCheckedAccessorFn
	slice        bool
	numeric      bool
}

type rawdata struct {
	v   []byte
	key []byte
	idx uint64
	pos uint64
}

type buildData struct {
	fieldStates        []*schema.BuildFieldState
	localUniverse      []posting.List
	localMeasureStates [][][][]indexdata.MeasureEntry
	keyIndex           indexdata.FieldStorage
	workerErrs         []error
	ok                 bool
}

func scan(cfg Config, active []buildField, activeMeasures []schema.MeasureFieldAccessor) (*buildData, error) {
	fieldStates := make([]*schema.BuildFieldState, len(active))
	for i := range active {
		fieldStates[i] = schema.NewBuildFieldState(active[i].slice)
	}

	workers := min(4, runtime.GOMAXPROCS(0))
	runTarget := schema.BuildFieldRunTargetEntries
	if len(activeMeasures) == 0 {
		scalarOnly := true
		for i := range active {
			if active[i].slice {
				scalarOnly = false
				break
			}
		}
		if scalarOnly {
			runTarget = scalarBuildFieldRunTargetEntries
		}
	}

	data := &buildData{
		fieldStates:        fieldStates,
		localUniverse:      make([]posting.List, workers),
		localMeasureStates: make([][][][]indexdata.MeasureEntry, workers),
		workerErrs:         make([]error, workers),
	}
	defer cleanupBuildFailure(&data.ok, data.fieldStates, data.localUniverse)
	defer cleanupMeasureStates(&data.ok, data.localMeasureStates)

	buildKeyIndex := cfg.StrKey && cfg.StrKeyIndex && !cfg.KeyIndexLoaded
	keyBuilder := indexdata.SortedUniqueStringFieldStorageBuilder{}
	if buildKeyIndex {
		keyBuilder.Init(0)
		defer func() {
			if !data.ok {
				keyBuilder.Release()
			}
		}()
	}

	jobs := make(chan rawdata, workers*64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(widx int) {
			defer wg.Done()

			lu := data.localUniverse[widx]
			lu.Release()
			lu = posting.List{}
			localStates := make([]schema.BuildFieldLocalState, len(active))
			for i := range active {
				localStates[i] = schema.NewBuildFieldLocalState(active[i].numeric, active[i].slice, runTarget)
			}
			defer cleanupLocalStates(localStates)

			localMeasures := make([][][]indexdata.MeasureEntry, len(cfg.Schema.Measures))
			measureOK := false
			defer func() {
				if !measureOK {
					releaseMeasureRuns(localMeasures)
				}
			}()

			for kv := range jobs {
				ptr, err := cfg.Decode(kv.v)
				if err != nil {
					data.localUniverse[widx] = lu
					data.workerErrs[widx] = fmt.Errorf(
						"worker=%d stage=decode scan_pos=%d %s idx=%d value_len=%d value_prefix_hex=%s: %w",
						widx,
						kv.pos,
						formatBuildIndexKeyDiagnostic(cfg.StrKey, kv.key),
						kv.idx,
						len(kv.v),
						formatDiagnosticBytesPrefix(kv.v, 32),
						err,
					)
					cancel()
					return
				}
				idx := kv.idx

				lu = lu.BuildAdded(idx)

				for k := range active {
					sink := schema.BuildSink{
						State: &localStates[k],
						Idx:   idx,
					}
					var fieldErr error
					if active[k].writeChecked != nil {
						fieldErr = active[k].writeChecked(ptr, sink)
					} else {
						active[k].write(ptr, sink)
					}
					if fieldErr != nil {
						cfg.Release(ptr)
						data.localUniverse[widx] = lu
						data.workerErrs[widx] = fmt.Errorf(
							"worker=%d stage=index scan_pos=%d %s idx=%d field=%q: %w",
							widx,
							kv.pos,
							formatBuildIndexKeyDiagnostic(cfg.StrKey, kv.key),
							kv.idx,
							active[k].acc.Name,
							fieldErr,
						)
						cancel()
						return
					}
					if localStates[k].ShouldFlushRegular() {
						localStates[k].FlushRegularInto(fieldStates[k])
					}
				}

				for _, acc := range activeMeasures {
					if value, ok := acc.Read(ptr); ok {
						runs := localMeasures[acc.Ordinal]
						if runs == nil {
							runs = indexdata.GetMeasureEntrySlots(0)
						}
						if len(runs) == 0 || len(runs[len(runs)-1]) == measureBuildRunTargetEntries {
							capHint := 0
							if len(runs) > 0 {
								capHint = measureBuildRunTargetEntries
							}
							runs = append(runs, indexdata.GetMeasureEntrySlice(capHint))
						}
						buf := runs[len(runs)-1]
						buf = append(buf, indexdata.MeasureEntry{ID: idx, Value: value})
						runs[len(runs)-1] = buf
						localMeasures[acc.Ordinal] = runs
					}
				}
				cfg.Release(ptr)
			}

			for i := range localStates {
				localStates[i].FlushAllInto(fieldStates[i])
			}
			data.localUniverse[widx] = lu
			data.localMeasureStates[widx] = localMeasures
			measureOK = true
		}(i)
	}

	err := cfg.Bolt.View(func(tx *bbolt.Tx) error {
		defer wg.Wait()
		defer close(jobs)

		b := tx.Bucket(cfg.DataBucket)
		if b == nil {
			if buildKeyIndex {
				data.keyIndex = keyBuilder.Finish()
			}
			return nil
		}
		var stringMap *bbolt.Bucket
		if cfg.StrKey {
			stringMap = tx.Bucket(cfg.StrMapBucket)
			if stringMap == nil {
				return fmt.Errorf("string storage format: missing string map bucket %q", cfg.StrMapBucket)
			}
		}
		done := ctx.Done()
		c := b.Cursor()

		scanned := uint64(0)
		maxStringIdx := uint64(0)

		for k, v := c.First(); k != nil; k, v = c.Next() {

			if !cfg.StrKey && len(k) != 8 {
				return fmt.Errorf(
					"invalid uint64 key size scan_pos=%d key_len=%d key_prefix_hex=%s",
					scanned+1,
					len(k),
					formatDiagnosticBytesPrefix(k, 24),
				)
			}

			var idx uint64
			if cfg.StrKey {
				physical := v
				if len(v) < 8 {
					return fmt.Errorf(
						"invalid string value scan_pos=%d id=%q idx=%d value_len=%d value_prefix_hex=%s: string storage format: value shorter than %d bytes",
						scanned+1,
						string(k),
						idx,
						len(physical),
						formatDiagnosticBytesPrefix(physical, 32),
						8,
					)
				}
				idx = keycodec.U64FromBytes(v[:8])
				if idx == 0 {
					return fmt.Errorf(
						"invalid string value scan_pos=%d id=%q idx=%d value_len=%d value_prefix_hex=%s: string storage format: zero string id",
						scanned+1,
						string(k),
						idx,
						len(physical),
						formatDiagnosticBytesPrefix(physical, 32),
					)
				}
				v = v[8:]
				if idx > maxStringIdx {
					maxStringIdx = idx
				}
				if buildKeyIndex {
					// bbolt data keys are capped at 32 KiB, below FieldStringRefMax.
					keyBuilder.AppendBytes(k, idx)
				}
			} else {
				idx = keycodec.U64FromBytes(k)
			}

			select {
			case <-done:
				return nil
			case jobs <- rawdata{v: v, key: k, idx: idx, pos: scanned + 1}:
			}

			scanned++
		}
		if cfg.StrKey && stringMap.Sequence() < maxStringIdx {
			return fmt.Errorf("string storage format: string map sequence %d lower than max live idx %d", stringMap.Sequence(), maxStringIdx)
		}
		if buildKeyIndex {
			data.keyIndex = keyBuilder.Finish()
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("scan error: db=%q bucket=%q: %w", cfg.Bolt.Path(), string(cfg.DataBucket), err)
	}
	for _, err = range data.workerErrs {
		if err != nil {
			data.keyIndex.Release()
			data.keyIndex = indexdata.FieldStorage{}
			return nil, fmt.Errorf("scan error: db=%q bucket=%q: %w", cfg.Bolt.Path(), string(cfg.DataBucket), err)
		}
	}

	data.ok = true
	return data, nil
}

/**/

const (
	indexBuildGCStride               = 500_000
	scalarBuildFieldRunTargetEntries = max(2<<10, indexdata.FieldChunkTargetEntries*4)
	measureBuildRunTargetEntries     = 16 << 10
)

func cleanupBuildFailure(ok *bool, fieldStates []*schema.BuildFieldState, localUniverse []posting.List) {
	if *ok {
		return
	}
	for i := range fieldStates {
		fieldStates[i].Release()
	}
	for i := range localUniverse {
		localUniverse[i].Release()
	}
}

func cleanupLocalStates(localStates []schema.BuildFieldLocalState) {
	for i := range localStates {
		localStates[i].Release()
	}
}

func cleanupMeasureStates(ok *bool, states [][][][]indexdata.MeasureEntry) {
	if *ok {
		return
	}
	for i := range states {
		releaseMeasureRuns(states[i])
	}
}

func releaseMeasureRuns(states [][][]indexdata.MeasureEntry) {
	for i := range states {
		if states[i] != nil {
			indexdata.ReleaseMeasureEntrySlots(states[i])
			states[i] = nil
		}
	}
}

/**/

func formatDiagnosticBytesPrefix(b []byte, limit int) string {
	if len(b) == 0 {
		return "empty"
	}
	if limit <= 0 || len(b) <= limit {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%x...(len=%d)", b[:limit], len(b))
}

func formatBuildIndexKeyDiagnostic(strKey bool, key []byte) string {
	if strKey {
		s := string(key)
		if len(s) > 64 {
			return fmt.Sprintf("id=%q...(len=%d key_prefix_hex=%s)", s[:64], len(s), formatDiagnosticBytesPrefix(key, 24))
		}
		return fmt.Sprintf("id=%q", s)
	}
	if len(key) == 8 {
		return fmt.Sprintf("id=%d", keycodec.U64FromBytes(key))
	}
	return fmt.Sprintf("key_len=%d key_prefix_hex=%s", len(key), formatDiagnosticBytesPrefix(key, 24))
}
