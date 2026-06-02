package persist

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
)

type Errors struct {
	Stale   error
	Invalid error
}

type LoadConfig struct {
	File            string
	DBPath          string
	Bucket          []byte
	CurrentSeq      uint64
	Schema          *schema.Schema
	StrMapCompactAt int
	Errors          Errors
}

type LoadResult struct {
	Storage           snapshot.Storage
	StrMap            *strmap.Mapper
	PlannerStats      *qexec.PlannerStatsSnapshot
	SkipFields        map[string]struct{}
	SkipMeasureFields map[string]struct{}
	LenLoaded         bool
}

type StoreConfig struct {
	File         string
	BucketSeq    uint64
	Schema       *schema.Schema
	Snapshot     *snapshot.View
	PlannerStats *qexec.PlannerStatsSnapshot
}

const fileBufferSize = 64 << 10

func Load(cfg LoadConfig) (result LoadResult, err error) {
	diag := loadDiag{
		file:   cfg.File,
		dbPath: cfg.DBPath,
		bucket: string(cfg.Bucket),
		size:   -1,
	}
	var f *os.File
	defer func() {
		if err != nil {
			result.Storage.Release()
			result = LoadResult{}
		}
	}()
	defer func() {
		if f != nil {
			if closeErr := f.Close(); err == nil && closeErr != nil {
				err = diag.wrap("close", fmt.Errorf("closing persisted index file: %w", closeErr))
			}
		}
	}()
	defer recoverLoad(&err, &diag)

	f, err = os.Open(cfg.File)
	if err != nil {
		return LoadResult{}, err
	}

	if info, statErr := f.Stat(); statErr == nil {
		diag.size = info.Size()
	}

	reader := bufio.NewReaderSize(f, fileBufferSize)

	ver, err := reader.ReadByte()
	if err != nil {
		return LoadResult{}, diag.wrap("read_version", fmt.Errorf("%w: reading version: %w", cfg.Errors.Invalid, err))
	}
	diag.version = ver
	diag.versionKnown = true

	switch ver {
	case persistedIndexVersion:
		result, err = loadV26(reader, cfg)
	default:
		return LoadResult{}, diag.wrap("version", fmt.Errorf("%w: unsupported persisted index version: %v", cfg.Errors.Invalid, ver))
	}
	if err != nil {
		if errors.Is(err, cfg.Errors.Stale) || errors.Is(err, cfg.Errors.Invalid) {
			return LoadResult{}, diag.wrap("load_v26", err)
		}
		return LoadResult{}, diag.wrap("load_v26", fmt.Errorf("error loading index: %w", err))
	}
	return result, nil
}

func Store(cfg StoreConfig) (err error) {
	tmpFile := cfg.File + ".temp"
	removeTemp := true
	defer func() {
		if removeTemp {
			if removeErr := os.Remove(tmpFile); err == nil && removeErr != nil {
				err = fmt.Errorf("removing persisted index temp file: %w", removeErr)
			}
		}
	}()

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			closeErr := f.Close()
			closed = true
			if err == nil && closeErr != nil {
				err = fmt.Errorf("closing persisted index temp file: %w", closeErr)
			}
		}
	}()

	buf := bufio.NewWriterSize(f, fileBufferSize)
	if err = storeV26(buf, cfg); err != nil {
		return err
	}

	if err = buf.Flush(); err != nil {
		return fmt.Errorf("flushing write buffers: %w", err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("syncing persisted index temp file: %w", err)
	}

	err = f.Close()
	closed = true
	if err != nil {
		return fmt.Errorf("closing persisted index temp file: %w", err)
	}
	if err = os.Rename(tmpFile, cfg.File); err != nil {
		return err
	}
	removeTemp = false
	if err = syncDir(cfg.File); err != nil {
		return err
	}
	return nil
}

func loadV26(reader *bufio.Reader, cfg LoadConfig) (LoadResult, error) {
	storedSeq, err := binary.ReadUvarint(reader)
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading bucket sequence: %w", err)
	}
	if storedSeq != cfg.CurrentSeq {
		return LoadResult{}, fmt.Errorf("%w: bucket sequence mismatch (stored=%v, current=%v)", cfg.Errors.Stale, storedSeq, cfg.CurrentSeq)
	}

	result, err := loadPayload(reader, cfg.Schema, cfg.StrMapCompactAt)
	if err != nil {
		return LoadResult{}, fmt.Errorf("%w: %w", cfg.Errors.Invalid, err)
	}
	return result, nil
}

func loadPayload(reader *bufio.Reader, s *schema.Schema, strMapCompactAt int) (LoadResult, error) {
	universe, err := posting.ReadFrom(reader)
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading universe: %w", err)
	}
	universeOwned := true
	defer func() {
		if universeOwned {
			universe.Release()
		}
	}()

	sm, err := strmap.Read(reader, strMapCompactAt)
	if err != nil {
		return LoadResult{}, err
	}

	compatible, err := readFieldCompatibility(reader, s.Fields)
	if err != nil {
		return LoadResult{}, err
	}
	measureCompatible, err := readFieldCompatibility(reader, s.MeasureFields)
	if err != nil {
		return LoadResult{}, err
	}

	indexes, err := readIndexSections(reader, compatible, "regular index")
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading index sections: %w", err)
	}
	defer indexdata.ReleaseFieldStorageMap(indexes)

	nilIndexes, err := readIndexSections(reader, compatible, "nil index")
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading nil index sections: %w", err)
	}
	defer indexdata.ReleaseFieldStorageMap(nilIndexes)

	lenIndexes, err := readIndexSections(reader, compatible, "len index")
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading len index sections: %w", err)
	}
	defer indexdata.ReleaseFieldStorageMap(lenIndexes)

	measureIndexes, err := readMeasureIndexSections(reader, measureCompatible)
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading measure index sections: %w", err)
	}
	defer indexdata.ReleaseMeasureStorageMap(measureIndexes)

	plannerStats, err := readPlannerStatsSnapshot(reader, compatible)
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading planner stats: %w", err)
	}

	skipFields := make(map[string]struct{}, len(s.Fields))
	for name := range s.Fields {
		_, hasRegular := indexes[name]
		_, hasNil := nilIndexes[name]
		if compatible[name] && (hasRegular || hasNil) {
			skipFields[name] = struct{}{}
		}
	}
	skipMeasureFields := make(map[string]struct{}, len(s.MeasureFields))
	for name := range s.MeasureFields {
		if measureCompatible[name] {
			if _, hasMeasure := measureIndexes[name]; hasMeasure {
				skipMeasureFields[name] = struct{}{}
			}
		}
	}

	slotCount := len(s.Indexed)
	index := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	lenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	measureSlotCount := len(s.Measures)
	measure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for _, acc := range s.Indexed {
		if storage, ok := indexes[acc.Name]; ok {
			index[acc.Ordinal] = storage
			delete(indexes, acc.Name)
		}
		if storage, ok := nilIndexes[acc.Name]; ok {
			nilIndex[acc.Ordinal] = storage
			delete(nilIndexes, acc.Name)
		}
		if storage, ok := lenIndexes[acc.Name]; ok {
			lenIndex[acc.Ordinal] = storage
			delete(lenIndexes, acc.Name)
		}
	}
	for _, acc := range s.Measures {
		if storage, ok := measureIndexes[acc.Name]; ok {
			measure[acc.Ordinal] = storage
			delete(measureIndexes, acc.Name)
		}
	}

	lenZeroComplement := detectLenZeroComplement(lenIndex, s.Indexed)
	lenLoaded := true
	for name := range s.Fields {
		if _, ok := skipFields[name]; !ok {
			lenLoaded = false
			break
		}
	}
	if lenLoaded && !universe.IsEmpty() {
		for name, f := range s.Fields {
			if !f.Slice {
				continue
			}
			if _, ok := skipFields[name]; !ok {
				continue
			}
			acc, ok := s.IndexedByName[name]
			if !ok {
				lenLoaded = false
				break
			}
			if lenIndex[acc.Ordinal].KeyCount() == 0 {
				lenLoaded = false
				break
			}
		}
	}

	storage := snapshot.Storage{
		Index:             index,
		NilIndex:          nilIndex,
		LenIndex:          lenIndex,
		LenZeroComplement: lenZeroComplement,
		Measure:           measure,
		Universe:          universe,
	}
	universeOwned = false
	return LoadResult{
		Storage:           storage,
		StrMap:            sm,
		PlannerStats:      plannerStats,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasureFields,
		LenLoaded:         lenLoaded,
	}, nil
}

func storeV26(writer *bufio.Writer, cfg StoreConfig) error {
	if err := writer.WriteByte(persistedIndexVersion); err != nil {
		return fmt.Errorf("store: writing version: %w", err)
	}
	if err := writeSidecarUvarint(writer, cfg.BucketSeq); err != nil {
		return fmt.Errorf("store: writing bucket sequence: %w", err)
	}
	return storePayload(writer, cfg.Schema, cfg.Snapshot, cfg.PlannerStats)
}

func storePayload(writer *bufio.Writer, s *schema.Schema, snap *snapshot.View, plannerStats *qexec.PlannerStatsSnapshot) error {
	universe := snap.Universe.Borrow()
	if err := universe.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
	}

	if err := strmap.WriteSnapshot(writer, snap.StrMap); err != nil {
		return err
	}

	if err := writeFields(writer, s.Fields); err != nil {
		return err
	}
	if err := writeFields(writer, s.MeasureFields); err != nil {
		return err
	}

	fieldNames := sortedFieldNames(snap.FieldNameSet())
	if err := writeSidecarUvarint(writer, uint64(len(fieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range fieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		storage, _ := snap.FieldIndexStorage(field)
		if err := storage.WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	nilFieldNames := sortedFieldNames(snap.NilFieldNameSet())
	if err := writeSidecarUvarint(writer, uint64(len(nilFieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range nilFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		acc := snap.IndexedFieldByName[field]
		if err := snap.NilIndex[acc.Ordinal].WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	lenFieldNames := sortedFieldNames(snap.LenFieldNameSet())
	if err := writeSidecarUvarint(writer, uint64(len(lenFieldNames))); err != nil {
		return fmt.Errorf("encode: writing index family len: %w", err)
	}
	for _, field := range lenFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing index field %q name: %w", field, err)
		}
		acc := snap.IndexedFieldByName[field]
		if err := snap.LenIndex[acc.Ordinal].WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing index field %q: %w", field, err)
		}
	}

	measureFieldNames := sortedMapFieldNames(s.MeasureFields)
	if err := writeSidecarUvarint(writer, uint64(len(measureFieldNames))); err != nil {
		return fmt.Errorf("encode: writing measure index family len: %w", err)
	}
	for _, field := range measureFieldNames {
		if err := writeSidecarString(writer, field); err != nil {
			return fmt.Errorf("encode: writing measure field %q name: %w", field, err)
		}
		acc := s.MeasuresByName[field]
		storage := snap.Measure[acc.Ordinal]
		if err := storage.WriteInto(writer); err != nil {
			return fmt.Errorf("encode: writing measure field %q: %w", field, err)
		}
	}

	return writePlannerStatsSnapshot(writer, plannerStats)
}

func detectLenZeroComplement(indexes []indexdata.FieldStorage, access []schema.IndexedFieldAccessor) []bool {
	out := pooled.GetBoolSlice(len(access))[:len(access)]
	clear(out)
	for _, acc := range access {
		if indexdata.NewFieldIndexViewFromStorage(indexes[acc.Ordinal]).LookupCardinality(indexdata.LenIndexNonEmptyKey) > 0 {
			out[acc.Ordinal] = true
		}
	}
	return out
}

func syncDir(path string) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		dir = "."
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening persisted index directory: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("closing persisted index directory: %w", closeErr)
		}
	}()
	if err = f.Sync(); err != nil {
		return fmt.Errorf("syncing persisted index directory: %w", err)
	}
	return nil
}
