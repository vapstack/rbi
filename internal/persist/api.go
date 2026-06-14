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
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"
)

type LoadConfig struct {
	File        string
	DBPath      string
	Bucket      []byte
	CurrentSeq  uint64
	Schema      *schema.Schema
	StrKey      bool
	StrKeyIndex bool
}

type LoadResult struct {
	Storage           snapshot.Storage
	PlannerStats      *rbistats.PlannerSnapshot
	SkipFields        map[string]struct{}
	SkipMeasureFields map[string]struct{}
	LenLoaded         bool
	KeyIndexLoaded    bool
}

type StoreConfig struct {
	File           string
	BucketSeq      uint64
	Schema         *schema.Schema
	StrKey         bool
	StrKeyIndex    bool
	KeyIndexLoaded bool
	Snapshot       *snapshot.View
	PlannerStats   *rbistats.PlannerSnapshot
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
		return LoadResult{}, diag.wrap("read_version", fmt.Errorf("%w: reading version: %w", rbierrors.ErrPersistedIndexInvalid, err))
	}
	diag.version = ver
	diag.versionKnown = true

	switch ver {
	case 29:
		result, err = loadV29(reader, cfg)
	default:
		return LoadResult{}, diag.wrap("version", fmt.Errorf("%w: unsupported persisted index version: %v", rbierrors.ErrPersistedIndexInvalid, ver))
	}
	if err != nil {
		if errors.Is(err, rbierrors.ErrPersistedIndexStale) || errors.Is(err, rbierrors.ErrPersistedIndexInvalid) {
			return LoadResult{}, diag.wrap("load_index", err)
		}
		return LoadResult{}, diag.wrap("load_index", fmt.Errorf("error loading index: %w", err))
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

	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
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
	if err = storeV29(buf, cfg); err != nil {
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

func loadV29(reader *bufio.Reader, cfg LoadConfig) (LoadResult, error) {
	storedSeq, err := binary.ReadUvarint(reader)
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading bucket sequence: %w", err)
	}
	if storedSeq != cfg.CurrentSeq {
		return LoadResult{}, fmt.Errorf("%w: bucket sequence mismatch (stored=%v, current=%v)", rbierrors.ErrPersistedIndexStale, storedSeq, cfg.CurrentSeq)
	}

	if err = readKeyStorageHeader(reader, cfg); err != nil {
		return LoadResult{}, err
	}

	result, err := loadPayloadWithKey(reader, cfg.Schema, cfg.StrKey, cfg.StrKeyIndex)
	if err != nil {
		return LoadResult{}, fmt.Errorf("%w: %w", rbierrors.ErrPersistedIndexInvalid, err)
	}
	return result, nil
}

func readKeyStorageHeader(reader *bufio.Reader, cfg LoadConfig) error {
	kind, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("%w: reading key storage kind: %w", rbierrors.ErrPersistedIndexInvalid, err)
	}
	if !cfg.StrKey {
		if kind != keyStorageNumeric {
			return fmt.Errorf("%w: key storage kind mismatch (stored=%d, expected=%d)", rbierrors.ErrPersistedIndexInvalid, kind, keyStorageNumeric)
		}
		return nil
	}
	if kind != keyStorageStringDurableID {
		return fmt.Errorf("%w: key storage kind mismatch (stored=%d, expected=%d)", rbierrors.ErrPersistedIndexInvalid, kind, keyStorageStringDurableID)
	}
	return nil
}

func loadPayload(reader *bufio.Reader, s *schema.Schema) (LoadResult, error) {
	return loadPayloadWithKey(reader, s, false, false)
}

func loadPayloadWithKey(reader *bufio.Reader, s *schema.Schema, strKey, strKeyIndex bool) (LoadResult, error) {
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

	keyIndex, keyIndexLoaded, err := readKeyIndexSection(reader, strKey, strKeyIndex)
	if err != nil {
		return LoadResult{}, fmt.Errorf("decode: reading key index section: %w", err)
	}
	if keyIndexLoaded && keyIndex.KeyCount() == 0 && !universe.IsEmpty() {
		return LoadResult{}, fmt.Errorf("decode: empty key index with non-empty universe")
	}
	keyIndexOwned := true
	defer func() {
		if keyIndexOwned {
			keyIndex.Release()
		}
	}()

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
		KeyIndex:          keyIndex,
		NilIndex:          nilIndex,
		LenIndex:          lenIndex,
		LenZeroComplement: lenZeroComplement,
		Measure:           measure,
		Universe:          universe,
	}
	universeOwned = false
	keyIndexOwned = false
	return LoadResult{
		Storage:           storage,
		PlannerStats:      plannerStats,
		SkipFields:        skipFields,
		SkipMeasureFields: skipMeasureFields,
		LenLoaded:         lenLoaded,
		KeyIndexLoaded:    keyIndexLoaded,
	}, nil
}

func storeV29(writer *bufio.Writer, cfg StoreConfig) error {
	if err := writer.WriteByte(currentPersistedIndexVersion); err != nil {
		return fmt.Errorf("store: writing version: %w", err)
	}
	if err := writeSidecarUvarint(writer, cfg.BucketSeq); err != nil {
		return fmt.Errorf("store: writing bucket sequence: %w", err)
	}
	if err := writeKeyStorageHeader(writer, cfg); err != nil {
		return err
	}
	return storePayloadWithKey(writer, cfg.Schema, cfg.Snapshot, cfg.PlannerStats, cfg.StrKey && cfg.StrKeyIndex, cfg.KeyIndexLoaded)
}

func writeKeyStorageHeader(writer *bufio.Writer, cfg StoreConfig) error {
	if !cfg.StrKey {
		if err := writer.WriteByte(keyStorageNumeric); err != nil {
			return fmt.Errorf("store: writing key storage kind: %w", err)
		}
		return nil
	}
	if err := writer.WriteByte(keyStorageStringDurableID); err != nil {
		return fmt.Errorf("store: writing key storage kind: %w", err)
	}
	return nil
}

func storePayload(writer *bufio.Writer, s *schema.Schema, snap *snapshot.View, plannerStats *rbistats.PlannerSnapshot) error {
	return storePayloadWithKey(writer, s, snap, plannerStats, false, false)
}

func storePayloadWithKey(writer *bufio.Writer, s *schema.Schema, snap *snapshot.View, plannerStats *rbistats.PlannerSnapshot, strKeyIndex, keyIndexLoaded bool) error {
	universe := snap.Universe.Borrow()
	if err := universe.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing universe: %w", err)
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

	if err := writeKeyIndexSection(writer, snap.KeyIndex, strKeyIndex, keyIndexLoaded); err != nil {
		return err
	}

	return writePlannerStatsSnapshot(writer, plannerStats)
}

func readKeyIndexSection(reader *bufio.Reader, strKey, strKeyIndex bool) (indexdata.FieldStorage, bool, error) {
	state, err := reader.ReadByte()
	if err != nil {
		return indexdata.FieldStorage{}, false, fmt.Errorf("reading key index state: %w", err)
	}
	if !strKey && state != keyIndexStateAbsent {
		return indexdata.FieldStorage{}, false, fmt.Errorf("numeric key storage cannot have key index state %d", state)
	}
	keep := strKey && strKeyIndex
	switch state {
	case keyIndexStateAbsent:
		return indexdata.FieldStorage{}, false, nil
	case keyIndexStateEmpty:
		return indexdata.FieldStorage{}, keep, nil
	case keyIndexStatePresent:
		storage, err := indexdata.ReadFieldStorage(reader, keep, "key index", schema.ReservedKeyFieldName)
		if err != nil {
			return indexdata.FieldStorage{}, false, err
		}
		if keep && storage.KeyCount() == 0 {
			return indexdata.FieldStorage{}, false, fmt.Errorf("present key index section decoded empty storage")
		}
		return storage, keep, nil
	default:
		return indexdata.FieldStorage{}, false, fmt.Errorf("invalid key index state %d", state)
	}
}

func writeKeyIndexSection(writer *bufio.Writer, storage indexdata.FieldStorage, strKeyIndex, keyIndexLoaded bool) error {
	if !strKeyIndex || !keyIndexLoaded {
		if err := writer.WriteByte(keyIndexStateAbsent); err != nil {
			return fmt.Errorf("encode: writing key index state: %w", err)
		}
		return nil
	}
	if storage.KeyCount() == 0 {
		if err := writer.WriteByte(keyIndexStateEmpty); err != nil {
			return fmt.Errorf("encode: writing key index state: %w", err)
		}
		return nil
	}
	if err := writer.WriteByte(keyIndexStatePresent); err != nil {
		return fmt.Errorf("encode: writing key index state: %w", err)
	}
	if err := storage.WriteInto(writer); err != nil {
		return fmt.Errorf("encode: writing key index: %w", err)
	}
	return nil
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
