package persist

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sort"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbistats"
)

const currentPersistedIndexVersion = 29

const (
	keyStorageNumeric byte = iota
	keyStorageStringDurableID
)

const (
	keyIndexStateAbsent byte = iota
	keyIndexStateEmpty
	keyIndexStatePresent
)

func writeFields(writer *bufio.Writer, fields map[string]*schema.Field) error {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	if err := writeSidecarUvarint(writer, uint64(len(names))); err != nil {
		return fmt.Errorf("encode: writing fields len: %w", err)
	}
	for _, name := range names {
		if err := writeField(writer, name, fields[name]); err != nil {
			return fmt.Errorf("encode: writing field %q: %w", name, err)
		}
	}
	return nil
}

func writeField(writer *bufio.Writer, name string, f *schema.Field) error {
	if err := writeSidecarString(writer, name); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.Unique); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, uint64(f.IndexKind)); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, uint64(f.Kind)); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.Ptr); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.Slice); err != nil {
		return err
	}
	if err := writeSidecarBool(writer, f.UseVI); err != nil {
		return err
	}
	if err := writeSidecarString(writer, f.DBName); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, uint64(len(f.Index))); err != nil {
		return err
	}
	for _, idx := range f.Index {
		if idx < 0 {
			return fmt.Errorf("negative field index")
		}
		if err := writeSidecarUvarint(writer, uint64(idx)); err != nil {
			return err
		}
	}
	return nil
}

func readFieldCompatibilityEntry(reader *bufio.Reader, current map[string]*schema.Field) (string, bool, error) {
	name, err := readSidecarString(reader)
	if err != nil {
		return "", false, err
	}
	cur := current[name]
	compatible := cur != nil
	unique, err := readSidecarBool(reader)
	if err != nil {
		return "", false, err
	}
	indexKind, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", false, err
	}
	if indexKind > uint64(^schema.IndexKind(0)) {
		return "", false, fmt.Errorf("invalid IndexKind %d", indexKind)
	}
	fieldIndexKind := schema.IndexKind(indexKind)
	if err := schema.ValidateIndexKind(fieldIndexKind); err != nil {
		return "", false, err
	}
	kind, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", false, err
	}
	ptr, err := readSidecarBool(reader)
	if err != nil {
		return "", false, err
	}
	slice, err := readSidecarBool(reader)
	if err != nil {
		return "", false, err
	}
	useVI, err := readSidecarBool(reader)
	if err != nil {
		return "", false, err
	}
	dbName, err := readSidecarString(reader)
	if err != nil {
		return "", false, err
	}
	valIndexLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", false, err
	}
	if valIndexLen > uint64(^uint(0)>>1) {
		return "", false, fmt.Errorf("field index len overflows int")
	}
	if compatible && (cur.Unique != unique ||
		cur.IndexKind != fieldIndexKind ||
		cur.Kind != reflect.Kind(kind) ||
		cur.Ptr != ptr ||
		cur.Slice != slice ||
		cur.UseVI != useVI ||
		cur.DBName != dbName ||
		len(cur.Index) != int(valIndexLen)) {
		compatible = false
	}
	for i := uint64(0); i < valIndexLen; i++ {
		v, err := binary.ReadUvarint(reader)
		if err != nil {
			return "", false, err
		}
		if v > uint64(^uint(0)>>1) {
			return "", false, fmt.Errorf("field index element overflows int")
		}
		if compatible && cur.Index[i] != int(v) {
			compatible = false
		}
	}
	return name, compatible, nil
}

func readFieldCompatibility(reader *bufio.Reader, current map[string]*schema.Field) (map[string]bool, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading fields len: %w", err)
	}
	if count > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("decode: stored field count overflows int: %v", count)
	}
	compatible := make(map[string]bool, max(0, min(int(count), len(current))))
	seen := make(map[string]struct{}, max(0, min(int(count), len(current))))

	for i := uint64(0); i < count; i++ {
		name, ok, err := readFieldCompatibilityEntry(reader, current)
		if err != nil {
			return nil, fmt.Errorf("decode: reading field %d/%d: %w", i+1, count, err)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("decode: duplicate field %q at entry %d/%d", name, i+1, count)
		}
		seen[name] = struct{}{}
		if ok {
			compatible[name] = true
		}
	}
	return compatible, nil
}

func readIndexSections(reader *bufio.Reader, compatible map[string]bool, section string) (map[string]indexdata.FieldStorage, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("reading %s field count: %w", section, err)
	}
	if count == 0 {
		return make(map[string]indexdata.FieldStorage), nil
	}
	if count > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("reading %s field count overflows int: %v", section, count)
	}

	out := make(map[string]indexdata.FieldStorage, min(int(count), len(compatible)))
	seen := make(map[string]struct{}, min(int(count), len(compatible)))
	for i := uint64(0); i < count; i++ {
		f, err := readSidecarString(reader)
		if err != nil {
			indexdata.ReleaseFieldStorageMap(out)
			return nil, fmt.Errorf("reading %s field name %d/%d: %w", section, i+1, count, err)
		}
		if _, exists := seen[f]; exists {
			indexdata.ReleaseFieldStorageMap(out)
			return nil, fmt.Errorf("duplicate %s field %q at entry %d/%d", section, f, i+1, count)
		}
		seen[f] = struct{}{}

		keep := compatible[f]
		storage, err := indexdata.ReadFieldStorage(reader, keep, section, f)
		if err != nil {
			indexdata.ReleaseFieldStorageMap(out)
			return nil, fmt.Errorf("reading %s storage for field %q (%d/%d keep=%t): %w", section, f, i+1, count, keep, err)
		}
		if keep && storage.KeyCount() > 0 {
			out[f] = storage
		}
	}

	return out, nil
}

func readMeasureIndexSections(reader *bufio.Reader, compatible map[string]bool) (map[string]indexdata.MeasureStorage, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("reading measure index field count: %w", err)
	}
	if count == 0 {
		return make(map[string]indexdata.MeasureStorage), nil
	}
	if count > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("reading measure index field count overflows int: %v", count)
	}

	out := make(map[string]indexdata.MeasureStorage, min(int(count), len(compatible)))
	seen := make(map[string]struct{}, min(int(count), len(compatible)))
	for i := uint64(0); i < count; i++ {
		f, err := readSidecarString(reader)
		if err != nil {
			indexdata.ReleaseMeasureStorageMap(out)
			return nil, fmt.Errorf("reading measure index field name %d/%d: %w", i+1, count, err)
		}
		if _, exists := seen[f]; exists {
			indexdata.ReleaseMeasureStorageMap(out)
			return nil, fmt.Errorf("duplicate measure index field %q at entry %d/%d", f, i+1, count)
		}
		seen[f] = struct{}{}

		keep := compatible[f]
		storage, err := indexdata.ReadMeasureStorage(reader, keep)
		if err != nil {
			indexdata.ReleaseMeasureStorageMap(out)
			return nil, fmt.Errorf("reading measure index storage for field %q (%d/%d keep=%t): %w", f, i+1, count, keep, err)
		}
		if keep {
			out[f] = storage
		}
	}

	return out, nil
}

func sortedMapPlannerFieldNames(m map[string]rbistats.PlannerField) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for f := range m {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func sortedMapFieldNames(m map[string]*schema.Field) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for f := range m {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func sortedFieldNames(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func writeSidecarUvarint(writer *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	if _, err := writer.Write(buf[:n]); err != nil {
		return err
	}
	return nil
}

func writeSidecarBool(writer *bufio.Writer, v bool) error {
	if v {
		return writer.WriteByte(1)
	}
	return writer.WriteByte(0)
}

func readSidecarBool(reader *bufio.Reader) (bool, error) {
	v, err := reader.ReadByte()
	if v != 0 && v != 1 {
		return false, fmt.Errorf("corrupted bool value: %v", v)
	}
	return v > 0, err
}

func writeSidecarString(writer *bufio.Writer, s string) error {
	if err := writeSidecarUvarint(writer, uint64(len(s))); err != nil {
		return err
	}
	if len(s) == 0 {
		return nil
	}
	if _, err := io.WriteString(writer, s); err != nil {
		return err
	}
	return nil
}

func readSidecarString(reader *bufio.Reader) (string, error) {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	if n > indexdata.MaxStoredStringLen {
		return "", fmt.Errorf("string len %v exceeds limit (%v)", n, indexdata.MaxStoredStringLen)
	}
	if n > uint64(^uint(0)>>1) {
		return "", fmt.Errorf("string len %v overflows int", n)
	}
	b := make([]byte, int(n))
	if _, err = io.ReadFull(reader, b); err != nil {
		return "", err
	}
	s := unsafe.String(unsafe.SliceData(b), len(b))
	return s, nil
}
