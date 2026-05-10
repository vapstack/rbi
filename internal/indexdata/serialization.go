package indexdata

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	MaxStoredStringLen = 8 << 20

	fieldStorageEncodingFlat    byte = 1
	fieldStorageEncodingChunked byte = 2

	fieldIndexChunkEncodingString byte = 1
	fieldIndexChunkEncodingRaw8   byte = 2

	indexKeyEncodingString byte = 1
	indexKeyEncodingRaw8   byte = 2
)

func (storage MeasureStorage) WriteInto(writer *bufio.Writer) error {
	rows := storage.Rows()
	if err := writeUvarint(writer, uint64(rows)); err != nil {
		return err
	}
	if rows == 0 {
		return nil
	}

	if storage.flat != nil {
		for i, id := range storage.flat.ids {
			if err := writeUvarint(writer, id); err != nil {
				return err
			}
			if err := writeUvarint(writer, storage.flat.values[i]); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < len(storage.chunked.refsByID); i++ {
		chunk := storage.chunked.refsByID[i].chunk
		for j, id := range chunk.ids {
			if err := writeUvarint(writer, id); err != nil {
				return err
			}
			if err := writeUvarint(writer, chunk.values[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (storage FieldStorage) WriteInto(writer *bufio.Writer) error {
	switch {

	case storage.chunked != nil:
		if err := writer.WriteByte(fieldStorageEncodingChunked); err != nil {
			return fmt.Errorf("encode: writing storage encoding: %w", err)
		}
		root := storage.chunked
		if err := writeUvarint(writer, uint64(len(root.pages))); err != nil {
			return fmt.Errorf("encode: writing page count: %w", err)
		}
		for i := range root.pages {
			page := root.pages[i]
			if err := writeUvarint(writer, uint64(len(page.refs))); err != nil {
				return fmt.Errorf("encode: writing page refs: %w", err)
			}
			for j := range page.refs {
				if err := page.refs[j].chunk.writeInto(writer); err != nil {
					return err
				}
			}
		}
		return nil

	case storage.flat != nil:
		if err := writer.WriteByte(fieldStorageEncodingFlat); err != nil {
			return fmt.Errorf("encode: writing storage encoding: %w", err)
		}
		entries := storage.flat.entries
		if err := writeUvarint(writer, uint64(len(entries))); err != nil {
			return fmt.Errorf("encode: writing flat len: %w", err)
		}
		return WriteEntries(writer, entries)

	default:
		return fmt.Errorf("encode: empty field storage")
	}
}

func (chunk *fieldIndexChunk) writeInto(writer *bufio.Writer) error {
	if chunk == nil || chunk.keyCount() == 0 {
		return fmt.Errorf("encode: empty chunk")
	}
	if chunk.hasNumericKeys() {
		if err := writer.WriteByte(fieldIndexChunkEncodingRaw8); err != nil {
			return fmt.Errorf("encode: writing chunk encoding: %w", err)
		}
		if err := writeUvarint(writer, uint64(chunk.keyCount())); err != nil {
			return fmt.Errorf("encode: writing numeric chunk len: %w", err)
		}

		var buf [8]byte
		for i := 0; i < chunk.keyCount(); i++ {
			binary.BigEndian.PutUint64(buf[:], chunk.keyAt(i).U64())
			if _, err := writer.Write(buf[:]); err != nil {
				return fmt.Errorf("encode: writing numeric chunk key: %w", err)
			}
			if err := chunk.postingAt(i).WriteTo(writer); err != nil {
				return fmt.Errorf("encode: writing numeric chunk posting: %w", err)
			}
		}
		return nil
	}

	if err := writer.WriteByte(fieldIndexChunkEncodingString); err != nil {
		return fmt.Errorf("encode: writing chunk encoding: %w", err)
	}
	if err := writeUvarint(writer, uint64(len(chunk.stringRefs))); err != nil {
		return fmt.Errorf("encode: writing string chunk len: %w", err)
	}

	for i := 0; i < chunk.keyCount(); i++ {
		ref := chunk.stringRefs[i]
		if err := writeUvarint(writer, uint64(fieldIndexStringRefLen(ref))); err != nil {
			return fmt.Errorf("encode: writing string chunk key len: %w", err)
		}

		if fieldIndexStringRefLen(ref) > 0 {
			start := fieldIndexStringRefOff(ref)
			end := start + fieldIndexStringRefLen(ref)
			if _, err := writer.Write(chunk.stringData[start:end]); err != nil {
				return fmt.Errorf("encode: writing string chunk key: %w", err)
			}
		}

		if err := chunk.postingAt(i).WriteTo(writer); err != nil {
			return fmt.Errorf("encode: writing string chunk posting: %w", err)
		}
	}
	return nil
}

func WriteEntries(writer *bufio.Writer, entries []Entry) error {
	for i := range entries {
		if err := entries[i].WriteInto(writer); err != nil {
			return err
		}
	}
	return nil
}

func (entry Entry) WriteInto(writer *bufio.Writer) error {
	if err := WriteKey(writer, entry.Key); err != nil {
		return fmt.Errorf("encode: writing entry key: %w", err)
	}
	if err := entry.IDs.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing entry posting: %w", err)
	}
	return nil
}

func WriteKey(writer *bufio.Writer, key keycodec.IndexKey) error {
	if key.IsNumeric() {
		if err := writer.WriteByte(indexKeyEncodingRaw8); err != nil {
			return err
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], key.U64())
		_, err := writer.Write(b[:])
		return err
	}
	if err := writer.WriteByte(indexKeyEncodingString); err != nil {
		return err
	}
	return writeString(writer, key.UnsafeString())
}

func ReadMeasureStorage(reader *bufio.Reader, keep bool) (MeasureStorage, error) {
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return MeasureStorage{}, err
	}
	if count > uint64(^uint(0)>>1) {
		return MeasureStorage{}, fmt.Errorf("measure row count overflows int: %v", count)
	}

	var entries []MeasureEntry
	if keep {
		entries = GetMeasureEntrySlice(int(count))
	}

	for i := uint64(0); i < count; i++ {
		id, err := binary.ReadUvarint(reader)
		if err != nil {
			if entries != nil {
				PutMeasureEntrySlice(entries)
			}
			return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d id: %w", i+1, count, err)
		}

		value, err := binary.ReadUvarint(reader)
		if err != nil {
			if entries != nil {
				PutMeasureEntrySlice(entries)
			}
			return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d value: %w", i+1, count, err)
		}

		if keep {
			entries = append(entries, MeasureEntry{ID: id, Value: value})
		}
	}

	if !keep {
		return MeasureStorage{}, nil
	}
	return NewMeasureStorageFromEntriesOwned(entries), nil
}

func ReadFieldStorage(reader *bufio.Reader, keep bool, section string, fieldName string) (FieldStorage, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return FieldStorage{}, fmt.Errorf("reading storage encoding: %w", err)
	}
	switch tag {

	case fieldStorageEncodingFlat:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return FieldStorage{}, fmt.Errorf("reading flat len: %w", err)
		}
		if !keep {
			for i := uint64(0); i < count; i++ {
				if err := SkipEntry(reader); err != nil {
					return FieldStorage{}, fmt.Errorf("skipping flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
				}
			}
			return FieldStorage{}, nil
		}

		if count > uint64(^uint(0)>>1) {
			return FieldStorage{}, fmt.Errorf("flat entry count overflows int for field %q in %s: %v", fieldName, section, count)
		}

		entries := GetFieldEntrySlice(int(count))[:int(count)]
		n := 0
		for i := uint64(0); i < count; i++ {
			ent, err := ReadEntry(reader)
			if err != nil {
				for j := 0; j < n; j++ {
					entries[j].IDs.Release()
				}
				PutFieldEntrySlice(entries)
				return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
			}
			if ent.IDs.IsEmpty() {
				continue
			}
			entries[n] = ent
			n++
		}
		entries = entries[:n]
		if len(entries) == 0 {
			PutFieldEntrySlice(entries)
			return FieldStorage{}, nil
		}
		return newFlatFieldStorage(&entries), nil

	case fieldStorageEncodingChunked:
		pageCount, err := binary.ReadUvarint(reader)
		if err != nil {
			return FieldStorage{}, fmt.Errorf("reading page count: %w", err)
		}

		if !keep {
			for i := uint64(0); i < pageCount; i++ {
				refCount, err := binary.ReadUvarint(reader)
				if err != nil {
					return FieldStorage{}, fmt.Errorf("reading page ref count %d/%d for field %q in %s: %w", i+1, pageCount, fieldName, section, err)
				}
				for j := uint64(0); j < refCount; j++ {
					if err = skipFieldIndexChunk(reader); err != nil {
						return FieldStorage{}, fmt.Errorf("skipping chunk page=%d/%d ref=%d/%d for field %q in %s: %w", i+1, pageCount, j+1, refCount, fieldName, section, err)
					}
				}
			}
			return FieldStorage{}, nil
		}

		builder := newFieldIndexChunkBuilder(0)
		for i := uint64(0); i < pageCount; i++ {
			refCount, err := binary.ReadUvarint(reader)
			if err != nil {
				builder.releaseOwned()
				return FieldStorage{}, fmt.Errorf("reading page ref count %d/%d for field %q in %s: %w", i+1, pageCount, fieldName, section, err)
			}
			refs := fieldIndexChunkRefSlicePool.Get(int(refCount))

			for j := uint64(0); j < refCount; j++ {
				chunk, err := readFieldIndexChunk(reader)
				if err != nil {
					putOwnedFieldIndexChunkRefSlice(refs)
					builder.releaseOwned()
					return FieldStorage{}, fmt.Errorf("reading chunk page=%d/%d ref=%d/%d for field %q in %s: %w", i+1, pageCount, j+1, refCount, fieldName, section, err)
				}
				if chunk == nil || chunk.keyCount() == 0 {
					continue
				}

				last := chunk.keyCount() - 1
				refs = append(refs, fieldIndexChunkRef{
					last:  chunk.keyAt(last),
					chunk: chunk,
				})
			}
			if len(refs) > 0 {
				builder.appendOwnedPage(newFieldIndexChunkDirPageOwned(refs))
			} else {
				fieldIndexChunkRefSlicePool.Put(refs)
			}
		}
		return newChunkedFieldStorage(builder.root()), nil

	default:
		return FieldStorage{}, fmt.Errorf("invalid field storage encoding %v", tag)
	}
}

func readFieldIndexChunk(reader *bufio.Reader) (*fieldIndexChunk, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("reading field chunk encoding: %w", err)
	}
	switch tag {

	case fieldIndexChunkEncodingRaw8:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading numeric chunk len: %w", err)
		}
		if count == 0 {
			return nil, nil
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("numeric chunk len overflows int: %v", count)
		}
		keys := fieldUint64Slice(int(count))
		posts := fieldPostingSlice(int(count))
		var buf [8]byte

		for i := range keys {
			if _, err = io.ReadFull(reader, buf[:]); err != nil {
				releaseReadFieldChunkBuffers(posts, keys, nil, nil)
				return nil, fmt.Errorf("reading numeric chunk key %d/%d: %w", i+1, len(keys), err)
			}

			keys[i] = binary.BigEndian.Uint64(buf[:])
			ids, err := posting.ReadFrom(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, keys, nil, nil)
				return nil, fmt.Errorf("reading numeric chunk posting %d/%d: %w", i+1, len(keys), err)
			}
			posts[i] = ids
		}
		return newNumericFieldIndexChunk(posts, keys, postingRows(posts)), nil

	case fieldIndexChunkEncodingString:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading string chunk len: %w", err)
		}
		if count == 0 {
			return nil, nil
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("string chunk len overflows int: %v", count)
		}

		refs := fieldStringRefSlice(int(count))
		posts := fieldPostingSlice(int(count))
		data := pooled.GetByteSlice(int(count) * 8)

		for i := range refs {
			n, err := binary.ReadUvarint(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				return nil, fmt.Errorf("reading string chunk key len %d/%d: %w", i+1, len(refs), err)
			}

			if n > uint64(^uint(0)>>1) {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				return nil, fmt.Errorf("string chunk key len overflows int at entry %d/%d: %v", i+1, len(refs), n)
			}

			keyLen := int(n)
			start := len(data)

			if cap(data)-len(data) < keyLen {
				next := pooled.GetByteSlice(len(data) + keyLen)
				next = append(next, data...)
				pooled.PutByteSlice(data)
				data = next
			}

			data = data[:start+keyLen]
			if keyLen > 0 {
				if _, err = io.ReadFull(reader, data[start:start+keyLen]); err != nil {
					releaseReadFieldChunkBuffers(posts, nil, refs, data)
					return nil, fmt.Errorf("reading string chunk key %d/%d: %w", i+1, len(refs), err)
				}
			}

			if !fieldIndexStringRefFits(start, keyLen) {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				if keyLen > fieldIndexStringRefMax {
					return nil, fmt.Errorf("string chunk key len exceeds uint16 at entry %d/%d: %d", i+1, len(refs), keyLen)
				}
				return nil, fmt.Errorf("string chunk key offset exceeds uint16 at entry %d/%d: %d", i+1, len(refs), start)
			}

			refs[i] = newFieldIndexStringRef(start, keyLen)
			ids, err := posting.ReadFrom(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				return nil, fmt.Errorf("reading string chunk posting %d/%d: %w", i+1, len(refs), err)
			}
			posts[i] = ids
		}

		return newStringFieldIndexChunk(posts, refs, data, postingRows(posts)), nil

	default:
		return nil, fmt.Errorf("invalid field chunk encoding %v", tag)
	}
}

func releaseReadFieldChunkBuffers(posts []posting.List, keys []uint64, refs []fieldIndexStringRef, data []byte) {
	posting.ReleaseAll(posts)
	posting.PutSlice(posts)
	pooled.PutUint64Slice(keys)
	pooled.PutUint32Slice(refs)
	pooled.PutByteSlice(data)
}

func skipFieldIndexChunk(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("reading field chunk encoding: %w", err)
	}
	switch tag {

	case fieldIndexChunkEncodingRaw8:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return fmt.Errorf("reading numeric chunk len: %w", err)
		}
		for i := uint64(0); i < count; i++ {
			if _, err = io.CopyN(io.Discard, reader, 8); err != nil {
				return fmt.Errorf("skipping numeric chunk key %d/%d: %w", i+1, count, err)
			}
			if err = posting.Skip(reader); err != nil {
				return fmt.Errorf("skipping numeric chunk posting %d/%d: %w", i+1, count, err)
			}
		}
		return nil

	case fieldIndexChunkEncodingString:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return fmt.Errorf("reading string chunk len: %w", err)
		}
		for i := uint64(0); i < count; i++ {
			n, err := binary.ReadUvarint(reader)
			if err != nil {
				return fmt.Errorf("reading string chunk key len %d/%d: %w", i+1, count, err)
			}
			if n > 0 {
				if _, err = io.CopyN(io.Discard, reader, int64(n)); err != nil {
					return fmt.Errorf("skipping string chunk key %d/%d: %w", i+1, count, err)
				}
			}
			if err = posting.Skip(reader); err != nil {
				return fmt.Errorf("skipping string chunk posting %d/%d: %w", i+1, count, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("invalid field chunk encoding %v", tag)
	}
}

func ReadEntry(reader *bufio.Reader) (Entry, error) {
	key, err := ReadKey(reader)
	if err != nil {
		return Entry{}, fmt.Errorf("reading Entry key: %w", err)
	}
	ids, err := posting.ReadFrom(reader)
	if err != nil {
		return Entry{}, fmt.Errorf("reading Entry posting: %w", err)
	}
	return Entry{Key: key, IDs: ids}, nil
}

func SkipEntry(reader *bufio.Reader) error {
	if err := SkipKey(reader); err != nil {
		return fmt.Errorf("skipping Entry key: %w", err)
	}
	if err := posting.Skip(reader); err != nil {
		return fmt.Errorf("skipping Entry posting: %w", err)
	}
	return nil
}

func ReadKey(reader *bufio.Reader) (keycodec.IndexKey, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return keycodec.IndexKey{}, err
	}
	switch tag {

	case indexKeyEncodingString:
		s, err := readString(reader)
		if err != nil {
			return keycodec.IndexKey{}, err
		}
		if err = ValidateIndexedStringKeyLen(len(s)); err != nil {
			return keycodec.IndexKey{}, err
		}
		return keycodec.FromString(s), nil

	case indexKeyEncodingRaw8:
		var buf [8]byte
		if _, err = io.ReadFull(reader, buf[:]); err != nil {
			return keycodec.IndexKey{}, err
		}
		return keycodec.FromU64(binary.BigEndian.Uint64(buf[:])), nil

	default:
		return keycodec.IndexKey{}, fmt.Errorf("invalid Entry key encoding %v", tag)
	}
}

func SkipKey(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch tag {
	case indexKeyEncodingString:
		return skipString(reader)
	case indexKeyEncodingRaw8:
		_, err = io.CopyN(io.Discard, reader, 8)
		return err
	default:
		return fmt.Errorf("invalid Entry key encoding %v", tag)
	}
}

func writeString(writer *bufio.Writer, s string) error {
	if err := writeUvarint(writer, uint64(len(s))); err != nil {
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

func writeUvarint(writer *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	if _, err := writer.Write(buf[:n]); err != nil {
		return err
	}
	return nil
}

func readString(reader *bufio.Reader) (string, error) {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	if n > MaxStoredStringLen {
		return "", fmt.Errorf("string len %v exceeds limit (%v)", n, MaxStoredStringLen)
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

func skipString(reader *bufio.Reader) error {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n > MaxStoredStringLen {
		return fmt.Errorf("string len %v exceeds limit (%v)", n, MaxStoredStringLen)
	}
	if n > uint64(^uint(0)>>1) {
		return fmt.Errorf("string len %v overflows int", n)
	}
	if _, err = io.CopyN(io.Discard, reader, int64(n)); err != nil {
		return err
	}
	return nil
}
