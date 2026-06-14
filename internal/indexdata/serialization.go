package indexdata

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
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

func (s MeasureStorage) WriteInto(writer *bufio.Writer) error {
	rows := s.Rows()
	if err := writeUvarint(writer, uint64(rows)); err != nil {
		return err
	}
	if rows == 0 {
		return nil
	}

	if s.flat != nil {
		for i, id := range s.flat.ids {
			if err := writeUvarint(writer, id); err != nil {
				return err
			}
			if err := writeUvarint(writer, s.flat.values[i]); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < len(s.chunked.refsByID); i++ {
		chunk := s.chunked.refsByID[i].chunk
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

func (s FieldStorage) WriteInto(writer *bufio.Writer) error {
	switch {

	case s.chunked != nil:
		if err := writer.WriteByte(fieldStorageEncodingChunked); err != nil {
			return fmt.Errorf("encode: writing storage encoding: %w", err)
		}
		root := s.chunked
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

	case s.flat != nil:
		if err := writer.WriteByte(fieldStorageEncodingFlat); err != nil {
			return fmt.Errorf("encode: writing storage encoding: %w", err)
		}
		entries := s.flat.entries
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

		if chunk.posts == nil {
			for i := 0; i < len(chunk.numeric); i += 2 {
				if err := writeBEUint64(writer, chunk.numeric[i]); err != nil {
					return fmt.Errorf("encode: writing numeric chunk key: %w", err)
				}
				if err := posting.WriteSingleton(writer, chunk.numeric[i+1]); err != nil {
					return fmt.Errorf("encode: writing numeric chunk posting: %w", err)
				}
			}
			return nil
		}

		for i := range chunk.numeric {
			if err := writeBEUint64(writer, chunk.numeric[i]); err != nil {
				return fmt.Errorf("encode: writing numeric chunk key: %w", err)
			}
			if err := chunk.posts[i].WriteTo(writer); err != nil {
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

	if chunk.posts == nil {
		for i := range chunk.stringRefs {
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

			if err := posting.WriteSingleton(writer, chunk.numeric[i]); err != nil {
				return fmt.Errorf("encode: writing string chunk posting: %w", err)
			}
		}
		return nil
	}

	for i := range chunk.stringRefs {
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

		if err := chunk.posts[i].WriteTo(writer); err != nil {
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
	if err := writeKey(writer, entry.Key); err != nil {
		return fmt.Errorf("encode: writing entry key: %w", err)
	}
	if err := entry.IDs.WriteTo(writer); err != nil {
		return fmt.Errorf("encode: writing entry posting: %w", err)
	}
	return nil
}

func writeKey(writer *bufio.Writer, key keycodec.IndexKey) error {
	if key.IsNumeric() {
		if err := writer.WriteByte(indexKeyEncodingRaw8); err != nil {
			return err
		}
		return writeBEUint64(writer, key.U64())
	}
	if err := writer.WriteByte(indexKeyEncodingString); err != nil {
		return err
	}
	return writeString(writer, key.UnsafeString())
}

func ReadMeasureStorage(reader *bufio.Reader, keep bool) (MeasureStorage, error) {
	count, err := readUvarint(reader)
	if err != nil {
		return MeasureStorage{}, err
	}
	if count > uint64(^uint(0)>>1) {
		return MeasureStorage{}, fmt.Errorf("measure row count overflows int: %v", count)
	}
	rows := int(count)

	if !keep {
		for i := 0; i < rows; i++ {
			if _, err = readUvarint(reader); err != nil {
				return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d id: %w", i+1, rows, err)
			}
			if _, err = readUvarint(reader); err != nil {
				return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d value: %w", i+1, rows, err)
			}
		}
		return MeasureStorage{}, nil
	}

	if rows == 0 {
		return MeasureStorage{}, nil
	}

	if rows <= MeasureChunkThreshold {
		root := measureFlatRootPool.Get()
		root.ids = pooled.GetUint64Slice(rows)[:rows]
		root.values = pooled.GetUint64Slice(rows)[:rows]
		var prevID uint64
		for i := 0; i < rows; i++ {
			id, err := readUvarint(reader)
			if err != nil {
				measureFlatRootPool.Put(root)
				return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d id: %w", i+1, rows, err)
			}
			if i > 0 && id <= prevID {
				measureFlatRootPool.Put(root)
				return MeasureStorage{}, fmt.Errorf("measure rows must be strictly increasing at row %d/%d", i+1, rows)
			}
			value, err := readUvarint(reader)
			if err != nil {
				measureFlatRootPool.Put(root)
				return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d value: %w", i+1, rows, err)
			}
			root.ids[i] = id
			root.values[i] = value
			prevID = id
		}
		root.refs.Store(1)
		return MeasureStorage{flat: root}, nil
	}

	root := measureChunkedRootPool.Get()
	root.refsByID = measureChunkRefSlicePool.Get(rows/MeasureChunkTargetRows + 1)
	var prevID uint64
	havePrev := false
	for start := 0; start < rows; {
		size := min(MeasureChunkTargetRows, rows-start)
		chunk := measureChunkPool.Get()
		chunk.init(size)
		for i := 0; i < size; i++ {
			row := start + i
			id, err := readUvarint(reader)
			if err != nil {
				measureChunkPool.Put(chunk)
				measureChunkedRootPool.Put(root)
				return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d id: %w", row+1, rows, err)
			}
			if havePrev && id <= prevID {
				measureChunkPool.Put(chunk)
				measureChunkedRootPool.Put(root)
				return MeasureStorage{}, fmt.Errorf("measure rows must be strictly increasing at row %d/%d", row+1, rows)
			}
			value, err := readUvarint(reader)
			if err != nil {
				measureChunkPool.Put(chunk)
				measureChunkedRootPool.Put(root)
				return MeasureStorage{}, fmt.Errorf("reading measure row %d/%d value: %w", row+1, rows, err)
			}
			chunk.ids[i] = id
			chunk.values[i] = value
			prevID = id
			havePrev = true
		}
		chunk.refs.Store(1)
		root.appendChunkRef(chunk)
		start += size
	}
	root.refs.Store(1)
	return MeasureStorage{chunked: root}, nil
}

func ReadFieldStorage(reader *bufio.Reader, keep bool, section string, fieldName string) (FieldStorage, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return FieldStorage{}, fmt.Errorf("reading storage encoding: %w", err)
	}
	switch tag {

	case fieldStorageEncodingFlat:
		count, err := readUvarint(reader)
		if err != nil {
			return FieldStorage{}, fmt.Errorf("reading flat len: %w", err)
		}
		if !keep {
			for i := uint64(0); i < count; i++ {
				if err := skipEntry(reader); err != nil {
					return FieldStorage{}, fmt.Errorf("skipping flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
				}
			}
			return FieldStorage{}, nil
		}

		if count > uint64(^uint(0)>>1) {
			return FieldStorage{}, fmt.Errorf("flat entry count overflows int for field %q in %s: %v", fieldName, section, count)
		}

		entries := GetFieldEntrySlice(int(count))[:int(count)]
		var data []byte
		n := 0
		var prevKey keycodec.IndexKey
		var prevOff, prevLen int
		prevString := false
		havePrev := false
		for i := uint64(0); i < count; i++ {
			tag, err := reader.ReadByte()
			if err != nil {
				releaseReadFlatBuffers(entries, n, data)
				return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
			}
			var key keycodec.IndexKey
			keyOff := 0
			keyLen := 0
			keyString := false
			switch tag {
			case indexKeyEncodingString:
				keyString = true
				keyBytes, err := readUvarint(reader)
				if err != nil {
					releaseReadFlatBuffers(entries, n, data)
					return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
				}
				if keyBytes > uint64(^uint(0)>>1) {
					releaseReadFlatBuffers(entries, n, data)
					return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: string len %v overflows int", i+1, count, fieldName, section, keyBytes)
				}
				keyLen = int(keyBytes)
				if err = ValidateIndexedStringKeyLen(keyLen); err != nil {
					releaseReadFlatBuffers(entries, n, data)
					return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
				}
				keyOff = len(data)
				if keyLen > 0 {
					if data == nil {
						data = pooled.GetByteSlice(int(count) * 8)
					}
					if cap(data)-len(data) < keyLen {
						old := data
						next := pooled.GetByteSlice(len(data) + keyLen)
						next = append(next, data...)
						if len(old) > 0 {
							base := uintptr(unsafe.Pointer(unsafe.SliceData(old)))
							for j := 0; j < n; j++ {
								if !entries[j].Key.IsNumeric() {
									l := entries[j].Key.ByteLen()
									if l > 0 {
										p := uintptr(unsafe.Pointer(unsafe.StringData(entries[j].Key.UnsafeString())))
										off := int(p - base)
										entries[j].Key = keycodec.FromBytes(next[off : off+l])
									}
								}
							}
						}
						pooled.ReleaseByteSlice(old)
						data = next
					}
					data = data[:keyOff+keyLen]
					if err = readFull(reader, data[keyOff:keyOff+keyLen]); err != nil {
						releaseReadFlatBuffers(entries, n, data)
						return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
					}
					key = keycodec.FromBytes(data[keyOff : keyOff+keyLen])
				}

			case indexKeyEncodingRaw8:
				v, err := readBEUint64(reader)
				if err != nil {
					releaseReadFlatBuffers(entries, n, data)
					return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: %w", i+1, count, fieldName, section, err)
				}
				key = keycodec.FromU64(v)

			default:
				releaseReadFlatBuffers(entries, n, data)
				return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: invalid Entry key encoding %v", i+1, count, fieldName, section, tag)
			}

			if havePrev {
				prev := prevKey
				if prevString {
					prev = keycodec.FromBytes(data[prevOff : prevOff+prevLen])
				}
				if keycodec.Compare(prev, key) >= 0 {
					releaseReadFlatBuffers(entries, n, data)
					return FieldStorage{}, fmt.Errorf("flat entry keys must be strictly increasing for field %q in %s at entry %d/%d", fieldName, section, i+1, count)
				}
			}
			if keyString {
				prevString = true
				prevOff = keyOff
				prevLen = keyLen
			} else {
				prevString = false
				prevKey = key
			}
			havePrev = true

			ids, err := posting.ReadFrom(reader)
			if err != nil {
				releaseReadFlatBuffers(entries, n, data)
				return FieldStorage{}, fmt.Errorf("reading flat entry %d/%d for field %q in %s: reading Entry posting: %w", i+1, count, fieldName, section, err)
			}
			if ids.IsEmpty() {
				continue
			}
			entries[n] = Entry{Key: key, IDs: ids}
			n++
		}
		entries = entries[:n]
		if len(entries) == 0 {
			ReleaseFieldEntrySlice(entries)
			pooled.ReleaseByteSlice(data)
			return FieldStorage{}, nil
		}
		return newFlatFieldStorage(entries, data), nil

	case fieldStorageEncodingChunked:
		pageCount, err := readUvarint(reader)
		if err != nil {
			return FieldStorage{}, fmt.Errorf("reading page count: %w", err)
		}

		if !keep {
			for i := uint64(0); i < pageCount; i++ {
				refCount, err := readUvarint(reader)
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
		var prevKey keycodec.IndexKey
		havePrev := false
		for i := uint64(0); i < pageCount; i++ {
			refCount, err := readUvarint(reader)
			if err != nil {
				builder.releaseOwned()
				return FieldStorage{}, fmt.Errorf("reading page ref count %d/%d for field %q in %s: %w", i+1, pageCount, fieldName, section, err)
			}
			if refCount > fieldIndexDirPageTargetRefs {
				builder.releaseOwned()
				return FieldStorage{}, fmt.Errorf("page ref count %d exceeds limit %d for field %q in %s", refCount, fieldIndexDirPageTargetRefs, fieldName, section)
			}
			refs := fieldIndexChunkRefSlicePool.Get(int(refCount))

			for j := uint64(0); j < refCount; j++ {
				chunk, err := readFieldIndexChunk(reader)
				if err != nil {
					releaseOwnedFieldIndexChunkRefSlice(refs)
					builder.releaseOwned()
					return FieldStorage{}, fmt.Errorf("reading chunk page=%d/%d ref=%d/%d for field %q in %s: %w", i+1, pageCount, j+1, refCount, fieldName, section, err)
				}
				if chunk == nil || chunk.keyCount() == 0 {
					continue
				}

				last := chunk.keyCount() - 1
				if havePrev && keycodec.Compare(prevKey, chunk.keyAt(0)) >= 0 {
					chunk.release()
					releaseOwnedFieldIndexChunkRefSlice(refs)
					builder.releaseOwned()
					return FieldStorage{}, fmt.Errorf("chunk keys must be strictly increasing for field %q in %s at page %d/%d ref %d/%d", fieldName, section, i+1, pageCount, j+1, refCount)
				}
				refs = append(refs, fieldIndexChunkRef{
					last:  chunk.keyAt(last),
					chunk: chunk,
				})
				prevKey = chunk.keyAt(last)
				havePrev = true
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
		count, err := readUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading numeric chunk len: %w", err)
		}
		if count == 0 {
			return nil, nil
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("numeric chunk len overflows int: %v", count)
		}
		if count > fieldIndexChunkMaxEntries {
			return nil, fmt.Errorf("numeric chunk len %d exceeds limit %d", count, fieldIndexChunkMaxEntries)
		}
		n := int(count)
		var keys []uint64
		var keyOwners []uint64
		var posts []posting.List
		var rows uint64
		var prevKey uint64

		for i := 0; i < n; i++ {
			key, err := readBEUint64(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, keys, nil, nil)
				pooled.ReleaseUint64Slice(keyOwners)
				return nil, fmt.Errorf("reading numeric chunk key %d/%d: %w", i+1, n, err)
			}
			if i > 0 && key <= prevKey {
				releaseReadFieldChunkBuffers(posts, keys, nil, nil)
				pooled.ReleaseUint64Slice(keyOwners)
				return nil, fmt.Errorf("numeric chunk keys must be strictly increasing at entry %d/%d", i+1, n)
			}
			prevKey = key

			ids, err := posting.ReadFrom(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, keys, nil, nil)
				pooled.ReleaseUint64Slice(keyOwners)
				return nil, fmt.Errorf("reading numeric chunk posting %d/%d: %w", i+1, n, err)
			}

			if posts != nil {
				keys[i] = key
				posts[i] = ids
				rows += ids.Cardinality()
				continue
			}

			owner, ok := ids.TrySingle()
			if ok {
				if keyOwners == nil {
					keyOwners = fieldUint64Slice(n * 2)
				}
				base := i << 1
				keyOwners[base] = key
				keyOwners[base+1] = owner
				rows++
				continue
			}

			keys = fieldUint64Slice(n)
			posts = fieldPostingSlice(n)
			for j := 0; j < i; j++ {
				base := j << 1
				keys[j] = keyOwners[base]
				var single posting.List
				posts[j] = single.BuildAdded(keyOwners[base+1])
			}
			pooled.ReleaseUint64Slice(keyOwners)
			keyOwners = nil
			keys[i] = key
			posts[i] = ids
			rows += ids.Cardinality()
		}
		if posts == nil {
			return newUniqueNumericFieldIndexChunk(keyOwners), nil
		}
		return newNumericFieldIndexChunk(posts, keys, rows), nil

	case fieldIndexChunkEncodingString:
		count, err := readUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("reading string chunk len: %w", err)
		}
		if count == 0 {
			return nil, nil
		}
		if count > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("string chunk len overflows int: %v", count)
		}
		if count > fieldIndexChunkMaxEntries {
			return nil, fmt.Errorf("string chunk len %d exceeds limit %d", count, fieldIndexChunkMaxEntries)
		}

		n := int(count)
		refs := fieldStringRefSlice(n)
		var owners []uint64
		var posts []posting.List
		data := pooled.GetByteSlice(n * 8)
		var rows uint64
		prevOff := 0
		prevLen := 0

		for i := range refs {
			n, err := readUvarint(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				pooled.ReleaseUint64Slice(owners)
				return nil, fmt.Errorf("reading string chunk key len %d/%d: %w", i+1, len(refs), err)
			}

			if n > uint64(^uint(0)>>1) {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				pooled.ReleaseUint64Slice(owners)
				return nil, fmt.Errorf("string chunk key len overflows int at entry %d/%d: %v", i+1, len(refs), n)
			}

			keyLen := int(n)
			if keyLen > fieldIndexStringRefMax {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				pooled.ReleaseUint64Slice(owners)
				return nil, fmt.Errorf("string chunk key len exceeds uint16 at entry %d/%d: %d", i+1, len(refs), keyLen)
			}
			start := len(data)

			if cap(data)-len(data) < keyLen {
				next := pooled.GetByteSlice(len(data) + keyLen)
				next = append(next, data...)
				pooled.ReleaseByteSlice(data)
				data = next
			}

			data = data[:start+keyLen]
			if keyLen > 0 {
				if err = readFull(reader, data[start:start+keyLen]); err != nil {
					releaseReadFieldChunkBuffers(posts, nil, refs, data)
					pooled.ReleaseUint64Slice(owners)
					return nil, fmt.Errorf("reading string chunk key %d/%d: %w", i+1, len(refs), err)
				}
			}
			if i > 0 && bytes.Compare(data[prevOff:prevOff+prevLen], data[start:start+keyLen]) >= 0 {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				pooled.ReleaseUint64Slice(owners)
				return nil, fmt.Errorf("string chunk keys must be strictly increasing at entry %d/%d", i+1, len(refs))
			}

			if !fieldIndexStringRefFits(start, keyLen) {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				pooled.ReleaseUint64Slice(owners)
				return nil, fmt.Errorf("string chunk key offset exceeds uint16 at entry %d/%d: %d", i+1, len(refs), start)
			}

			refs[i] = newFieldIndexStringRef(start, keyLen)
			prevOff = start
			prevLen = keyLen
			ids, err := posting.ReadFrom(reader)
			if err != nil {
				releaseReadFieldChunkBuffers(posts, nil, refs, data)
				pooled.ReleaseUint64Slice(owners)
				return nil, fmt.Errorf("reading string chunk posting %d/%d: %w", i+1, len(refs), err)
			}

			if posts != nil {
				posts[i] = ids
				rows += ids.Cardinality()
				continue
			}

			owner, ok := ids.TrySingle()
			if ok {
				if owners == nil {
					owners = fieldUint64Slice(len(refs))
				}
				owners[i] = owner
				rows++
				continue
			}

			posts = fieldPostingSlice(len(refs))
			for j := 0; j < i; j++ {
				var single posting.List
				posts[j] = single.BuildAdded(owners[j])
			}
			pooled.ReleaseUint64Slice(owners)
			owners = nil
			posts[i] = ids
			rows += ids.Cardinality()
		}

		if posts == nil {
			return newUniqueStringFieldIndexChunk(owners, refs, data), nil
		}
		return newStringFieldIndexChunk(posts, refs, data, rows), nil

	default:
		return nil, fmt.Errorf("invalid field chunk encoding %v", tag)
	}
}

func releaseReadFieldChunkBuffers(posts []posting.List, keys []uint64, refs []fieldIndexStringRef, data []byte) {
	posting.ReleaseAll(posts)
	posting.ReleaseSlice(posts)
	pooled.ReleaseUint64Slice(keys)
	pooled.ReleaseUint32Slice(refs)
	pooled.ReleaseByteSlice(data)
}

func releaseReadFlatBuffers(entries []Entry, n int, data []byte) {
	for i := 0; i < n; i++ {
		entries[i].IDs.Release()
	}
	ReleaseFieldEntrySlice(entries)
	pooled.ReleaseByteSlice(data)
}

func skipFieldIndexChunk(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("reading field chunk encoding: %w", err)
	}
	switch tag {

	case fieldIndexChunkEncodingRaw8:
		count, err := readUvarint(reader)
		if err != nil {
			return fmt.Errorf("reading numeric chunk len: %w", err)
		}
		for i := uint64(0); i < count; i++ {
			if err = discardN(reader, 8); err != nil {
				return fmt.Errorf("skipping numeric chunk key %d/%d: %w", i+1, count, err)
			}
			if err = posting.Skip(reader); err != nil {
				return fmt.Errorf("skipping numeric chunk posting %d/%d: %w", i+1, count, err)
			}
		}
		return nil

	case fieldIndexChunkEncodingString:
		count, err := readUvarint(reader)
		if err != nil {
			return fmt.Errorf("reading string chunk len: %w", err)
		}
		for i := uint64(0); i < count; i++ {
			n, err := readUvarint(reader)
			if err != nil {
				return fmt.Errorf("reading string chunk key len %d/%d: %w", i+1, count, err)
			}
			if n > 0 {
				if err = discardN(reader, n); err != nil {
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

func readEntry(reader *bufio.Reader) (Entry, error) {
	key, err := readKey(reader)
	if err != nil {
		return Entry{}, fmt.Errorf("reading Entry key: %w", err)
	}
	ids, err := posting.ReadFrom(reader)
	if err != nil {
		return Entry{}, fmt.Errorf("reading Entry posting: %w", err)
	}
	return Entry{Key: key, IDs: ids}, nil
}

func skipEntry(reader *bufio.Reader) error {
	if err := skipKey(reader); err != nil {
		return fmt.Errorf("skipping Entry key: %w", err)
	}
	if err := posting.Skip(reader); err != nil {
		return fmt.Errorf("skipping Entry posting: %w", err)
	}
	return nil
}

func readKey(reader *bufio.Reader) (keycodec.IndexKey, error) {
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
		v, err := readBEUint64(reader)
		if err != nil {
			return keycodec.IndexKey{}, err
		}
		return keycodec.FromU64(v), nil

	default:
		return keycodec.IndexKey{}, fmt.Errorf("invalid Entry key encoding %v", tag)
	}
}

func skipKey(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch tag {
	case indexKeyEncodingString:
		return skipString(reader)
	case indexKeyEncodingRaw8:
		return discardN(reader, 8)
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
	if _, err := writer.WriteString(s); err != nil {
		return err
	}
	return nil
}

func writeUvarint(writer *bufio.Writer, v uint64) error {
	if v < 0x80 {
		return writer.WriteByte(byte(v))
	}
	if writer.Available() >= binary.MaxVarintLen64 {
		buf := writer.AvailableBuffer()
		for v >= 0x80 {
			buf = append(buf, byte(v)|0x80)
			v >>= 7
		}
		buf = append(buf, byte(v))
		_, err := writer.Write(buf)
		return err
	}
	for v >= 0x80 {
		if err := writer.WriteByte(byte(v) | 0x80); err != nil {
			return err
		}
		v >>= 7
	}
	return writer.WriteByte(byte(v))
}

func writeBEUint64(writer *bufio.Writer, v uint64) error {
	if writer.Available() >= 8 {
		buf := writer.AvailableBuffer()
		buf = binary.BigEndian.AppendUint64(buf, v)
		_, err := writer.Write(buf)
		return err
	}
	if err := writer.WriteByte(byte(v >> 56)); err != nil {
		return err
	}
	if err := writer.WriteByte(byte(v >> 48)); err != nil {
		return err
	}
	if err := writer.WriteByte(byte(v >> 40)); err != nil {
		return err
	}
	if err := writer.WriteByte(byte(v >> 32)); err != nil {
		return err
	}
	if err := writer.WriteByte(byte(v >> 24)); err != nil {
		return err
	}
	if err := writer.WriteByte(byte(v >> 16)); err != nil {
		return err
	}
	if err := writer.WriteByte(byte(v >> 8)); err != nil {
		return err
	}
	return writer.WriteByte(byte(v))
}

func readUvarint(reader *bufio.Reader) (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			if i > 0 && err == io.EOF {
				return x, io.ErrUnexpectedEOF
			}
			return x, err
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, fmt.Errorf("binary: varint overflows a 64-bit integer")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, fmt.Errorf("binary: varint overflows a 64-bit integer")
}

func readBEUint64(reader *bufio.Reader) (uint64, error) {
	if b, err := reader.Peek(8); err == nil {
		v := binary.BigEndian.Uint64(b)
		_, err = reader.Discard(8)
		return v, err
	}
	var v uint64
	for i := 0; i < 8; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			if i > 0 && err == io.EOF {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		v = v<<8 | uint64(b)
	}
	return v, nil
}

func readFull(reader *bufio.Reader, buf []byte) error {
	for len(buf) > 0 {
		n, err := reader.Read(buf)
		buf = buf[n:]
		if len(buf) == 0 {
			return nil
		}
		if err != nil {
			if err == io.EOF && n > 0 {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func discardN(reader *bufio.Reader, n uint64) error {
	for n > 0 {
		step := 64 << 10
		if n < uint64(step) {
			step = int(n)
		}
		skipped, err := reader.Discard(step)
		n -= uint64(skipped)
		if err != nil {
			return err
		}
		if skipped != step {
			return io.ErrNoProgress
		}
	}
	return nil
}

func readString(reader *bufio.Reader) (string, error) {
	n, err := readUvarint(reader)
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
	if b, err := reader.Peek(int(n)); err == nil {
		s := string(b)
		_, err = reader.Discard(int(n))
		return s, err
	}
	b := make([]byte, int(n))
	if err = readFull(reader, b); err != nil {
		return "", err
	}
	s := unsafe.String(unsafe.SliceData(b), len(b))
	return s, nil
}

func skipString(reader *bufio.Reader) error {
	n, err := readUvarint(reader)
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
	return discardN(reader, n)
}
