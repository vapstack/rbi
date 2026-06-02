package strmap

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
)

const (
	encodingDense  byte = 1
	encodingSparse byte = 2
)

func WriteSnapshot(writer *bufio.Writer, sm *Snapshot) error {
	if sm == nil {
		if err := writeUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing strmap next: %w", err)
		}
		if err := writer.WriteByte(encodingSparse); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		return writeUvarint(writer, 0)
	}

	if err := writeUvarint(writer, sm.next); err != nil {
		return fmt.Errorf("encode: writing strmap next: %w", err)
	}

	if len(sm.readDirs) == 0 && sm.base == nil {
		if len(sm.denseStrs) > 0 || len(sm.denseUsed) > 0 {

			usedCount := snapshotOwnUsedCount(sm)
			limit := min(len(sm.denseStrs), len(sm.denseUsed))

			if snapshotShouldPersistSparse(sm, usedCount) {
				if err := writer.WriteByte(encodingSparse); err != nil {
					return fmt.Errorf("encode: writing strmap encoding: %w", err)
				}
				if err := writeUvarint(writer, uint64(usedCount)); err != nil {
					return fmt.Errorf("encode: writing strmap sparse len: %w", err)
				}
				for i := 1; i < limit; i++ {
					if !sm.denseUsed[i] {
						continue
					}
					if err := writeUvarint(writer, uint64(i)); err != nil {
						return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
					}
					if err := writeString(writer, sm.denseStrs[i]); err != nil {
						return fmt.Errorf("encode: writing strmap sparse string: %w", err)
					}
				}
				return nil
			}

			if err := writer.WriteByte(encodingDense); err != nil {
				return fmt.Errorf("encode: writing strmap encoding: %w", err)
			}

			denseLen := int(sm.next) + 1
			if err := writeUvarint(writer, uint64(denseLen)); err != nil {
				return fmt.Errorf("encode: writing strmap dense len: %w", err)
			}
			if limit > denseLen {
				limit = denseLen
			}
			for base := 0; base < denseLen; base += 8 {
				start := base
				if start == 0 {
					start = 1
				}
				end := min(base+8, limit)
				var flags byte
				for i := start; i < end; i++ {
					if sm.denseUsed[i] {
						flags |= 1 << uint(i-base)
					}
				}
				if err := writer.WriteByte(flags); err != nil {
					return fmt.Errorf("encode: writing strmap flags: %w", err)
				}
			}

			for i := 1; i < limit; i++ {
				if !sm.denseUsed[i] {
					continue
				}
				if err := writeString(writer, sm.denseStrs[i]); err != nil {
					return fmt.Errorf("encode: writing strmap string: %w", err)
				}
			}

			return nil
		}

		if sm.strs != nil {

			usedCount := len(sm.strs)

			if snapshotShouldPersistSparse(sm, usedCount) {
				if err := writer.WriteByte(encodingSparse); err != nil {
					return fmt.Errorf("encode: writing strmap encoding: %w", err)
				}
				if err := writeUvarint(writer, uint64(usedCount)); err != nil {
					return fmt.Errorf("encode: writing strmap sparse len: %w", err)
				}
				if len(sm.strs) <= 64 {
					var idxs [64]uint64
					count := 0
					for idx := range sm.strs {
						i := count
						count++
						for i > 0 && idxs[i-1] > idx {
							idxs[i] = idxs[i-1]
							i--
						}
						idxs[i] = idx
					}
					for _, idx := range idxs[:count] {
						if err := writeUvarint(writer, idx); err != nil {
							return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
						}
						if err := writeString(writer, sm.strs[idx]); err != nil {
							return fmt.Errorf("encode: writing strmap sparse string: %w", err)
						}
					}
					return nil
				}

				idxs := pooled.GetUint64Slice(len(sm.strs))
				for idx := range sm.strs {
					idxs = append(idxs, idx)
				}
				slices.Sort(idxs)

				for _, idx := range idxs {
					if err := writeUvarint(writer, idx); err != nil {
						pooled.ReleaseUint64Slice(idxs)
						return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
					}
					if err := writeString(writer, sm.strs[idx]); err != nil {
						pooled.ReleaseUint64Slice(idxs)
						return fmt.Errorf("encode: writing strmap sparse string: %w", err)
					}
				}

				pooled.ReleaseUint64Slice(idxs)

				return nil
			}

			if err := writer.WriteByte(encodingDense); err != nil {
				return fmt.Errorf("encode: writing strmap encoding: %w", err)
			}

			denseLen := int(sm.next) + 1
			if err := writeUvarint(writer, uint64(denseLen)); err != nil {
				return fmt.Errorf("encode: writing strmap dense len: %w", err)
			}

			for base := 0; base < denseLen; base += 8 {
				start := base
				if start == 0 {
					start = 1
				}
				end := min(base+8, denseLen)
				var flags byte
				for i := start; i < end; i++ {
					if _, ok := sm.strs[uint64(i)]; ok {
						flags |= 1 << uint(i-base)
					}
				}
				if err := writer.WriteByte(flags); err != nil {
					return fmt.Errorf("encode: writing strmap flags: %w", err)
				}
			}

			for i := 1; i < denseLen; i++ {
				value, ok := sm.strs[uint64(i)]
				if !ok {
					continue
				}
				if err := writeString(writer, value); err != nil {
					return fmt.Errorf("encode: writing strmap string: %w", err)
				}
			}

			return nil
		}
	}

	if len(sm.readDirs) > 0 {

		usedCount := snapshotUsedCountNoLock(sm)

		if snapshotShouldPersistSparse(sm, usedCount) {
			return writeSparseReadDirsSnapshot(writer, sm, usedCount)
		}

		if err := writer.WriteByte(encodingDense); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		denseLen := int(sm.next) + 1
		if err := writeUvarint(writer, uint64(denseLen)); err != nil {
			return fmt.Errorf("encode: writing strmap dense len: %w", err)
		}

		pageIdx := -1
		var page *readPage
		for base := 0; base < denseLen; base += 8 {
			start := base
			if start == 0 {
				start = 1
			}
			end := min(base+8, denseLen)
			var flags byte
			for i := start; i < end; i++ {
				curPage := readPageIndex(uint64(i))
				if curPage != pageIdx {
					pageIdx = curPage
					page = sm.readPageAtNoLock(curPage)
				}
				if page == nil {
					continue
				}
				if _, ok := page.getStringNoLock(uint64(i)); ok {
					flags |= 1 << uint(i-base)
				}
			}
			if err := writer.WriteByte(flags); err != nil {
				return fmt.Errorf("encode: writing strmap flags: %w", err)
			}
		}

		pageIdx = -1
		page = nil
		for i := 1; i < denseLen; i++ {
			curPage := readPageIndex(uint64(i))
			if curPage != pageIdx {
				pageIdx = curPage
				page = sm.readPageAtNoLock(curPage)
			}
			if page == nil {
				continue
			}
			value, ok := page.getStringNoLock(uint64(i))
			if !ok {
				continue
			}
			if err := writeString(writer, value); err != nil {
				return fmt.Errorf("encode: writing strmap string: %w", err)
			}
		}

		return nil
	}

	var chainInline [32]snapshotPersistNode
	chain, usedCount := snapshotPersistNodes(sm, chainInline[:0])

	if snapshotShouldPersistSparse(sm, usedCount) {
		if err := writer.WriteByte(encodingSparse); err != nil {
			return fmt.Errorf("encode: writing strmap encoding: %w", err)
		}
		if err := writeUvarint(writer, uint64(usedCount)); err != nil {
			return fmt.Errorf("encode: writing strmap sparse len: %w", err)
		}

		it := snapshotPersistIter{chain: chain}
		for {
			idx, value, ok := it.next()
			if !ok {
				break
			}
			if err := writeUvarint(writer, idx); err != nil {
				it.release()
				return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
			}
			if err := writeString(writer, value); err != nil {
				it.release()
				return fmt.Errorf("encode: writing strmap sparse string: %w", err)
			}
		}
		return nil
	}

	if err := writer.WriteByte(encodingDense); err != nil {
		return fmt.Errorf("encode: writing strmap encoding: %w", err)
	}

	denseLen := int(sm.next) + 1
	if err := writeUvarint(writer, uint64(denseLen)); err != nil {
		return fmt.Errorf("encode: writing strmap dense len: %w", err)
	}

	flagIter := snapshotPersistIter{chain: chain}
	idx, _, ok := flagIter.next()
	for base := 0; base < denseLen; base += 8 {
		baseIdx := uint64(base)
		limit := baseIdx + 8
		var flags byte
		for ok && idx < limit {
			flags |= 1 << uint(idx-baseIdx)
			idx, _, ok = flagIter.next()
		}
		if err := writer.WriteByte(flags); err != nil {
			flagIter.release()
			return fmt.Errorf("encode: writing strmap flags: %w", err)
		}
	}

	valueIter := snapshotPersistIter{chain: chain}
	for {
		_, value, ok := valueIter.next()
		if !ok {
			break
		}
		if err := writeString(writer, value); err != nil {
			valueIter.release()
			return fmt.Errorf("encode: writing strmap string: %w", err)
		}
	}
	return nil
}

type snapshotPersistNode struct {
	start     uint64
	next      uint64
	strs      map[uint64]string
	singleIdx uint64
	singleStr string
	denseStrs []string
	denseUsed []bool
}

func snapshotPersistNodes(sm *Snapshot, inline []snapshotPersistNode) ([]snapshotPersistNode, int) {
	if len(sm.readDirs) > 0 {
		nodes := inline
		usedCount := 0
		pageCount := readPageCount(sm.next)
		for page := 0; page < pageCount; page++ {
			rp := sm.readPageAtNoLock(page)
			if rp == nil {
				continue
			}
			nodes = append(nodes, snapshotPersistNode{
				start:     rp.start,
				next:      rp.next,
				strs:      rp.strs,
				singleIdx: rp.singleIdx,
				singleStr: rp.singleStr,
				denseStrs: rp.denseStrs,
				denseUsed: rp.denseUsed,
			})
			usedCount += rp.usedCountNoLock()
		}
		if len(nodes) == 0 {
			return nil, usedCount
		}
		return nodes, usedCount
	}

	depth := 0
	usedCount := 0
	for cur := sm; cur != nil; cur = cur.base {
		depth++
		usedCount += snapshotOwnUsedCount(cur)
	}
	if depth == 0 {
		return nil, 0
	}

	var nodes []snapshotPersistNode
	if depth <= cap(inline) {
		nodes = inline[:depth]
	} else {
		nodes = make([]snapshotPersistNode, depth)
	}
	i := depth
	for cur := sm; cur != nil; cur = cur.base {
		i--
		start := cur.baseNextNoLock() + 1
		if cur.base == nil {
			start = 1
		}
		denseStrs, denseUsed := denseWindowNoLock(cur.denseStart, cur.denseStrs, cur.denseUsed, start, cur.next)
		nodes[i] = snapshotPersistNode{
			start:     start,
			next:      cur.next,
			strs:      cur.strs,
			singleIdx: cur.singleIdx,
			singleStr: cur.singleStr,
			denseStrs: denseStrs,
			denseUsed: denseUsed,
		}
	}
	return nodes, usedCount
}

type snapshotPersistIter struct {
	chain        []snapshotPersistNode
	node         int
	mode         byte
	densePos     int
	denseLimit   int
	sparse       []sparseEntry
	sparsePos    int
	sparsePooled bool
}

func (it *snapshotPersistIter) next() (uint64, string, bool) {
	for it.node < len(it.chain) {
		switch it.mode {

		case 1:
			node := it.chain[it.node]
			for it.densePos < it.denseLimit {
				idx := uint64(it.densePos)
				it.densePos++
				pos := int(idx - node.start)
				if pos < 0 || pos >= len(node.denseStrs) || len(node.denseUsed) != 0 && (pos >= len(node.denseUsed) || !node.denseUsed[pos]) {
					continue
				}
				return idx, node.denseStrs[pos], true
			}
			it.mode = 0
			it.node++
			continue

		case 2:
			if it.sparsePos < len(it.sparse) {
				entry := it.sparse[it.sparsePos]
				it.sparsePos++
				return entry.idx, entry.value, true
			}
			if it.sparsePooled {
				sparseEntryPool.Put(it.sparse)
				it.sparsePooled = false
			}
			it.sparse = nil
			it.sparsePos = 0
			it.mode = 0
			it.node++
			continue
		}

		node := it.chain[it.node]

		if node.singleIdx != 0 {
			it.node++
			if node.singleIdx >= node.start && node.singleIdx <= node.next {
				return node.singleIdx, node.singleStr, true
			}
			continue
		}

		if len(node.denseStrs) > 0 || len(node.denseUsed) > 0 {
			start := int(node.start)
			limitLen := len(node.denseStrs)
			if len(node.denseUsed) != 0 {
				limitLen = min(limitLen, len(node.denseUsed))
			}
			limit := start + limitLen
			if start >= limit {
				it.node++
				continue
			}
			it.densePos = start
			it.denseLimit = limit
			it.mode = 1
			continue
		}

		if len(node.strs) == 0 {
			it.node++
			continue
		}

		it.sparse = sparseEntryPool.Get(len(node.strs))
		it.sparsePooled = true
		for idx, value := range node.strs {
			if idx < node.start || idx > node.next {
				continue
			}
			it.sparse = append(it.sparse, sparseEntry{idx: idx, value: value})
		}

		slices.SortFunc(it.sparse, func(a, b sparseEntry) int {
			switch {
			case a.idx < b.idx:
				return -1
			case a.idx > b.idx:
				return 1
			default:
				return 0
			}
		})
		it.mode = 2
	}

	return 0, "", false
}

func (it *snapshotPersistIter) release() {
	if it.sparsePooled {
		sparseEntryPool.Put(it.sparse)
		it.sparsePooled = false
		it.sparse = nil
	}
}

func writeSparseReadDirsSnapshot(writer *bufio.Writer, sm *Snapshot, usedCount int) error {
	if err := writer.WriteByte(encodingSparse); err != nil {
		return fmt.Errorf("encode: writing strmap encoding: %w", err)
	}
	if err := writeUvarint(writer, uint64(usedCount)); err != nil {
		return fmt.Errorf("encode: writing strmap sparse len: %w", err)
	}

	pageCount := readPageCount(sm.next)
	for pageIdx := 0; pageIdx < pageCount; pageIdx++ {
		page := sm.readPageAtNoLock(pageIdx)
		if page == nil {
			continue
		}

		if page.singleIdx != 0 {
			if err := writeUvarint(writer, page.singleIdx); err != nil {
				return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
			}
			if err := writeString(writer, page.singleStr); err != nil {
				return fmt.Errorf("encode: writing strmap sparse string: %w", err)
			}
			continue
		}

		if len(page.denseStrs) > 0 || len(page.denseUsed) > 0 {
			limit := min(len(page.denseStrs), len(page.denseUsed))
			if len(page.denseUsed) == 0 {
				limit = len(page.denseStrs)
			}
			for i := 0; i < limit; i++ {
				if len(page.denseUsed) != 0 && !page.denseUsed[i] {
					continue
				}
				if err := writeUvarint(writer, page.start+uint64(i)); err != nil {
					return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
				}
				if err := writeString(writer, page.denseStrs[i]); err != nil {
					return fmt.Errorf("encode: writing strmap sparse string: %w", err)
				}
			}
			continue
		}

		if len(page.strs) == 0 {
			continue
		}

		if len(page.strs) <= 64 {
			var idxs [64]uint64
			count := 0

			for idx := range page.strs {
				if idx < page.start || idx > page.next {
					continue
				}
				i := count
				count++
				for i > 0 && idxs[i-1] > idx {
					idxs[i] = idxs[i-1]
					i--
				}
				idxs[i] = idx
			}

			for _, idx := range idxs[:count] {
				if err := writeUvarint(writer, idx); err != nil {
					return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
				}
				if err := writeString(writer, page.strs[idx]); err != nil {
					return fmt.Errorf("encode: writing strmap sparse string: %w", err)
				}
			}

			continue
		}

		idxs := pooled.GetUint64Slice(len(page.strs))
		for idx := range page.strs {
			if idx >= page.start && idx <= page.next {
				idxs = append(idxs, idx)
			}
		}
		slices.Sort(idxs)

		for _, idx := range idxs {
			if err := writeUvarint(writer, idx); err != nil {
				pooled.ReleaseUint64Slice(idxs)
				return fmt.Errorf("encode: writing strmap sparse idx: %w", err)
			}
			if err := writeString(writer, page.strs[idx]); err != nil {
				pooled.ReleaseUint64Slice(idxs)
				return fmt.Errorf("encode: writing strmap sparse string: %w", err)
			}
		}

		pooled.ReleaseUint64Slice(idxs)

	}
	return nil
}

func Read(reader *bufio.Reader, compactAt int) (*Mapper, error) {
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap next: %w", err)
	}

	enc, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("decode: reading strmap encoding: %w", err)
	}
	switch enc {

	case encodingDense:
		denseLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap dense len: %w", err)
		}
		if denseLen == 0 {
			return nil, fmt.Errorf("decode: invalid zero strmap dense len")
		}
		if denseLen > maxIntUint64 {
			return nil, fmt.Errorf("decode: strmap dense len overflows int: %v", denseLen)
		}
		if next >= denseLen {
			return nil, fmt.Errorf("decode: strmap next out of range: next=%v denseLen=%v", next, denseLen)
		}

		flagLen := (int(denseLen) + 7) >> 3
		var flagsInline [128]byte
		flags := flagsInline[:]
		pooledFlags := false
		if flagLen > len(flagsInline) {
			flags = pooled.GetByteSlice(flagLen)[:flagLen]
			pooledFlags = true
		} else {
			flags = flags[:flagLen]
		}

		if _, err = io.ReadFull(reader, flags); err != nil {
			if pooledFlags {
				pooled.ReleaseByteSlice(flags)
			}
			return nil, fmt.Errorf("decode: reading strmap flags: %w", err)
		}

		usedCount := 0
		for _, b := range flags {
			for x := b; x != 0; x &= x - 1 {
				usedCount++
			}
		}

		strs := make([]string, int(denseLen))
		used := make([]bool, int(denseLen))
		keys := make(map[string]uint64, max(0, usedCount-1))
		arenaCap := usedCount
		if arenaCap > indexdata.MaxStoredStringLen/16 {
			arenaCap = indexdata.MaxStoredStringLen
		} else {
			arenaCap *= 16
		}
		stringArena := make([]byte, 0, arenaCap)

		for i := 0; i < int(denseLen); i++ {
			if flags[i>>3]&(1<<uint(i&7)) == 0 {
				continue
			}
			s, err := readString(reader, &stringArena)
			if err != nil {
				if pooledFlags {
					pooled.ReleaseByteSlice(flags)
				}
				return nil, fmt.Errorf("decode: reading strmap dense string idx=%d: %w", i, err)
			}
			strs[i] = s
			used[i] = true
			if i > 0 {
				keys[s] = uint64(i)
			}
		}
		if pooledFlags {
			pooled.ReleaseByteSlice(flags)
		}

		m := &Mapper{
			compactAt: compactAt,
		}
		m.replaceAllDenseNoLock(keys, strs, used, next)
		return m, nil

	case encodingSparse:
		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading strmap sparse len: %w", err)
		}
		if count > maxIntUint64 {
			return nil, fmt.Errorf("decode: strmap sparse len overflows int: %v", count)
		}
		keys := make(map[string]uint64, int(count))
		strs := make(map[uint64]string, int(count))

		arenaCap := int(count)
		if arenaCap > indexdata.MaxStoredStringLen/16 {
			arenaCap = indexdata.MaxStoredStringLen
		} else {
			arenaCap *= 16
		}
		stringArena := make([]byte, 0, arenaCap)

		for i := uint64(0); i < count; i++ {
			idx, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap sparse idx entry=%d/%d: %w", i+1, count, err)
			}
			if idx == 0 || idx > next {
				return nil, fmt.Errorf("decode: strmap sparse idx out of range at entry=%d/%d: idx=%v next=%v", i+1, count, idx, next)
			}
			if _, exists := strs[idx]; exists {
				return nil, fmt.Errorf("decode: duplicate strmap sparse idx at entry=%d/%d: %v", i+1, count, idx)
			}
			s, err := readString(reader, &stringArena)
			if err != nil {
				return nil, fmt.Errorf("decode: reading strmap sparse string idx=%d entry=%d/%d: %w", idx, i+1, count, err)
			}
			if _, exists := keys[s]; exists {
				return nil, fmt.Errorf("decode: duplicate strmap sparse key at entry=%d/%d: %q", i+1, count, s)
			}
			keys[s] = idx
			strs[idx] = s
		}
		m := &Mapper{
			compactAt: compactAt,
		}
		m.replaceAllSparseNoLock(keys, strs, next)

		return m, nil

	default:
		return nil, fmt.Errorf("decode: invalid strmap encoding %v", enc)
	}
}

type sparseEntry struct {
	idx   uint64
	value string
}

var sparseEntryPool = pooled.Slices[sparseEntry]{MaxCap: 4096, Clear: pooled.ClearCap}

func snapshotShouldPersistSparse(sm *Snapshot, usedCount int) bool {
	if sm.next > maxIntUint64 {
		return true
	}
	denseLen := max(len(sm.denseStrs), len(sm.denseUsed))
	if denseLen <= 1 {
		denseLen = int(sm.next) + 1
	}
	if denseLen <= 1 {
		return false
	}
	if usedCount == 0 {
		return false
	}
	return estimateSparseReverseBytes(usedCount) < estimateDenseReverseBytes(denseLen)
}

func estimateDenseReverseBytes(denseLen int) uint64 {
	if denseLen <= 0 {
		return 0
	}
	return uint64(denseLen) * uint64(unsafe.Sizeof("")+unsafe.Sizeof(false))
}

func estimateSparseReverseBytes(usedCount int) uint64 {
	if usedCount <= 0 {
		return 0
	}
	const sparseMapLoadNumerator = 2
	const sparseMapLoadDenominator = 13 // ceil(n / 6.5) == ceil(2n / 13)
	buckets := (usedCount*sparseMapLoadNumerator + sparseMapLoadDenominator - 1) / sparseMapLoadDenominator
	if buckets < 1 {
		buckets = 1
	}
	bucketSize := uint64(8 + 8*unsafe.Sizeof(uint64(0)) + 8*unsafe.Sizeof("") + unsafe.Sizeof(uintptr(0)))
	return bucketSize * uint64(buckets)
}

func writeUvarint(writer *bufio.Writer, v uint64) error {
	for v >= 0x80 {
		if err := writer.WriteByte(byte(v&0x7f) | 0x80); err != nil {
			return err
		}
		v >>= 7
	}
	return writer.WriteByte(byte(v))
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

func readString(reader *bufio.Reader, arena *[]byte) (string, error) {
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
	if n > maxIntUint64 {
		return "", fmt.Errorf("string len %v overflows int", n)
	}

	l := int(n)
	buf := *arena
	if l > cap(buf)-len(buf) {
		capHint := l
		if c := cap(buf) * 2; c > capHint {
			capHint = c
		}
		if capHint < 256 {
			capHint = 256
		}
		if capHint > indexdata.MaxStoredStringLen {
			capHint = indexdata.MaxStoredStringLen
		}
		buf = make([]byte, l, capHint)
		*arena = buf

		if _, err = io.ReadFull(reader, buf); err != nil {
			return "", err
		}

		return unsafe.String(unsafe.SliceData(buf), l), nil
	}

	start := len(buf)
	buf = buf[:start+l]
	*arena = buf
	b := buf[start:]

	if _, err = io.ReadFull(reader, b); err != nil {
		return "", err
	}
	return unsafe.String(unsafe.SliceData(b), l), nil
}
