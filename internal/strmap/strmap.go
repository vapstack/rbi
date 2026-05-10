package strmap

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

const maxIntUint64 = uint64(^uint(0) >> 1)

type Mapper struct {
	next uint64
	keys map[string]uint64
	strs []string

	sparseStrs   map[uint64]string
	strsUsed     []bool
	snap         *Snapshot
	published    *Snapshot
	pubSource    *Snapshot
	committed    *Snapshot
	committedPub *Snapshot
	dirty        bool
	compactAt    int

	sync.Mutex
}

type Writer struct {
	m *Mapper
}

func New(size uint64, compactAt int) *Mapper {
	capHint := 1
	if size > 0 && size < maxIntUint64 {
		capHint = int(size) + 1
	}
	m := &Mapper{
		keys:      make(map[string]uint64, size),
		strs:      make([]string, 1, capHint),
		strsUsed:  make([]bool, 1, capHint),
		compactAt: compactAt,
	}
	empty := new(Snapshot)
	m.snap = empty
	m.published = empty
	m.pubSource = empty
	m.committed = empty
	m.committedPub = empty
	return m
}

func EmptySnapshot() *Snapshot {
	return &Snapshot{}
}

func (m *Mapper) Create(s string) (uint64, bool) {
	m.Lock()
	idx, created := m.createNoLock(s)
	m.Unlock()
	return idx, created
}

func (m *Mapper) LockWriter() Writer {
	m.Lock()
	return Writer{m: m}
}

func (m *Mapper) RollbackCreated(s string, idx uint64) {
	if idx == 0 {
		return
	}

	m.Lock()
	cur, ok := m.keys[s]
	if !ok || cur != idx {
		m.Unlock()
		return
	}

	delete(m.keys, s)
	if m.sparseStrs != nil {
		delete(m.sparseStrs, idx)
	} else if idx <= maxIntUint64 {
		i := int(idx)
		if i < len(m.strs) {
			m.strs[i] = ""
		}
		if i < len(m.strsUsed) {
			m.strsUsed[i] = false
		}
	}

	if idx <= m.next {
		for m.next > 0 {
			if m.sparseStrs != nil {
				if _, ok := m.sparseStrs[m.next]; ok {
					break
				}
				m.next--
				continue
			}
			if m.next > maxIntUint64 {
				m.next = 0
				break
			}
			i := int(m.next)
			if i < len(m.strsUsed) && m.strsUsed[i] {
				break
			}
			m.next--
		}

		if m.sparseStrs == nil {
			trim := int(m.next) + 1
			if trim < len(m.strs) {
				clear(m.strs[trim:])
				m.strs = m.strs[:trim]
			}
			if trim < len(m.strsUsed) {
				clear(m.strsUsed[trim:])
				m.strsUsed = m.strsUsed[:trim]
			}
		}
	}

	m.restoreCommittedNoLock()
	m.Unlock()
}

func (m *Mapper) Truncate() {
	m.Lock()
	defer m.Unlock()

	m.next = 0
	m.keys = make(map[string]uint64)
	m.strs = m.strs[:1]
	m.strs[0] = ""
	m.sparseStrs = nil
	m.strsUsed = m.strsUsed[:1]
	m.strsUsed[0] = false
	empty := new(Snapshot)
	m.snap = empty
	m.published = empty
	m.pubSource = empty
	m.committed = empty
	m.committedPub = empty
	m.dirty = false
}

func (m *Mapper) Snapshot() *Snapshot {
	m.Lock()
	defer m.Unlock()
	return m.snapshotNoLock()
}

func (m *Mapper) MarkCommittedPublished(published *Snapshot) {
	m.Lock()
	defer m.Unlock()
	m.markCommittedPublishedNoLock(published)
}

func (w Writer) Create(s string) (uint64, bool) {
	return w.m.createNoLock(s)
}

func (w Writer) Unlock() {
	w.m.Unlock()
}

func (m *Mapper) createNoLock(s string) (uint64, bool) {
	if v, ok := m.keys[s]; ok {
		return v, false
	}
	m.next++
	idx := m.next
	m.keys[s] = idx
	if m.sparseStrs != nil {
		m.sparseStrs[idx] = s
		m.dirty = true
		return idx, true
	}
	if idx > maxIntUint64 {
		panic(fmt.Errorf("strmap index overflows int: %v", idx))
	}
	need := int(idx) + 1
	if need > len(m.strs) {
		grow := need - len(m.strs)
		m.strs = append(m.strs, make([]string, grow)...)
		m.strsUsed = append(m.strsUsed, make([]bool, grow)...)
	}
	m.strs[int(idx)] = s
	m.strsUsed[int(idx)] = true
	m.dirty = true
	return idx, true
}

func (m *Mapper) replaceAllDenseNoLock(keys map[string]uint64, strs []string, used []bool, next uint64) {
	m.next = next
	m.keys = keys
	m.strs = strs
	m.sparseStrs = nil
	m.strsUsed = used
	m.snap = &Snapshot{
		next:      next,
		strs:      nil,
		denseStrs: slices.Clone(strs),
		denseUsed: slices.Clone(used),
		depth:     1,
	}
	m.published = &Snapshot{
		next:      next,
		denseStrs: m.snap.denseStrs,
		denseUsed: m.snap.denseUsed,
	}
	m.pubSource = m.snap
	m.committed = m.snap
	m.committedPub = m.published
	m.dirty = false
}

func (m *Mapper) replaceAllSparseNoLock(keys map[string]uint64, strs map[uint64]string, next uint64) {
	m.next = next
	m.keys = keys
	m.strs = nil
	m.sparseStrs = strs
	m.strsUsed = nil
	m.snap = &Snapshot{
		next:      next,
		strs:      maps.Clone(strs),
		denseStrs: nil,
		denseUsed: nil,
		depth:     1,
	}
	m.published = &Snapshot{
		next: next,
		strs: m.snap.strs,
	}
	m.pubSource = m.snap
	m.committed = m.snap
	m.committedPub = m.published
	m.dirty = false
}

func (m *Mapper) snapshotNoLock() *Snapshot {
	base := m.stateSnapshotNoLock()
	if m.published != nil && m.pubSource == base {
		return m.published
	}
	m.published = buildPublishedSnapshot(base, m.pubSource, m.published)
	m.pubSource = base
	return m.published
}

func (m *Mapper) stateSnapshotNoLock() *Snapshot {
	if m.snap == nil {
		m.snap = &Snapshot{}
	}
	if !m.dirty {
		return m.snap
	}
	if next, ok := m.deltaSnapshotNoLock(m.snap); ok {
		m.snap = next
		m.dirty = false
		return next
	}
	m.snap = m.fullSnapshotNoLock()
	m.dirty = false
	return m.snap
}

func (m *Mapper) fullSnapshotNoLock() *Snapshot {
	snap := &Snapshot{
		next:  m.next,
		depth: 1,
	}
	if m.sparseStrs != nil {
		snap.strs = maps.Clone(m.sparseStrs)
		return snap
	}
	snap.denseStrs = slices.Clone(m.strs)
	snap.denseUsed = slices.Clone(m.strsUsed)
	return snap
}

func (m *Mapper) deltaSnapshotNoLock(base *Snapshot) (*Snapshot, bool) {
	if base == nil {
		return nil, false
	}
	if m.compactAt > 0 && base.depth >= m.compactAt {
		return nil, false
	}
	if m.next < base.next {
		return nil, false
	}
	if m.next == base.next {
		return base, true
	}

	start := base.next + 1
	count := 0

	if m.sparseStrs != nil {
		for idx := start; idx <= m.next; idx++ {
			if _, ok := m.sparseStrs[idx]; ok {
				count++
			}
		}

	} else {
		if m.next > maxIntUint64 {
			return nil, false
		}
		end := int(m.next)
		for i := int(start); i <= end; i++ {
			if i < len(m.strsUsed) && m.strsUsed[i] {
				count++
			}
		}
	}

	if count == 0 {
		return base, true
	}

	strs := make(map[uint64]string, count)

	if m.sparseStrs != nil {
		for idx := start; idx <= m.next; idx++ {
			s, ok := m.sparseStrs[idx]
			if !ok {
				continue
			}
			strs[idx] = s
		}

	} else {
		end := int(m.next)
		for i := int(start); i <= end; i++ {
			if i >= len(m.strsUsed) || !m.strsUsed[i] {
				continue
			}
			s := m.strs[i]
			idx := uint64(i)
			strs[idx] = s
		}
	}

	anchor := base
	if anchor.depth > 1 && anchor.anchor != nil {
		anchor = anchor.anchor
	}

	return &Snapshot{
		next:   m.next,
		strs:   strs,
		base:   base,
		anchor: anchor,
		depth:  base.depth + 1,
	}, true
}

func (m *Mapper) restoreCommittedNoLock() {
	baseNext := uint64(0)
	if m.committed != nil {
		baseNext = m.committed.next
	}
	if m.committed == nil {
		base := &Snapshot{}
		m.snap = base
		m.published = base
		m.pubSource = base
		m.committed = base
		m.committedPub = base
		m.dirty = m.next != 0
		return
	}
	m.snap = m.committed
	m.published = m.committedPub
	m.pubSource = m.committed
	m.dirty = m.next != baseNext
}

func (m *Mapper) markCommittedPublishedNoLock(published *Snapshot) {
	if m.published == published && m.pubSource != nil {
		m.committed = m.pubSource
		m.committedPub = published
		return
	}
	if m.snap != nil && !m.dirty {
		m.committed = m.snap
		if published != nil {
			m.committedPub = published
			return
		}
		m.committedPub = buildPublishedSnapshot(m.snap, nil, nil)
	}
}
