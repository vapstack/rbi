package rbi

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/snapshot"
)

const rootGenerationEntryMaxRetainedCap = 4096
const rootRuntimeEpochSlotCount = 4096

type rootGeneration struct {
	epoch   uint64
	entries []rootCollectionEntry
}

type rootCollectionEntry struct {
	collection *collection
	read       *collectionReadState
}

type collectionReadState struct {
	collection  *collection
	dataSeq     uint64
	snap        *snapshot.View
	refs        atomic.Int64
	runtimePins atomic.Int64
}

type rootRegistry struct {
	current    atomic.Pointer[rootGeneration]
	currentRef atomic.Pointer[generationRef]

	byEpoch map[uint64]*generationRef
	staged  map[uint64]*rootGeneration

	mu                 sync.RWMutex
	cleanupState       atomic.Uint32
	runtimeCachesDirty atomic.Bool

	releaseMu    sync.Mutex
	releaseRoots []*rootGeneration

	runtimeEpoch rootRuntimeEpochState
}

type generationRef struct {
	gen            *rootGeneration
	retired        []*rootGeneration
	refs           atomic.Int64
	cleanupPending atomic.Bool
}

type generationPin struct {
	ref       *generationRef
	readEpoch uint64
}

// Pins are issued under rootRegistry.mu read lock, while safeEpoch is read
// under the write lock. That keeps issued-but-not-yet-published spill pins
// outside the reader-visible set without putting every pin on the spill mutex.
type rootRuntimeEpochState struct {
	issued atomic.Uint64
	slots  [rootRuntimeEpochSlotCount]rootRuntimeEpochSlot

	mu    sync.Mutex
	spill map[uint64]int64
}

type rootRuntimeEpochSlot struct {
	epoch atomic.Uint64
}

const (
	rootRegistryCleanupIdle uint32 = iota
	rootRegistryCleanupRunning
	rootRegistryCleanupAgain
)

var (
	rootGenerationPool pooled.Pointers[rootGeneration]
	rootRefPool        pooled.Pointers[generationRef]
)

func (s *rootRuntimeEpochState) pin() uint64 {
	epoch := s.issued.Add(1)

	if s.slots[epoch&(rootRuntimeEpochSlotCount-1)].epoch.CompareAndSwap(0, epoch) {
		return epoch
	}

	s.mu.Lock()
	if s.spill == nil {
		s.spill = make(map[uint64]int64, 8)
	}
	s.spill[epoch]++
	s.mu.Unlock()

	return epoch
}

func (s *rootRuntimeEpochState) unpin(epoch uint64) {
	if s.slots[epoch&(rootRuntimeEpochSlotCount-1)].epoch.CompareAndSwap(epoch, 0) {
		return
	}
	s.mu.Lock()
	refs := s.spill[epoch] - 1
	if refs == 0 {
		delete(s.spill, epoch)
	} else {
		s.spill[epoch] = refs
	}
	s.mu.Unlock()
}

func (s *rootRuntimeEpochState) safeEpoch() uint64 {
	safe := s.issued.Load() + 1
	for i := range s.slots {
		epoch := s.slots[i].epoch.Load()
		if epoch != 0 && epoch < safe {
			safe = epoch
		}
	}
	s.mu.Lock()
	for epoch, refs := range s.spill {
		if refs != 0 && epoch < safe {
			safe = epoch
		}
	}
	s.mu.Unlock()
	return safe
}

func (rr *rootRegistry) markRuntimeCachesDirty() {
	rr.runtimeCachesDirty.CompareAndSwap(false, true)
}

func getRootRef(gen *rootGeneration) *generationRef {
	ref := rootRefPool.Get()
	ref.gen = gen
	ref.retired = nil
	ref.refs.Store(0)
	ref.cleanupPending.Store(false)
	return ref
}

func releaseRootRef(ref *generationRef) {
	ref.gen = nil
	ref.retired = nil
	ref.refs.Store(0)
	ref.cleanupPending.Store(false)
	rootRefPool.Put(ref)
}

func newCollectionReadState(c *collection, dataSeq uint64, snap *snapshot.View) *collectionReadState {
	read := &collectionReadState{
		collection: c,
		dataSeq:    dataSeq,
		snap:       snap,
	}
	read.refs.Store(1)
	return read
}

func (read *collectionReadState) retain() {
	read.refs.Add(1)
}

func (read *collectionReadState) release() {
	if read.refs.Add(-1) != 0 {
		return
	}
	if read.snap != nil {
		read.snap.Release()
	}
}

func (read *collectionReadState) retainRuntimePin(rr *rootRegistry) uint64 {
	read.retain()
	read.runtimePins.Add(1)
	return rr.runtimeEpoch.pin()
}

func (read *collectionReadState) releaseRuntimePin(readEpoch uint64) {
	rr := &read.collection.root.registry
	// Only the last runtime pin takes rr.mu: safeEpoch is read under that lock,
	// so non-last unpins avoid serializing the read/cache hot path.
	for {
		pins := read.runtimePins.Load()
		if pins > 1 {
			if read.runtimePins.CompareAndSwap(pins, pins-1) {
				rr.runtimeEpoch.unpin(readEpoch)
				rr.scheduleRuntimeCleanup()
				read.release()
				return
			}
			continue
		}
		break
	}

	rr.mu.Lock()
	read.runtimePins.Add(-1)
	rr.runtimeEpoch.unpin(readEpoch)
	rr.mu.Unlock()

	rr.scheduleRuntimeCleanup()
	read.release()
}

type collectionReadStatePin struct {
	readState *collectionReadState
	readEpoch uint64
}

func (pin collectionReadStatePin) Unpin() {
	pin.readState.releaseRuntimePin(pin.readEpoch)
}

func (c *collection) currentSnapshotSeq() uint64 {
	rr := &c.root.registry
	rr.mu.RLock()
	ref := rr.currentRef.Load()
	if ref == nil || ref.gen == nil {
		rr.mu.RUnlock()
		return 0
	}
	ordinal := int(c.ordinal)
	if ordinal >= len(ref.gen.entries) {
		rr.mu.RUnlock()
		return 0
	}
	read := ref.gen.entries[ordinal].read
	if read == nil || read.snap == nil {
		rr.mu.RUnlock()
		return 0
	}
	seq := read.snap.Seq
	rr.mu.RUnlock()
	return seq
}

func (c *collection) pinCurrentReadStateBySnapshotSeq(seq uint64) (*snapshot.View, collectionReadStatePin, bool) {
	rr := &c.root.registry
	rr.mu.RLock()
	ref := rr.currentRef.Load()
	if ref == nil || ref.gen == nil {
		rr.mu.RUnlock()
		return nil, collectionReadStatePin{}, false
	}
	ordinal := int(c.ordinal)
	if ordinal >= len(ref.gen.entries) {
		rr.mu.RUnlock()
		return nil, collectionReadStatePin{}, false
	}
	read := ref.gen.entries[ordinal].read
	if read == nil || read.snap == nil || read.snap.Seq != seq {
		rr.mu.RUnlock()
		return nil, collectionReadStatePin{}, false
	}
	readEpoch := read.retainRuntimePin(rr)
	snap := read.snap
	rr.mu.RUnlock()
	return snap, collectionReadStatePin{readState: read, readEpoch: readEpoch}, true
}

func getRootGeneration(epoch uint64, entryCount int) *rootGeneration {
	gen := rootGenerationPool.Get()
	gen.epoch = epoch
	if cap(gen.entries) < entryCount {
		gen.entries = make([]rootCollectionEntry, entryCount)
	} else {
		gen.entries = gen.entries[:entryCount]
	}
	return gen
}

func releaseRootGeneration(gen *rootGeneration) {
	for i := range gen.entries {
		if gen.entries[i].read != nil {
			gen.entries[i].read.release()
		}
	}
	if cap(gen.entries) > rootGenerationEntryMaxRetainedCap {
		gen.entries = nil
	} else {
		clear(gen.entries)
		gen.entries = gen.entries[:0]
	}
	gen.epoch = 0
	rootGenerationPool.Put(gen)
}

func appendRetiredRootGeneration(buf []*rootGeneration, gen *rootGeneration) []*rootGeneration {
	if buf == nil {
		buf = rootGenerationRetiredListPool.Get(1)
	}
	return append(buf, gen)
}

func releaseRetiredRootGenerations(buf []*rootGeneration) {
	if buf == nil {
		return
	}
	for _, v := range buf {
		releaseRootGeneration(v)
	}
	rootGenerationRetiredListPool.Put(buf)
}

var (
	rootGenerationRetiredListPool = pooled.Slices[*rootGeneration]{
		MaxCap: 64,
		Clear:  pooled.ClearCap,
	}
	runtimeCacheReadStatePool = pooled.Slices[*collectionReadState]{
		MaxCap: rootGenerationEntryMaxRetainedCap,
		Clear:  pooled.ClearCap,
	}
	runtimeCachesRetiredListPool = pooled.Slices[snapshot.RuntimeCachesRetired]{
		MaxCap: 64,
		Clear:  pooled.ClearCap,
	}
)

func releaseRuntimeCachesRetiredList(buf []snapshot.RuntimeCachesRetired) {
	if buf == nil {
		return
	}
	for i := range buf {
		buf[i].Release()
	}
	runtimeCachesRetiredListPool.Put(buf)
}

func (rr *rootRegistry) enqueueRetiredRootGenerations(buf []*rootGeneration) {
	if buf == nil {
		return
	}
	rr.releaseMu.Lock()
	if rr.releaseRoots == nil {
		rr.releaseRoots = rootGenerationRetiredListPool.Get(len(buf))
	}
	rr.releaseRoots = append(rr.releaseRoots, buf...)
	rr.releaseMu.Unlock()

	rootGenerationRetiredListPool.Put(buf)
	rr.scheduleCleanup()
}

func (rr *rootRegistry) scheduleRuntimeCleanup() {
	if rr.runtimeCachesDirty.Load() || rr.cleanupState.Load() == rootRegistryCleanupRunning {
		rr.scheduleCleanup()
	}
}

func (rr *rootRegistry) takePendingReleases() []*rootGeneration {
	rr.releaseMu.Lock()
	roots := rr.releaseRoots
	rr.releaseRoots = nil
	rr.releaseMu.Unlock()
	return roots
}

func (rr *rootRegistry) markRefCleanupPendingLocked(ref *generationRef) {
	ref.cleanupPending.Store(true)
	if ref.refs.Load() == 0 {
		rr.scheduleCleanup()
	}
}

func (rr *rootRegistry) collectRefCleanupLocked(epoch uint64, ref *generationRef, retired []*rootGeneration) []*rootGeneration {
	if !ref.cleanupPending.Load() || ref.refs.Load() != 0 {
		return retired
	}
	if ref == rr.currentRef.Load() {
		if ref.retired != nil {
			for i := range ref.retired {
				retired = appendRetiredRootGeneration(retired, ref.retired[i])
			}
			rootGenerationRetiredListPool.Put(ref.retired)
			ref.retired = nil
		}
		ref.cleanupPending.Store(false)
		return retired
	}
	return rr.releaseRefLocked(epoch, ref, retired)
}

func (rr *rootRegistry) scheduleCleanup() {
	for {
		switch rr.cleanupState.Load() {
		case rootRegistryCleanupIdle:
			if rr.cleanupState.CompareAndSwap(rootRegistryCleanupIdle, rootRegistryCleanupRunning) {
				go rr.runCleanup()
				return
			}
		case rootRegistryCleanupRunning:
			if rr.cleanupState.CompareAndSwap(rootRegistryCleanupRunning, rootRegistryCleanupAgain) {
				return
			}
		default:
			return
		}
	}
}

func (rr *rootRegistry) runCleanup() {
	for {
		rr.cleanupOnce()
		switch rr.cleanupState.Load() {
		case rootRegistryCleanupRunning:
			if rr.cleanupState.CompareAndSwap(rootRegistryCleanupRunning, rootRegistryCleanupIdle) {
				return
			}
		case rootRegistryCleanupAgain:
			if rr.cleanupState.CompareAndSwap(rootRegistryCleanupAgain, rootRegistryCleanupRunning) {
				continue
			}
		default:
			return
		}
	}
}

func (rr *rootRegistry) cleanupOnce() {
	pendingRoots := rr.takePendingReleases()
	releaseRetiredRootGenerations(pendingRoots)

	var retired []*rootGeneration
	rr.mu.Lock()
	for epoch, ref := range rr.byEpoch {
		retired = rr.collectRefCleanupLocked(epoch, ref, retired)
	}
	rr.mu.Unlock()

	releaseRetiredRootGenerations(retired)
	rr.drainCurrentRuntimeCaches()
}

func (rr *rootRegistry) drainCurrentRuntimeCaches() bool {
	if !rr.runtimeCachesDirty.Load() {
		return false
	}

	rr.mu.Lock()
	// Read states are retained before rr.mu is dropped so their snapshots cannot
	// be released while retired runtime-cache payloads are being detached.
	reads, safeEpoch, ok := rr.retainCurrentRuntimeCacheReadsLocked()
	rr.mu.Unlock()

	if !ok {
		return rr.runtimeCachesDirty.Load()
	}

	var runtimeRetired []snapshot.RuntimeCachesRetired
	stillDirty := false

	for i := range reads {
		snap := reads[i].snap
		if !snap.RuntimeCachesDirty() {
			continue
		}
		retired := snap.TakeRetiredRuntimeCachesBefore(safeEpoch)
		if !retired.Empty() {
			if runtimeRetired == nil {
				runtimeRetired = runtimeCachesRetiredListPool.Get(1)
			}
			runtimeRetired = append(runtimeRetired, retired)
		}
		if snap.RuntimeCachesDirty() {
			stillDirty = true
		}
	}
	releaseRuntimeCacheReadStates(reads)
	releaseRuntimeCachesRetiredList(runtimeRetired)

	if stillDirty {
		rr.markRuntimeCachesDirty()
	}

	return stillDirty || rr.runtimeCachesDirty.Load()
}

func (rr *rootRegistry) retainCurrentRuntimeCacheReadsLocked() ([]*collectionReadState, uint64, bool) {
	if !rr.runtimeCachesDirty.Load() {
		return nil, 0, false
	}
	ref := rr.currentRef.Load()
	if ref == nil || ref.gen == nil {
		rr.runtimeCachesDirty.Store(false)
		return nil, 0, false
	}

	safeEpoch := rr.runtimeEpoch.safeEpoch()

	// Clear under rr.mu after safeEpoch is fixed; later retirements either see a
	// newer reader epoch or mark the root dirty again through the shared owner.
	rr.runtimeCachesDirty.Store(false)

	var reads []*collectionReadState
	for i := range ref.gen.entries {
		read := ref.gen.entries[i].read
		if read == nil || read.snap == nil {
			continue
		}
		read.retain()
		if reads == nil {
			reads = runtimeCacheReadStatePool.Get(1)
		}
		reads = append(reads, read)
	}
	return reads, safeEpoch, true
}

func releaseRuntimeCacheReadStates(reads []*collectionReadState) {
	if reads == nil {
		return
	}
	for i := range reads {
		reads[i].release()
	}
	runtimeCacheReadStatePool.Put(reads)
}

func (rr *rootRegistry) init(epoch uint64) {
	rr.mu.Lock()
	if rr.current.Load() != nil {
		rr.mu.Unlock()
		return
	}
	if rr.byEpoch == nil {
		rr.byEpoch = make(map[uint64]*generationRef, 128)
		rr.staged = make(map[uint64]*rootGeneration, 8)
	}
	gen := getRootGeneration(epoch, 0)
	ref := getRootRef(gen)
	rr.byEpoch[epoch] = ref
	rr.current.Store(gen)
	rr.currentRef.Store(ref)
	rr.mu.Unlock()
}

func (rr *rootRegistry) buildFromCurrent(epoch uint64, entryCount int) *rootGeneration {
	rr.mu.RLock()
	current := rr.current.Load()
	if current != nil && len(current.entries) > entryCount {
		entryCount = len(current.entries)
	}
	gen := getRootGeneration(epoch, entryCount)
	if current != nil {
		copy(gen.entries, current.entries)
		for i := range gen.entries {
			if gen.entries[i].read != nil {
				gen.entries[i].read.retain()
			}
		}
	}
	rr.mu.RUnlock()
	return gen
}

func (gen *rootGeneration) setEntry(ordinal uint32, c *collection, read *collectionReadState) {
	entry := &gen.entries[ordinal]
	if entry.read != nil {
		entry.read.release()
	}
	entry.collection = c
	entry.read = read
}

func (gen *rootGeneration) clearEntry(ordinal uint32) {
	entry := gen.entries[ordinal]
	if entry.read != nil {
		entry.read.release()
	}
	gen.entries[ordinal] = rootCollectionEntry{}
}

func (rr *rootRegistry) stage(epoch uint64, gen *rootGeneration) {
	rr.mu.Lock()
	prev := rr.staged[epoch]
	rr.staged[epoch] = gen
	rr.mu.Unlock()

	if prev != nil {
		releaseRootGeneration(prev)
		if rr.drainCurrentRuntimeCaches() {
			rr.scheduleCleanup()
		}
	}
}

func (rr *rootRegistry) dropStaged(epoch uint64) {
	rr.mu.Lock()
	gen := rr.staged[epoch]
	delete(rr.staged, epoch)
	rr.mu.Unlock()

	if gen != nil {
		releaseRootGeneration(gen)
		if rr.drainCurrentRuntimeCaches() {
			rr.scheduleCleanup()
		}
	}
}

func (rr *rootRegistry) publish(epoch uint64) {
	var retired []*rootGeneration

	rr.mu.Lock()
	gen := rr.staged[epoch]
	if gen == nil {
		rr.mu.Unlock()
		panic(fmt.Sprintf("missing staged root generation for epoch %d", epoch))
	}
	delete(rr.staged, epoch)

	ref := getRootRef(gen)
	rr.byEpoch[epoch] = ref

	prevRef := rr.currentRef.Load()
	rr.current.Store(gen)
	rr.currentRef.Store(ref)
	if prevRef != nil {
		retired = rr.releaseRefIfUnpinnedLocked(prevRef, retired)
	}
	rr.mu.Unlock()

	releaseRetiredRootGenerations(retired)
	if retired != nil {
		if rr.drainCurrentRuntimeCaches() {
			rr.scheduleCleanup()
		}
	}
}

func (rr *rootRegistry) publishMetadata(gen *rootGeneration) {
	var retired []*rootGeneration

	rr.mu.Lock()
	ref := rr.currentRef.Load()
	if ref == nil {
		ref = getRootRef(nil)
		rr.byEpoch[gen.epoch] = ref
		rr.currentRef.Store(ref)

	} else if ref.gen != nil {
		// Metadata publishes reuse the live ref so epoch-pinned readers keep a
		// stable ref object; replaced generations wait behind that ref if needed.
		if ref.refs.Load() == 0 {
			retired = appendRetiredRootGeneration(retired, ref.gen)
			if ref.retired != nil {
				for i := range ref.retired {
					retired = appendRetiredRootGeneration(retired, ref.retired[i])
				}
				rootGenerationRetiredListPool.Put(ref.retired)
				ref.retired = nil
			}
			ref.cleanupPending.Store(false)
		} else {
			ref.retired = appendRetiredRootGeneration(ref.retired, ref.gen)
			rr.markRefCleanupPendingLocked(ref)
		}
	}
	ref.gen = gen
	rr.byEpoch[gen.epoch] = ref
	rr.current.Store(gen)
	rr.mu.Unlock()

	releaseRetiredRootGenerations(retired)
	if retired != nil {
		if rr.drainCurrentRuntimeCaches() {
			rr.scheduleCleanup()
		}
	}
}

func (rr *rootRegistry) pinCurrent() (*rootGeneration, uint64, generationPin) {
	rr.mu.RLock()
	ref := rr.currentRef.Load()
	if ref == nil || ref.gen == nil {
		rr.mu.RUnlock()
		return nil, 0, generationPin{}
	}
	gen := ref.gen
	ref.refs.Add(1)
	readEpoch := rr.runtimeEpoch.pin()
	rr.mu.RUnlock()
	return gen, gen.epoch, generationPin{ref: ref, readEpoch: readEpoch}
}

func (rr *rootRegistry) pinByEpoch(epoch uint64) (*rootGeneration, generationPin, bool) {
	rr.mu.RLock()
	ref := rr.currentRef.Load()
	if ref != nil && ref.gen != nil && ref.gen.epoch == epoch {
		ref.refs.Add(1)
		readEpoch := rr.runtimeEpoch.pin()
		gen := ref.gen
		rr.mu.RUnlock()
		return gen, generationPin{ref: ref, readEpoch: readEpoch}, true
	}
	ref = rr.byEpoch[epoch]
	if ref == nil || ref.gen == nil {
		rr.mu.RUnlock()
		return nil, generationPin{}, false
	}
	ref.refs.Add(1)
	readEpoch := rr.runtimeEpoch.pin()
	gen := ref.gen
	rr.mu.RUnlock()
	return gen, generationPin{ref: ref, readEpoch: readEpoch}, true
}

func (rr *rootRegistry) unpin(epoch uint64, pin generationPin) bool {
	ref := pin.ref
	for {
		refs := ref.refs.Load()
		if refs > 1 {
			if ref.refs.CompareAndSwap(refs, refs-1) {
				rr.runtimeEpoch.unpin(pin.readEpoch)
				rr.scheduleRuntimeCleanup()
				return false
			}
			continue
		}
		break
	}

	var retired []*rootGeneration
	rr.mu.Lock()
	if ref.refs.Add(-1) != 0 {
		rr.runtimeEpoch.unpin(pin.readEpoch)
		rr.mu.Unlock()
		rr.scheduleRuntimeCleanup()
		return false
	}
	rr.runtimeEpoch.unpin(pin.readEpoch)
	retired = rr.collectRefCleanupLocked(epoch, ref, retired)
	rr.mu.Unlock()

	rr.enqueueRetiredRootGenerations(retired)
	rr.scheduleRuntimeCleanup()
	return true
}

func (rr *rootRegistry) releaseRefIfUnpinnedLocked(ref *generationRef, retired []*rootGeneration) []*rootGeneration {
	if ref.refs.Load() != 0 {
		rr.markRefCleanupPendingLocked(ref)
		return retired
	}
	if ref.gen != nil {
		return rr.releaseRefLocked(ref.gen.epoch, ref, retired)
	}
	return retired
}

func (rr *rootRegistry) releaseRefLocked(epoch uint64, ref *generationRef, retired []*rootGeneration) []*rootGeneration {
	if ref.refs.Load() != 0 {
		return retired
	}
	if ref == rr.currentRef.Load() {
		return retired
	}
	if ref.gen != nil {
		retired = appendRetiredRootGeneration(retired, ref.gen)
	}
	if ref.retired != nil {
		for i := range ref.retired {
			retired = appendRetiredRootGeneration(retired, ref.retired[i])
		}
		rootGenerationRetiredListPool.Put(ref.retired)
	}
	ref.gen = nil
	ref.retired = nil
	delete(rr.byEpoch, epoch)
	releaseRootRef(ref)
	return retired
}

func (rr *rootRegistry) reap() bool {
	var retired []*rootGeneration

	rr.mu.Lock()
	if len(rr.staged) != 0 {
		rr.mu.Unlock()
		return false
	}
	for _, ref := range rr.byEpoch {
		if ref.refs.Load() != 0 {
			rr.mu.Unlock()
			return false
		}
	}
	for epoch, ref := range rr.byEpoch {
		if ref.gen != nil {
			retired = appendRetiredRootGeneration(retired, ref.gen)
		}
		if ref.retired != nil {
			for i := range ref.retired {
				retired = appendRetiredRootGeneration(retired, ref.retired[i])
			}
			rootGenerationRetiredListPool.Put(ref.retired)
		}
		delete(rr.byEpoch, epoch)
		releaseRootRef(ref)
	}
	rr.current.Store(nil)
	rr.currentRef.Store(nil)
	rr.mu.Unlock()

	releaseRetiredRootGenerations(retired)
	if retired != nil {
		if rr.drainCurrentRuntimeCaches() {
			rr.scheduleCleanup()
		}
	}
	return true
}
