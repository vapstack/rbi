package rbi

import (
	"errors"
	"io"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
)

type rootRegistryCacheRec struct {
	Email string `rbi:"index"`
}

type rootRegistryNumericCacheRec struct {
	Age int `rbi:"index"`
}

func TestReserveCollectionInstallsLoggerBeforeNew(t *testing.T) {
	logger := log.Default()
	out := logger.Writer()
	logger.SetOutput(io.Discard)
	defer logger.SetOutput(out)

	raw, path := openRawBolt(t)
	defer raw.Close()

	r, p, err := reserveCollection(raw, path, "users", logger)
	if err != nil {
		t.Fatalf("reserveCollection: %v", err)
	}
	defer unreserveCollection(p)

	if p.logger == nil {
		t.Fatal("reserved collection logger is nil")
	}
	r.markBroken("test", errors.New("boom"))

	if !r.broken.Load() {
		t.Fatal("root was not marked broken")
	}
	if p.state.Load()&collectionBroken == 0 {
		t.Fatal("collection was not marked broken")
	}
}

func TestCollectionUnavailableSeesRootBrokenBeforeCollectionPropagation(t *testing.T) {
	r := &rootStore{
		collections: map[string]*collection{},
	}
	c := &collection{
		root:       r,
		dataBucket: []byte("users"),
		logger:     log.New(io.Discard, "", 0),
	}
	r.collections["users"] = c

	r.mu.Lock()
	done := make(chan struct{})
	go func() {
		r.markBroken("test", errors.New("boom"))
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for !r.broken.Load() {
		if time.Now().After(deadline) {
			r.mu.Unlock()
			t.Fatal("root was not marked broken")
		}
		time.Sleep(time.Millisecond)
	}

	if c.state.Load()&collectionBroken != 0 {
		r.mu.Unlock()
		t.Fatal("collection was marked broken before propagation lock was released")
	}
	if err := c.unavailableErr(); !errors.Is(err, rbierrors.ErrBroken) {
		r.mu.Unlock()
		t.Fatalf("unavailableErr=%v want ErrBroken", err)
	}
	r.mu.Unlock()
	<-done
}

func TestRootRegistryStagePublishAndPin(t *testing.T) {
	var rr rootRegistry
	rr.init(1)

	c := &collection{ordinal: 0, dataBucket: []byte("users")}
	read := newCollectionReadState(c, 12, nil)
	gen := rr.buildFromCurrent(2, 1)
	gen.setEntry(0, c, read)
	rr.stage(2, gen)

	if pinned, pin, ok := rr.pinByEpoch(2); ok || pinned != nil || pin.ref != nil {
		t.Fatalf("staged generation was pinnable before publish")
	}

	rr.publish(2)

	pinned, epoch, pin := rr.pinCurrent()
	if pinned == nil || pin.ref == nil || epoch != 2 {
		t.Fatalf("PinCurrent epoch=%d gen=%v pin=%v", epoch, pinned, pin)
	}
	if got := pinned.entries[0]; got.collection != c || got.read != read || got.read.dataSeq != 12 {
		t.Fatalf("published entry=%+v", got)
	}
	rr.unpin(epoch, pin)

	if pinned, pin, ok := rr.pinByEpoch(1); ok || pinned != nil || pin.ref != nil {
		t.Fatalf("old unpinned generation remained pinnable")
	}
	if read.refs.Load() != 1 {
		t.Fatalf("published read refs=%d want 1", read.refs.Load())
	}
	if !rr.reap() {
		t.Fatal("expected registry reap to succeed")
	}
	if read.refs.Load() != 0 {
		t.Fatalf("reaped read refs=%d want 0", read.refs.Load())
	}
}

func TestRootRegistryPublishMetadataRetiresPinnedCurrentGeneration(t *testing.T) {
	var rr rootRegistry
	rr.init(1)

	c := &collection{ordinal: 0, dataBucket: []byte("users")}
	read := newCollectionReadState(c, 7, nil)
	gen := rr.buildFromCurrent(2, 1)
	gen.setEntry(0, c, read)
	rr.stage(2, gen)
	rr.publish(2)

	pinned, epoch, pin := rr.pinCurrent()
	if pinned == nil || pinned.entries[0].read != read {
		t.Fatalf("expected current generation with read state")
	}

	next := rr.buildFromCurrent(epoch, len(pinned.entries))
	next.clearEntry(0)
	rr.publishMetadata(next)

	current, currentEpoch, currentRef := rr.pinCurrent()
	if currentEpoch != epoch || currentRef.ref != pin.ref {
		t.Fatalf("metadata publish changed epoch/ref: epoch=%d ref=%v", currentEpoch, currentRef)
	}
	if current.entries[0].read != nil || current.entries[0].collection != nil {
		t.Fatalf("metadata generation kept cleared collection: %+v", current.entries[0])
	}
	rr.unpin(currentEpoch, currentRef)

	if read.refs.Load() != 1 {
		t.Fatalf("pinned retired read refs=%d want 1", read.refs.Load())
	}
	rr.unpin(epoch, pin)
	waitRootRegistryCleanup(t, &rr)
	if read.refs.Load() != 0 {
		t.Fatalf("unpinned retired read refs=%d want 0", read.refs.Load())
	}
	if !rr.reap() {
		t.Fatal("expected registry reap to release metadata current generation")
	}
}

func TestRootRegistryRuntimePinDefersCurrentCacheDrain(t *testing.T) {
	r := &rootStore{}
	r.registry.init(1)

	c := &collection{root: r, ordinal: 0, dataBucket: []byte("users")}
	snap := newRootRegistrySnapshot(t, &r.registry)
	read := newCollectionReadState(c, 9, snap)
	gen := r.registry.buildFromCurrent(2, 1)
	gen.setEntry(0, c, read)
	r.registry.stage(2, gen)
	r.registry.publish(2)

	_, epoch, pin := r.registry.pinCurrent()
	readEpoch := read.retainRuntimePin(&r.registry)
	dirtyRootRegistrySnapshot(snap)
	r.registry.unpin(epoch, pin)
	if !snap.RuntimeCachesDirty() {
		t.Fatal("current root unpin drained runtime caches while runtime pin was active")
	}

	read.releaseRuntimePin(readEpoch)
	waitRootRegistryCleanup(t, &r.registry)
	if snap.RuntimeCachesDirty() {
		t.Fatal("last runtime pin release did not drain retired runtime caches")
	}
	if read.refs.Load() != 1 {
		t.Fatalf("read refs=%d want 1", read.refs.Load())
	}
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
	if read.refs.Load() != 0 {
		t.Fatalf("reaped read refs=%d want 0", read.refs.Load())
	}
}

func TestRootRegistryLastUnpinSchedulesCurrentRuntimeCachesDrain(t *testing.T) {
	r := &rootStore{}
	r.registry.init(1)

	c := &collection{root: r, ordinal: 0, dataBucket: []byte("users")}
	snap := newRootRegistrySnapshot(t, &r.registry)
	read := newCollectionReadState(c, 9, snap)
	gen := r.registry.buildFromCurrent(2, 1)
	gen.setEntry(0, c, read)
	r.registry.stage(2, gen)
	r.registry.publish(2)

	_, epoch, pin := r.registry.pinCurrent()
	dirtyRootRegistrySnapshot(snap)
	r.registry.cleanupState.Store(rootRegistryCleanupRunning)
	r.registry.unpin(epoch, pin)
	if !snap.RuntimeCachesDirty() {
		t.Fatal("last root unpin detached retired runtime caches on reader path")
	}

	_, repinEpoch, repinPin := r.registry.pinCurrent()
	if !snap.RuntimeCachesDirty() {
		t.Fatal("repin observed runtime caches drained before background cleanup")
	}
	r.registry.unpin(repinEpoch, repinPin)

	r.registry.cleanupState.Store(rootRegistryCleanupIdle)
	r.registry.scheduleCleanup()
	waitRootRegistryCleanup(t, &r.registry)
	if snap.RuntimeCachesDirty() {
		t.Fatal("background cleanup did not detach retired runtime caches")
	}
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
	if read.refs.Load() != 0 {
		t.Fatalf("reaped read refs=%d want 0", read.refs.Load())
	}
}

func TestRootRegistryRuntimeCleanupDefersUntilRuntimePinRelease(t *testing.T) {
	r := &rootStore{}
	r.registry.init(1)

	c := &collection{root: r, ordinal: 0, dataBucket: []byte("users")}
	snap := newRootRegistrySnapshot(t, &r.registry)
	read := newCollectionReadState(c, 9, snap)
	gen := r.registry.buildFromCurrent(2, 1)
	gen.setEntry(0, c, read)
	r.registry.stage(2, gen)
	r.registry.publish(2)

	readEpoch := read.retainRuntimePin(&r.registry)
	dirtyRootRegistrySnapshot(snap)

	r.registry.scheduleCleanup()
	time.Sleep(5 * time.Millisecond)
	if !snap.RuntimeCachesDirty() {
		t.Fatal("runtime cleanup detached retired caches before safe epoch advanced")
	}

	read.releaseRuntimePin(readEpoch)
	waitRootRegistryCleanup(t, &r.registry)
	if snap.RuntimeCachesDirty() {
		t.Fatal("runtime pin release did not drain retired runtime caches")
	}
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
	if read.refs.Load() != 0 {
		t.Fatalf("reaped read refs=%d want 0", read.refs.Load())
	}
}

func TestRootRegistryCleanupAppendsCurrentRetiredGenerations(t *testing.T) {
	r := &rootStore{}
	r.registry.init(1)

	c := &collection{root: r, ordinal: 0, dataBucket: []byte("users")}
	currentRead := newCollectionReadState(c, 10, nil)
	currentRetiredRead := newCollectionReadState(c, 30, nil)
	oldReads := make([]*collectionReadState, 16)

	currentGen := r.registry.buildFromCurrent(2, 1)
	currentGen.setEntry(0, c, currentRead)
	r.registry.stage(2, currentGen)
	r.registry.publish(2)
	_, currentEpoch, currentPin := r.registry.pinCurrent()

	currentRetiredGen := getRootGeneration(5, 1)
	currentRetiredGen.setEntry(0, c, currentRetiredRead)

	r.registry.mu.Lock()
	for i := range oldReads {
		read := newCollectionReadState(c, uint64(40+i), nil)
		oldReads[i] = read
		oldGen := getRootGeneration(uint64(10+i*2), 1)
		oldGen.setEntry(0, c, read)
		oldRef := getRootRef(oldGen)
		oldRetiredGen := getRootGeneration(uint64(11+i*2), 1)
		oldRetiredGen.setEntry(0, c, read)
		read.retain()
		r.registry.byEpoch[oldGen.epoch] = oldRef
		oldRef.retired = appendRetiredRootGeneration(oldRef.retired, oldRetiredGen)
		oldRef.cleanupPending.Store(true)
	}
	currentPin.ref.retired = appendRetiredRootGeneration(currentPin.ref.retired, currentRetiredGen)
	currentPin.ref.cleanupPending.Store(true)
	r.registry.mu.Unlock()

	r.registry.unpin(currentEpoch, currentPin)
	waitRootRegistryCleanup(t, &r.registry)

	for i := range oldReads {
		if oldReads[i].refs.Load() != 0 {
			t.Fatalf("old read %d refs=%d want 0", i, oldReads[i].refs.Load())
		}
	}
	if currentRetiredRead.refs.Load() != 0 {
		t.Fatalf("current retired read refs=%d want 0", currentRetiredRead.refs.Load())
	}
	if currentRead.refs.Load() != 1 {
		t.Fatalf("current read refs=%d want 1", currentRead.refs.Load())
	}
	if currentPin.ref.retired != nil {
		t.Fatal("current ref retained retired generations after cleanup")
	}
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
	if currentRead.refs.Load() != 0 {
		t.Fatalf("reaped current read refs=%d want 0", currentRead.refs.Load())
	}
}

func TestRootRegistryCleanCurrentUnpinDoesNotQueueCleanup(t *testing.T) {
	var rr rootRegistry
	rr.init(1)

	_, epoch, pin := rr.pinCurrent()
	rr.unpin(epoch, pin)
	if state := rr.cleanupState.Load(); state != rootRegistryCleanupIdle {
		t.Fatalf("clean current unpin queued cleanup state=%d", state)
	}
	if !rr.reap() {
		t.Fatal("expected registry reap to succeed")
	}
}

func TestRootRegistryOldGenerationReleaseReDrainsCurrentNumericRuntimeCaches(t *testing.T) {
	r := &rootStore{}
	r.registry.init(1)

	rt, err := schema.Compile(reflect.TypeOf(rootRegistryNumericCacheRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	field := rt.Indexed[0].Name

	firstIndex := indexdata.GetFieldStorageSlice(len(rt.Indexed))[:len(rt.Indexed)]
	firstIndex[0] = newRootRegistryNumericStorage()
	first := snapshot.NewView(17, nil, rt, rootRegistrySnapshotCacheConfig(&r.registry, 0), snapshot.Storage{Index: firstIndex})
	entry := qcache.GetNumericRangeBucketEntry(first.Index[0], qcache.NumericRangeBucketIndex{}, 0)
	first.NumericRangeBucketCache().StoreSlot(field, 0, entry)

	c := &collection{root: r, ordinal: 0, dataBucket: []byte("users")}
	firstRead := newCollectionReadState(c, first.Seq, first)
	firstGen := r.registry.buildFromCurrent(2, 1)
	firstGen.setEntry(0, c, firstRead)
	r.registry.stage(2, firstGen)
	r.registry.publish(2)

	_, oldEpoch, oldPin := r.registry.pinCurrent()

	second := snapshot.NewView(18, first, rt, rootRegistrySnapshotCacheConfig(&r.registry, 0), snapshot.Storage{
		Index: indexdata.CloneFieldStorageSlots(first.Index, len(rt.Indexed)),
	})
	second.NumericRangeBucketCache().InheritFrom(first.NumericRangeBucketCache(), second.Index, second.IndexedFieldByName)
	if got, ok := second.NumericRangeBucketCache().LoadField(field); !ok || got != entry {
		t.Fatal("expected second snapshot to inherit first numeric range entry")
	}

	secondRead := newCollectionReadState(c, second.Seq, second)
	secondGen := r.registry.buildFromCurrent(3, 1)
	secondGen.setEntry(0, c, secondRead)
	r.registry.stage(3, secondGen)
	r.registry.publish(3)

	evicted := false
	count := entry.FullSpanEntryCount()
	for i := 0; !evicted && i < 64; i++ {
		cached, ok := entry.TryStoreFullSpan(i, i, (posting.List{}).BuildAdded(uint64(i+1)))
		if !ok {
			t.Fatalf("TryStoreFullSpan(%d): not stored", i)
		}
		cached.Release()
		nextCount := entry.FullSpanEntryCount()
		evicted = nextCount == count
		count = nextCount
	}
	if !evicted {
		t.Fatal("expected shared numeric entry to have retired full-span postings")
	}

	_, currentEpoch, currentPin := r.registry.pinCurrent()
	r.registry.unpin(currentEpoch, currentPin)
	waitRootRegistryCleanup(t, &r.registry)
	retained := second.NumericRangeBucketCache().TakeRetiredBefore(r.registry.runtimeEpoch.safeEpoch())
	if !retained.IsEmpty() {
		retained.Release()
		t.Fatal("expected current drain to keep retired postings while numeric entry is shared")
	}
	retained.Release()
	if entry.FullSpanEntryCount() == 0 {
		t.Fatal("expected current drain to keep full-span cache while numeric entry is shared")
	}

	r.registry.unpin(oldEpoch, oldPin)
	waitRootRegistryCleanup(t, &r.registry)
	redrained := second.NumericRangeBucketCache().TakeRetiredBefore(r.registry.runtimeEpoch.safeEpoch())
	if !redrained.IsEmpty() {
		redrained.Release()
		t.Fatal("expected old generation release to re-drain current numeric retired postings")
	}
	redrained.Release()
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
}

func TestRootRegistryRuntimePinReleaseDrainsCurrentNumericRetiredWithCurrentPin(t *testing.T) {
	r := &rootStore{}
	r.registry.init(1)

	rt, err := schema.Compile(reflect.TypeOf(rootRegistryNumericCacheRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	field := rt.Indexed[0].Name

	firstIndex := indexdata.GetFieldStorageSlice(len(rt.Indexed))[:len(rt.Indexed)]
	firstIndex[0] = newRootRegistryNumericStorage()
	first := snapshot.NewView(17, nil, rt, rootRegistrySnapshotCacheConfig(&r.registry, 0), snapshot.Storage{Index: firstIndex})
	entry := qcache.GetNumericRangeBucketEntry(first.Index[0], qcache.NumericRangeBucketIndex{}, 0)
	first.NumericRangeBucketCache().StoreSlot(field, 0, entry)

	c := &collection{root: r, ordinal: 0, dataBucket: []byte("users")}
	firstRead := newCollectionReadState(c, first.Seq, first)
	firstGen := r.registry.buildFromCurrent(2, 1)
	firstGen.setEntry(0, c, firstRead)
	r.registry.stage(2, firstGen)
	r.registry.publish(2)

	readEpoch := firstRead.retainRuntimePin(&r.registry)

	second := snapshot.NewView(18, first, rt, rootRegistrySnapshotCacheConfig(&r.registry, 0), snapshot.Storage{
		Index: indexdata.CloneFieldStorageSlots(first.Index, len(rt.Indexed)),
	})
	second.NumericRangeBucketCache().InheritFrom(first.NumericRangeBucketCache(), second.Index, second.IndexedFieldByName)
	if got, ok := second.NumericRangeBucketCache().LoadField(field); !ok || got != entry {
		t.Fatal("expected second snapshot to inherit first numeric range entry")
	}

	secondRead := newCollectionReadState(c, second.Seq, second)
	secondGen := r.registry.buildFromCurrent(3, 1)
	secondGen.setEntry(0, c, secondRead)
	r.registry.stage(3, secondGen)
	r.registry.publish(3)

	evicted := false
	count := entry.FullSpanEntryCount()
	for i := 0; !evicted && i < 64; i++ {
		cached, ok := entry.TryStoreFullSpan(i, i, (posting.List{}).BuildAdded(uint64(i+1)))
		if !ok {
			t.Fatalf("TryStoreFullSpan(%d): not stored", i)
		}
		cached.Release()
		nextCount := entry.FullSpanEntryCount()
		evicted = nextCount == count
		count = nextCount
	}
	if !evicted {
		t.Fatal("expected shared numeric entry to have retired full-span postings")
	}

	_, currentEpoch, currentPin := r.registry.pinCurrent()
	firstRead.releaseRuntimePin(readEpoch)
	if !second.RuntimeCachesDirty() {
		t.Fatal("old runtime pin release detached current retired runtime caches on reader path")
	}
	waitRootRegistryCleanup(t, &r.registry)
	if second.RuntimeCachesDirty() {
		t.Fatal("background cleanup did not drain current retired runtime caches while current root was pinned")
	}
	retained := second.NumericRangeBucketCache().TakeRetiredBefore(r.registry.runtimeEpoch.safeEpoch())
	if !retained.IsEmpty() {
		retained.Release()
		t.Fatal("expected current numeric retired postings to be detached by old runtime pin release")
	}
	retained.Release()

	r.registry.unpin(currentEpoch, currentPin)
	waitRootRegistryCleanup(t, &r.registry)
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
}

func TestCollectionPinsCurrentReadStateBySnapshotSeq(t *testing.T) {
	r := new(rootStore)
	r.registry.init(1)

	c := &collection{
		root:       r,
		ordinal:    0,
		dataBucket: []byte("users"),
	}
	snap := newRootRegistryDirtySnapshot(t, &r.registry)
	read := newCollectionReadState(c, 4, snap)
	gen := r.registry.buildFromCurrent(2, 1)
	gen.setEntry(0, c, read)
	r.registry.stage(2, gen)
	r.registry.publish(2)

	if seq := c.currentSnapshotSeq(); seq != snap.Seq {
		t.Fatalf("current snapshot seq=%d want %d", seq, snap.Seq)
	}
	pinned, pin, ok := c.pinCurrentReadStateBySnapshotSeq(snap.Seq)
	if !ok || pinned != snap {
		t.Fatalf("pin current read state ok=%v snap=%v want %v", ok, pinned, snap)
	}
	if read.refs.Load() != 2 || read.runtimePins.Load() != 1 {
		t.Fatalf("read refs=%d runtimePins=%d want 2/1", read.refs.Load(), read.runtimePins.Load())
	}
	pin.Unpin()
	if read.refs.Load() != 1 || read.runtimePins.Load() != 0 {
		t.Fatalf("after unpin refs=%d runtimePins=%d want 1/0", read.refs.Load(), read.runtimePins.Load())
	}
	if pinned, _, ok = c.pinCurrentReadStateBySnapshotSeq(snap.Seq + 1); ok || pinned != nil {
		t.Fatalf("seq mismatch pinned current read state: ok=%v snap=%v", ok, pinned)
	}

	current := r.registry.current.Load()
	next := r.registry.buildFromCurrent(current.epoch, len(current.entries))
	next.clearEntry(0)
	r.registry.publishMetadata(next)
	if seq := c.currentSnapshotSeq(); seq != 0 {
		t.Fatalf("closed collection current snapshot seq=%d want 0", seq)
	}
	if pinned, _, ok = c.pinCurrentReadStateBySnapshotSeq(snap.Seq); ok || pinned != nil {
		t.Fatalf("closed collection pinned current read state: ok=%v snap=%v", ok, pinned)
	}
	if !r.registry.reap() {
		t.Fatal("expected registry reap to release current generation")
	}
	if read.refs.Load() != 0 {
		t.Fatalf("reaped read refs=%d want 0", read.refs.Load())
	}
}

func rootRegistrySnapshotCacheConfig(rr *rootRegistry, matPredMaxEntries int) snapshot.CacheConfig {
	return snapshot.CacheConfig{
		MatPredMaxEntries:        matPredMaxEntries,
		RuntimeCachesDirtyOwner:  &rr.runtimeCachesDirty,
		RuntimeCachesRetireEpoch: &rr.runtimeEpoch.issued,
	}
}

func newRootRegistrySnapshot(t testing.TB, rr *rootRegistry) *snapshot.View {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeOf(rootRegistryCacheRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	return snapshot.NewView(1, nil, rt, rootRegistrySnapshotCacheConfig(rr, 1), snapshot.Storage{})
}

func waitRootRegistryCleanup(t testing.TB, rr *rootRegistry) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for rr.cleanupState.Load() != rootRegistryCleanupIdle {
		if time.Now().After(deadline) {
			t.Fatal("root registry cleanup did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func newRootRegistryDirtySnapshot(t testing.TB, rr *rootRegistry) *snapshot.View {
	t.Helper()

	snap := newRootRegistrySnapshot(t, rr)
	dirtyRootRegistrySnapshot(snap)
	if !snap.RuntimeCachesDirty() {
		t.Fatal("expected materialized predicate eviction to dirty runtime caches")
	}
	return snap
}

func dirtyRootRegistrySnapshot(snap *snapshot.View) {
	snap.StoreMaterializedPredKey(qcache.MaterializedPredKeyFromOpaque("a"), (posting.List{}).BuildAdded(1))
	snap.StoreMaterializedPredKey(qcache.MaterializedPredKeyFromOpaque("b"), (posting.List{}).BuildAdded(2))
}

func newRootRegistryNumericStorage() indexdata.FieldStorage {
	m := indexdata.GetPostingMap()
	m["10"] = (posting.List{}).BuildAdded(1)
	m["20"] = (posting.List{}).BuildAdded(2)
	return indexdata.NewFlatFieldStorageFromPostingMapOwned(m)
}
