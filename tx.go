package rbi

import (
	"errors"
	"fmt"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

var errGenerationUnavailable = errors.New("generation unavailable")

type txLifecyclePanic string

func (p txLifecyclePanic) Error() string { return string(p) }

type Tx struct {
	kind uint8

	root   *rootStore
	boltTx *bbolt.Tx
	epoch  uint64
	gen    *rootGeneration
	pin    generationPin
	done   bool

	collections  []*collection
	readBindings []txReadBinding
	unit         writeUnit
	err          error
	state        uint8
	managed      bool

	hookUnit   *writeUnit
	hookDepth  uint8
	hookBatch  int
	hookInsert int
}

type writeSegment struct {
	collection *collection
	ops        wexec.Batch
	work       int
}

type txReadBinding struct {
	state  *collectionReadState
	bucket *bbolt.Bucket
}

var txPool = pooled.Pointers[Tx]{
	New: func() *Tx {
		return &Tx{
			collections:  make([]*collection, 0, 4),
			readBindings: make([]txReadBinding, 0, 4),
			unit: writeUnit{
				segments: make([]writeSegment, 0, 4),
			},
		}
	},
	Cleanup: func(tx *Tx) {
		clear(tx.collections)
		clear(tx.readBindings)
		clear(tx.unit.segments)
		clear(tx.unit.generatedCollections)
		hookTx := tx.unit.hookTx
		if hookTx != nil {
			*hookTx = Tx{}
		}

		*tx = Tx{
			collections:  tx.collections[:0],
			readBindings: tx.readBindings[:0],
			unit: writeUnit{
				done:                 tx.unit.done,
				segments:             tx.unit.segments[:0],
				generatedCollections: tx.unit.generatedCollections[:0],
				hookTx:               hookTx,
			},
		}
	},
}

type stagedCollectionWrite struct {
	collection *collection
	dataSeq    uint64
	snap       *snapshot.View
}

const (
	writeTxOpen uint8 = iota
	writeTxCommitting
	writeTxClosed
	writeTxCommitted
	writeTxTerminal
	writeTxHook
)

const (
	txKindUnset uint8 = iota
	txKindRead
	txKindIndex
	txKindWrite
)

const maxGeneratedWriteDepth uint8 = 64

// BeginView creates an unbound read transaction.
//
// The returned Tx must be closed with Close.
//
// Performance-critical callers may use Release instead of Close to reduce
// allocations. Release is not idempotent: it must be called exactly once,
// and only when the caller can guarantee that the Tx will not be used again.
func BeginView() *Tx {
	tx := txPool.Get()
	tx.kind = txKindRead
	return tx
}

// View executes fn within a managed read-only transaction.
// Any error that is returned from the function is returned to the caller.
//
// Attempting to manually commit or rollback transaction will cause a panic.
func View(fn func(*Tx) error) error {
	tx := txPool.Get()
	tx.kind = txKindRead
	tx.managed = true
	defer func() {
		tx.managed = false
		tx.Release()
	}()
	err := fn(tx)
	tx.managed = false
	return err
}

// BeginIndexView creates an unbound index-only transaction.
//
// The returned Tx must be closed with Close.
//
// Performance-critical callers may use Release instead of Close to reduce
// allocations. Release is not idempotent: it must be called exactly once,
// and only when the caller can guarantee that the Tx will not be used again.
func BeginIndexView() *Tx {
	tx := txPool.Get()
	tx.kind = txKindIndex
	return tx
}

// IndexView executes fn within a managed index-only transaction.
// Any error that is returned from the function is returned to the caller.
//
// Attempting to manually commit or rollback transaction will cause a panic.
func IndexView(fn func(*Tx) error) error {
	tx := txPool.Get()
	tx.kind = txKindIndex
	tx.managed = true
	defer func() {
		tx.managed = false
		tx.Release()
	}()
	err := fn(tx)
	tx.managed = false
	return err
}

// BeginUpdate creates an unbound write transaction.
//
// The returned Tx must be closed with Close.
//
// Performance-critical callers may use Release instead of Close to reduce
// allocations. Release is not idempotent: it must be called exactly once,
// and only when the caller can guarantee that the Tx will not be used again.
func BeginUpdate() *Tx {
	tx := txPool.Get()
	tx.kind = txKindWrite
	return tx
}

// Update executes fn within a managed write-only transaction.
//
// If fn returns nil, Update commits the transaction. If fn returns an error,
// the transaction is rolled back and that error is returned. If Commit fails,
// the commit error is returned.
//
// Attempting to manually commit or rollback the transaction will cause a panic.
func Update(fn func(*Tx) error) error {
	tx := txPool.Get()
	tx.kind = txKindWrite
	tx.managed = true
	defer func() {
		tx.managed = false
		tx.Release()
	}()
	err := fn(tx)
	tx.managed = false
	if err != nil {
		return err
	}
	return tx.commit()
}

// Close closes the transaction.
//
// For write transactions it rolls back any uncommitted writes.
func (tx *Tx) Close() {
	tx.rejectLifecycleCall("Close")
	tx.close()
}

// Release closes the transaction and returns it to the internal pool.
//
// Release is not idempotent. The caller must not use tx after Release.
//
// Release may only be called by the owner of a Tx returned by
// BeginUpdate, BeginView, or BeginIndexView.
// If unsure about transaction ownership or lifecycle, do not use this method.
func (tx *Tx) Release() {
	tx.rejectLifecycleCall("Release")
	tx.close()
	txPool.Put(tx)
}

func (tx *Tx) close() {
	switch tx.kind {
	case txKindRead:
		tx.closeRead()
	case txKindIndex:
		tx.closeIndex()
	case txKindWrite:
		if tx.state == writeTxOpen {
			tx.cancelBatches()
			tx.releaseCollections()
			tx.state = writeTxClosed
		}
	}
}

func (tx *Tx) closeRead() {
	if tx.done {
		return
	}
	r := tx.root
	btx := tx.boltTx
	epoch := tx.epoch
	pin := tx.pin

	tx.done = true

	clear(tx.collections)
	clear(tx.readBindings)
	tx.collections = tx.collections[:0]
	tx.readBindings = tx.readBindings[:0]

	if btx != nil {
		_ = btx.Rollback()
	}
	if pin.ref != nil && r.registry.unpin(epoch, pin) && r.reapPending.Load() {
		tryReapRoot(r)
	}
}

func (tx *Tx) closeIndex() {
	if tx.done {
		return
	}
	r := tx.root
	epoch := tx.epoch
	pin := tx.pin

	tx.done = true

	if pin.ref != nil && r.registry.unpin(epoch, pin) && r.reapPending.Load() {
		tryReapRoot(r)
	}
}

// Commit commits all writes queued in the transaction.
func (tx *Tx) Commit() error {
	tx.rejectLifecycleCall("Commit")
	if err := tx.useWrite(); err != nil {
		return err
	}
	return tx.commit()
}

func (tx *Tx) rejectLifecycleCall(op string) {
	if tx.managed {
		panic(txLifecyclePanic(op + " is not allowed inside View, IndexView, or Update"))
	}
	switch tx.state {
	case writeTxHook:
		panic(txLifecyclePanic(op + " is not allowed on the Tx passed to OnChange"))
	case writeTxCommitting:
		panic(txLifecyclePanic(op + " is not allowed on the outer Tx while Commit is running; inside OnChange use the passed Tx for generated writes"))
	}
}

func (tx *Tx) useWrite() error {
	if tx.kind != txKindWrite {
		return rbierrors.ErrWrongTx
	}
	return nil
}

func (tx *Tx) commit() error {
	if err := tx.useWrite(); err != nil {
		return err
	}
	switch tx.state {

	case writeTxOpen:
		var err error
		if len(tx.unit.segments) != 0 {
			work := 0
			limit := tx.unit.segments[0].collection.options.BatchSoftLimit
			for i := range tx.unit.segments {
				work += tx.unit.segments[i].work
				if maxOps := tx.unit.segments[i].collection.options.BatchSoftLimit; maxOps < limit {
					limit = maxOps
				}
			}
			unit := &tx.unit
			unit.root = tx.root
			unit.work = work
			unit.limit = limit
			tx.state = writeTxCommitting
			err = unit.root.scheduler.submit(unit)
		}
		tx.releaseCollections()
		if err != nil {
			tx.err = err
			tx.state = writeTxTerminal
			return err
		}
		tx.state = writeTxCommitted
		return nil

	case writeTxCommitted, writeTxClosed:
		return rbierrors.ErrTxDone

	default:
		if tx.err != nil {
			return tx.err
		}
		return rbierrors.ErrTxDone
	}
}

func (r *rootStore) stageCollectionWrites(epoch uint64, writes []stagedCollectionWrite) {
	entryCount := 0
	for i := range writes {
		next := int(writes[i].collection.ordinal) + 1
		if entryCount < next {
			entryCount = next
		}
	}
	gen := r.registry.buildFromCurrent(epoch, entryCount)
	for i := range writes {
		write := &writes[i]
		// The read state becomes the snapshot owner; index.current only publishes
		// the pointer after commit and does not release it independently.
		gen.setEntry(write.collection.ordinal, write.collection, newCollectionReadState(write.collection, write.dataSeq, write.snap))
	}
	r.registry.stage(epoch, gen)
}

func (tx *Tx) collectionSegment(c *collection) (*writeSegment, error) {
	if tx.state == writeTxHook {
		return tx.generatedCollectionSegment(c)
	}
	if tx.state != writeTxOpen {
		if err := tx.useWrite(); err != nil {
			return nil, err
		}
		if tx.err != nil {
			return nil, tx.err
		}
		return nil, rbierrors.ErrTxDone
	}
	if n := len(tx.unit.segments); n != 0 && tx.unit.segments[n-1].collection == c {
		return &tx.unit.segments[n-1], nil
	}
	if err := tx.bindCollection(c); err != nil {
		return nil, err
	}

	tx.unit.segments = append(tx.unit.segments, writeSegment{
		collection: c,
		ops:        c.executor.NewBatch(),
	})

	return &tx.unit.segments[len(tx.unit.segments)-1], nil
}

func (tx *Tx) bindCollection(c *collection) error {
	bound, err := tx.usableCollection(c)
	if err != nil || bound {
		return err
	}
	if tx.root == nil {
		tx.root = c.root
	}
	if err = c.retain(); err != nil {
		return tx.terminal(err)
	}
	tx.collections = append(tx.collections, c)
	return nil
}

func (tx *Tx) usableCollection(p *collection) (bool, error) {
	if tx.state == writeTxHook {
		return true, tx.checkGeneratedCollection(p)
	}
	if err := tx.useWrite(); err != nil {
		return false, err
	}
	if tx.state != writeTxOpen {
		if tx.err != nil {
			return false, tx.err
		}
		return false, rbierrors.ErrTxDone
	}
	if tx.root != nil {
		for i := range tx.collections {
			if tx.collections[i] == p {
				return true, nil
			}
		}
		if tx.root != p.root {
			return false, tx.terminal(rbierrors.ErrStoreMismatch)
		}
	}
	if err := p.unavailableErr(); err != nil {
		return false, tx.terminal(err)
	}
	return false, nil
}

func (tx *Tx) terminal(err error) error {
	if tx.state == writeTxHook {
		tx.err = err
		return err
	}
	tx.err = err
	tx.state = writeTxTerminal
	tx.cancelBatches()
	tx.releaseCollections()
	return err
}

func (tx *Tx) generatedDepth() uint8 {
	if tx.state == writeTxHook {
		return tx.hookDepth + 1
	}
	return 0
}

func (tx *Tx) checkGeneratedCollection(c *collection) error {
	if tx.err != nil {
		return tx.err
	}
	if tx.root != c.root {
		tx.err = rbierrors.ErrStoreMismatch
		return tx.err
	}
	return nil
}

func (tx *Tx) generatedCollectionSegment(c *collection) (*writeSegment, error) {
	if err := tx.checkGeneratedCollection(c); err != nil {
		return nil, err
	}
	if tx.hookDepth >= maxGeneratedWriteDepth {
		tx.err = rbierrors.ErrGeneratedWriteDepth
		return nil, tx.err
	}
	unit := tx.hookUnit

	// Generated writes share the parent logical unit, but collections first
	// touched from a hook need their own retain because the parent Tx did not bind them.
	found := false
	for i := range unit.segments {
		if unit.segments[i].collection == c {
			found = true
			break
		}
	}
	if !found {
		if err := c.retain(); err != nil {
			tx.err = err
			return nil, err
		}
		unit.generatedCollections = append(unit.generatedCollections, c)
		if maxOps := c.options.BatchSoftLimit; maxOps < unit.limit {
			unit.limit = maxOps
		}
	}

	insert := tx.hookInsert
	if insert > tx.hookBatch+1 && unit.segments[insert-1].collection == c {
		return &unit.segments[insert-1], nil
	}

	// Insert after the hook batch being prepared so recursive writes execute in
	// depth-first order without mutating already accepted segments.
	unit.segments = append(unit.segments, writeSegment{})
	copy(unit.segments[insert+1:], unit.segments[insert:])
	unit.segments[insert] = writeSegment{
		collection: c,
		ops:        c.executor.NewBatch(),
	}
	tx.hookInsert = insert + 1

	return &unit.segments[insert], nil
}

func (tx *Tx) cancelBatches() {
	for i := len(tx.unit.segments) - 1; i >= 0; i-- {
		tx.unit.segments[i].ops.Cancel()
	}
	clear(tx.unit.segments)
	tx.unit.segments = tx.unit.segments[:0]
}

func (tx *Tx) releaseCollections() {
	for i := len(tx.collections) - 1; i >= 0; i-- {
		tx.collections[i].releaseRetain()
	}
}

func (c *collection) unavailableErr() error {
	state := c.state.Load()
	if state&collectionClosed != 0 {
		return rbierrors.ErrClosed
	}
	if state&collectionBroken != 0 || c.root.broken.Load() {
		return rbierrors.ErrBroken
	}
	return nil
}

func (tx *Tx) useRead() error {
	if tx.done {
		return rbierrors.ErrTxDone
	}
	if tx.kind != txKindRead {
		return rbierrors.ErrWrongTx
	}
	return nil
}

func (tx *Tx) bindRead(c *collection) error {
	if err := tx.useRead(); err != nil {
		return err
	}
	if tx.root != nil {
		if tx.root != c.root {
			return rbierrors.ErrStoreMismatch
		}
		return nil
	}
	if err := c.unavailableErr(); err != nil {
		return err
	}
	for {
		// The Bolt read transaction fixes the bucket sequence first; the root
		// generation must match it or data and index snapshots would diverge.
		boltTx, err := c.root.bolt.Begin(false)
		if err != nil {
			return fmt.Errorf("tx error: %w", err)
		}
		meta := boltTx.Bucket(c.root.metaBucket)
		if meta == nil {
			_ = boltTx.Rollback()
			return fmt.Errorf("root metadata bucket does not exist")
		}
		epoch := meta.Sequence()
		gen, pin, ok := c.root.registry.pinByEpoch(epoch)
		if ok {
			tx.root = c.root
			tx.boltTx = boltTx
			tx.epoch = epoch
			tx.gen = gen
			tx.pin = pin
			return nil
		}
		_ = boltTx.Rollback()
		if err = c.unavailableErr(); err != nil {
			return err
		}
	}
}

func (tx *Tx) collectionReadState(p *collection) (*collectionReadState, error) {
	if tx.kind == txKindRead {
		for i := range tx.collections {
			if tx.collections[i] == p {
				return tx.readBindings[i].state, nil
			}
		}
	}
	if err := tx.bindRead(p); err != nil {
		return nil, err
	}
	if int(p.ordinal) >= len(tx.gen.entries) {
		return nil, rbierrors.ErrCollectionNotVisible
	}
	entry := tx.gen.entries[p.ordinal]
	if entry.collection != p || entry.read == nil {
		return nil, rbierrors.ErrCollectionNotVisible
	}
	tx.collections = append(tx.collections, p)
	tx.readBindings = append(tx.readBindings, txReadBinding{state: entry.read})
	return entry.read, nil
}

func (tx *Tx) collectionBucket(c *collection) (*bbolt.Bucket, *collectionReadState, error) {
	if tx.kind == txKindRead {
		for i := range tx.collections {
			if tx.collections[i] == c {
				bind := tx.readBindings[i]
				if bind.bucket != nil {
					return bind.bucket, bind.state, nil
				}
				bucket := tx.boltTx.Bucket(c.dataBucket)
				if bucket == nil {
					return nil, nil, fmt.Errorf("bucket does not exist")
				}
				// dataSeq catches bucket recreation under the same root epoch binding.
				if bind.state.dataSeq != bucket.Sequence() {
					return nil, nil, errGenerationUnavailable
				}
				bind.bucket = bucket
				tx.readBindings[i] = bind
				return bucket, bind.state, nil
			}
		}
	}
	idx := -1
	if tx.kind == txKindRead {
		idx = len(tx.collections)
	}
	read, err := tx.collectionReadState(c)
	if err != nil {
		return nil, nil, err
	}
	bucket := tx.boltTx.Bucket(c.dataBucket)
	if bucket == nil {
		return nil, nil, fmt.Errorf("bucket does not exist")
	}
	// dataSeq catches bucket recreation under the same root epoch binding.
	if read.dataSeq != bucket.Sequence() {
		return nil, nil, errGenerationUnavailable
	}
	if idx >= 0 {
		tx.readBindings[idx].bucket = bucket
	}
	return bucket, read, nil
}

func (tx *Tx) readCollectionSnapshot(c *collection) (*snapshot.View, error) {
	state, err := tx.collectionReadState(c)
	if err != nil {
		return nil, err
	}
	if state.snap == nil {
		return nil, rbierrors.ErrNoIndex
	}
	return state.snap, nil
}

func (tx *Tx) useIndexOnly() error {
	if tx.done {
		return rbierrors.ErrTxDone
	}
	if tx.kind != txKindIndex {
		return rbierrors.ErrWrongTx
	}
	return nil
}

func (tx *Tx) bindIndex(c *collection) error {
	if err := tx.useIndexOnly(); err != nil {
		return err
	}
	if tx.root != nil {
		if tx.root != c.root {
			return rbierrors.ErrStoreMismatch
		}
		return nil
	}
	if err := c.unavailableErr(); err != nil {
		return err
	}
	return tx.bindIndexRoot(c.root)
}

func (tx *Tx) bindIndexRoot(r *rootStore) error {
	if err := tx.useIndexOnly(); err != nil {
		return err
	}
	if tx.root != nil {
		if tx.root != r {
			return rbierrors.ErrStoreMismatch
		}
		return nil
	}
	gen, epoch, pin := r.registry.pinCurrent()
	if gen == nil || pin.ref == nil {
		return errGenerationUnavailable
	}
	tx.root = r
	tx.epoch = epoch
	tx.gen = gen
	tx.pin = pin
	return nil
}

func (tx *Tx) collectionSnapshot(c *collection) (*snapshot.View, error) {
	if tx.kind == txKindRead {
		return tx.readCollectionSnapshot(c)
	}
	if err := tx.bindIndex(c); err != nil {
		return nil, err
	}
	return tx.boundCollectionSnapshot(c)
}

func (tx *Tx) closedCollectionSnapshot(c *collection) (*snapshot.View, error) {
	if tx.done {
		return nil, rbierrors.ErrTxDone
	}
	if c.root.broken.Load() {
		return nil, rbierrors.ErrBroken
	}
	if err := tx.bindIndexRoot(c.root); err != nil {
		return nil, err
	}
	return tx.boundCollectionSnapshot(c)
}

func (tx *Tx) boundCollectionSnapshot(c *collection) (*snapshot.View, error) {
	if int(c.ordinal) >= len(tx.gen.entries) {
		return nil, rbierrors.ErrCollectionNotVisible
	}
	entry := tx.gen.entries[c.ordinal]
	if entry.collection != c || entry.read == nil {
		return nil, rbierrors.ErrCollectionNotVisible
	}
	if entry.read.snap == nil {
		return nil, rbierrors.ErrNoIndex
	}
	return entry.read.snap, nil
}
