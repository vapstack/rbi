package rbi

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/vapstack/monotime"
	"github.com/vapstack/rbi/internal/engine"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

var rootMetadataBucketName = []byte(".rbi")

const (
	rootCollectionUIDKeyKind byte = 0x01
	rootAutoUintKeyKind      byte = 0x02
	rootAutoStringKeyKind    byte = 0x03
)

var EnableStoreStats bool

const (
	collectionClosed uint32 = 1 << iota
	collectionBroken
)

type rootStore struct {
	bolt       *bbolt.DB
	metaBucket []byte

	registry     rootRegistry
	generationMu sync.Mutex
	scheduler    *writeScheduler
	statsEnabled bool
	autoMark     uint64

	mu                  sync.Mutex
	collections         map[string]*collection
	nextOrdinal         uint32
	collectionHighWater uint32

	broken      atomic.Bool
	reapPending atomic.Bool
}

type collection struct {
	root    *rootStore
	ordinal uint32

	dataBucket   []byte
	strmapBucket []byte
	strKey       bool

	options  *Options
	logger   *log.Logger
	schema   *schema.Schema
	index    *engine.Index
	executor *wexec.Executor

	state      atomic.Uint32
	retained   int64
	retainMu   sync.Mutex
	retainCond *sync.Cond

	boltPath string
	rbiFile  string
	rbiUID   [persist.UIDLen]byte

	autoMetaKey []byte
	autoMark    uint64
	autoUint    atomic.Uint64
	autoUUID    *monotime.GenUUID
}

var (
	rootRegistryMu sync.Mutex
	rootsByBolt    = make(map[*bbolt.DB]*rootStore)
)

func reserveCollection(bolt *bbolt.DB, dbPath string, bucket string, logger *log.Logger) (*rootStore, *collection, error) {
	rootRegistryMu.Lock()
	defer rootRegistryMu.Unlock()

	r := rootsByBolt[bolt]
	if r == nil {
		r = &rootStore{
			bolt:         bolt,
			metaBucket:   rootMetadataBucketName,
			scheduler:    newRootScheduler(EnableStoreStats),
			statsEnabled: EnableStoreStats,
			collections:  make(map[string]*collection),
		}
		rootsByBolt[bolt] = r
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.collections[bucket]; exists {
		return nil, nil, fmt.Errorf("rbi is already open for \"%v\" at %v", bucket, dbPath)
	}
	if r.broken.Load() {
		return nil, nil, rbierrors.ErrBroken
	}

	ordinal := r.nextOrdinal
	r.nextOrdinal++
	c := &collection{
		root:       r,
		ordinal:    ordinal,
		dataBucket: []byte(bucket),
		logger:     logger,
	}
	c.retainCond = sync.NewCond(&c.retainMu)
	r.collections[bucket] = c
	r.reapPending.Store(false)
	if r.collectionHighWater < r.nextOrdinal {
		r.collectionHighWater = r.nextOrdinal
	}
	return r, c, nil
}

func unreserveCollection(p *collection) {
	r := p.root
	rootRegistryMu.Lock()
	defer rootRegistryMu.Unlock()

	r.mu.Lock()
	if r.collections[string(p.dataBucket)] == p {
		delete(r.collections, string(p.dataBucket))
	}
	open := len(r.collections)
	r.mu.Unlock()

	tryReapRootLocked(r, open)
}

func tryReapRoot(r *rootStore) {
	rootRegistryMu.Lock()
	defer rootRegistryMu.Unlock()

	r.mu.Lock()
	open := len(r.collections)
	r.mu.Unlock()

	tryReapRootLocked(r, open)
}

func tryReapRootLocked(r *rootStore, open int) {
	if open != 0 || rootsByBolt[r.bolt] != r {
		return
	}
	if r.registry.reap() {
		r.reapPending.Store(false)
		delete(rootsByBolt, r.bolt)
		return
	}
	r.reapPending.Store(true)
}

func (c *collection) retain() error {
	c.retainMu.Lock()
	state := c.state.Load()
	if state&collectionClosed != 0 {
		c.retainMu.Unlock()
		return rbierrors.ErrClosed
	}
	if state&collectionBroken != 0 {
		c.retainMu.Unlock()
		return rbierrors.ErrBroken
	}
	c.retained++
	c.retainMu.Unlock()
	return nil
}

func (c *collection) releaseRetain() {
	c.retainMu.Lock()
	c.retained--
	if c.retained == 0 {
		c.retainCond.Broadcast()
	}
	c.retainMu.Unlock()
}

func (c *collection) beginClose() bool {
	c.retainMu.Lock()
	if c.state.Or(collectionClosed)&collectionClosed != 0 {
		c.retainMu.Unlock()
		return false
	}
	for c.retained != 0 {
		c.retainCond.Wait()
	}
	c.retainMu.Unlock()
	return true
}

func (r *rootStore) ensureRegistryCurrent() error {
	if r.registry.current.Load() != nil {
		return nil
	}

	tx, err := r.bolt.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	meta := tx.Bucket(r.metaBucket)
	if meta == nil {
		return fmt.Errorf("root metadata bucket %q does not exist", r.metaBucket)
	}
	epoch := meta.Sequence()
	r.registry.init(epoch)
	return nil
}

func publishCollectionRegistration(r *rootStore, p *collection) (readStateOwned bool, err error) {
	var (
		epoch   uint64
		dataSeq uint64
		gen     *rootGeneration
	)
	tx, err := r.bolt.Begin(true)
	if err != nil {
		return false, err
	}
	defer rollback(tx)

	meta := tx.Bucket(r.metaBucket)
	if meta == nil {
		return false, fmt.Errorf("root metadata bucket %q does not exist", r.metaBucket)
	}
	epoch, err = meta.NextSequence()
	if err != nil {
		return false, err
	}
	data := tx.Bucket(p.dataBucket)
	if data == nil {
		return false, fmt.Errorf("data bucket %q does not exist", p.dataBucket)
	}
	dataSeq = data.Sequence()
	var snap *snapshot.View
	if p.index != nil {
		snap = p.index.CurrentSnapshot()
	}

	r.generationMu.Lock()
	gen = r.registry.buildFromCurrent(epoch, int(p.ordinal)+1)
	gen.setEntry(p.ordinal, p, newCollectionReadState(p, dataSeq, snap))
	r.registry.stage(epoch, gen)
	readStateOwned = true
	if err = tx.Commit(); err != nil {
		r.registry.dropStaged(epoch)
		r.generationMu.Unlock()
		return true, err
	}
	if err = r.publishWrite(epoch); err != nil {
		r.generationMu.Unlock()
		return true, err
	}
	r.generationMu.Unlock()
	return true, nil
}

func publishCollectionClose(p *collection) {
	r := p.root
	r.generationMu.Lock()
	current := r.registry.current.Load()
	gen := r.registry.buildFromCurrent(current.epoch, len(current.entries))
	gen.clearEntry(p.ordinal)
	r.registry.publishMetadata(gen)
	r.generationMu.Unlock()
}

func (r *rootStore) advanceEpoch(tx *bbolt.Tx) (uint64, error) {
	meta := tx.Bucket(r.metaBucket)
	if meta == nil {
		return 0, fmt.Errorf("root metadata bucket %q does not exist", r.metaBucket)
	}
	return meta.NextSequence()
}

func (r *rootStore) publishWrite(epoch uint64) (err error) {
	defer r.finishPostCommit(&err, "post-commit root generation publish failed", "")
	r.registry.publish(epoch)
	return nil
}

func (r *rootStore) installCommitted(p *collection, snap *snapshot.View) (err error) {
	defer r.finishPostCommit(&err, "post-commit collection snapshot install panicked", "post-commit collection snapshot install failed")
	return p.index.InstallCommittedSnapshot(snap)
}

func (r *rootStore) finishPostCommit(err *error, panicReason string, errorReason string) {
	if v := recover(); v != nil {
		r.markBroken(panicReason, v)
		*err = rbierrors.ErrBroken
		return
	}
	if errorReason != "" && *err != nil {
		r.markBroken(errorReason, *err)
		*err = rbierrors.ErrBroken
	}
}

func (r *rootStore) markBroken(reason string, cause any) {
	type brokenCollection struct {
		logger *log.Logger
		index  *engine.Index
	}

	first := r.broken.CompareAndSwap(false, true)

	r.mu.Lock()
	var collections []brokenCollection
	if first {
		collections = make([]brokenCollection, 0, len(r.collections))
	}
	for _, p := range r.collections {
		p.state.Or(collectionBroken)
		if first {
			collections = append(collections, brokenCollection{logger: p.logger, index: p.index})
		}
	}
	r.mu.Unlock()

	if !first {
		return
	}
	for _, p := range collections {
		p.logger.Printf("rbi: root entered broken state: %s: %v", reason, cause)
		if p.index != nil {
			p.index.StopAnalyzeLoop()
		}
	}
}
