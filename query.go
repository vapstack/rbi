package rbi

import (
	"fmt"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

// Query evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.engine == nil {
		return nil, ErrNoIndex
	}
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	prepared, err := qir.PrepareQuery(q, db.engine.schema.IndexedByName)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	viewQ := qir.NewShape(prepared)

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		return nil, err
	}
	defer db.engine.snapshot.Unpin(seq, ref)
	defer rollback(tx)

	return db.queryRecords(tx, snap, &viewQ)
}

func (db *DB[K, V]) beginQueryTxSnapshot() (*bbolt.Tx, *snapshot.View, uint64, *snapshot.Ref, error) {
	// Hold the registry read lock across Begin(false) -> Sequence() -> pin so a
	// writer cannot publish/retire away the exact snapshot needed by this read tx
	// in the gap between opening the tx and pinning its sequence-aligned snapshot.
	guard := db.engine.snapshot.LockPin()
	tx, err := db.bolt.Begin(false)
	if err != nil {
		guard.Unlock()
		return nil, nil, 0, nil, fmt.Errorf("tx error: %w", err)
	}

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		guard.Unlock()
		_ = tx.Rollback()
		return nil, nil, 0, nil, fmt.Errorf("bucket does not exist")
	}

	seq := bucket.Sequence()
	snap, ref, ok := guard.PinBySeq(seq)
	if !ok {
		guard.Unlock()
		_ = tx.Rollback()
		if err = db.unavailableErr(); err != nil {
			return nil, nil, 0, nil, err
		}
		return nil, nil, 0, nil, fmt.Errorf("snapshot sequence %d is not available", seq)
	}
	guard.Unlock()
	return tx, snap, seq, ref, nil
}

func (db *DB[K, V]) queryRecords(tx *bbolt.Tx, snap *snapshot.View, q *qir.Shape) ([]*V, error) {
	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)

	if !db.engine.exec.TraceSamplingEnabled() {
		if empty, err := view.TryQueryEmptyOnSnapshot(q); empty || err != nil {
			return nil, err
		}
	}

	ids, err := view.Query(q, true, false)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	values, err := db.batchGetTxCompactByIdx(tx, snap, ids)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (db *DB[K, V]) batchGetTxCompactByIdx(tx *bbolt.Tx, snap *snapshot.View, ids []uint64) ([]*V, error) {
	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}
	if len(ids) == 0 {
		return nil, nil
	}

	out := make([]*V, 0, len(ids))
	if db.strKey {
		strmapView := snap.StrMap
		lookup := strmapView.Lookup()
		for _, idx := range ids {
			s, ok := lookup.String(idx)
			if !ok {
				panic("rbi: no string key associated with snapshot idx")
			}
			v := bucket.Get(keycodec.StringBytes(s))
			if v == nil {
				continue
			}
			value, err := db.decode(v)
			if err != nil {
				return out, fmt.Errorf("decode: %w", err)
			}
			out = append(out, value)
		}
		return out, nil
	}

	var key [8]byte
	for _, idx := range ids {
		v := bucket.Get(keycodec.U64BytesWithBuf(idx, &key))
		if v == nil {
			continue
		}
		value, err := db.decode(v)
		if err != nil {
			return out, fmt.Errorf("decode: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.engine == nil {
		return nil, ErrNoIndex
	}
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	prepared, err := qir.PrepareQuery(q, db.engine.schema.IndexedByName)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	viewQ := qir.NewShape(prepared)

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)

	ids, err := view.Query(&viewQ, true, false)
	if err != nil {
		return nil, err
	}
	return db.queryKeysFromIDs(snap, ids), nil
}

func (db *DB[K, V]) queryKeysFromIDs(snap *snapshot.View, ids []uint64) []K {
	if len(ids) == 0 {
		return nil
	}
	if !db.strKey {
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids))
	}
	out := make([]K, len(ids))
	strmapView := snap.StrMap
	lookup := strmapView.Lookup()
	for i, idx := range ids {
		s, ok := lookup.String(idx)
		if !ok {
			panic("rbi: no string key associated with snapshot idx")
		}
		out[i] = *(*K)(unsafe.Pointer(&s))
	}
	return out
}

type queryEngine struct {
	snapshot               *snapshot.Manager
	schema                 *schema.Runtime
	index                  []indexdata.FieldStorage
	nilIndex               []indexdata.FieldStorage
	lenIndex               []indexdata.FieldStorage
	lenZeroComplement      []bool
	measure                []indexdata.MeasureStorage
	universe               posting.List
	lenIndexLoaded         bool
	matPredCacheMaxEntries int
	matPredCacheMaxCard    uint64

	exec *qexec.Runtime
}

func (qe *queryEngine) snapshotCacheConfig() snapshot.CacheConfig {
	return snapshot.CacheConfig{
		MatPredMaxEntries: qe.matPredCacheMaxEntries,
		MatPredMaxCard:    qe.matPredCacheMaxCard,
	}
}

func (qe *queryEngine) installViewNoLock(s *snapshot.View) {
	qe.index = s.Index
	qe.nilIndex = s.NilIndex
	qe.lenIndex = s.LenIndex
	qe.lenZeroComplement = s.LenZeroComplement
	qe.measure = s.Measure
	qe.universe = s.Universe
	qe.snapshot.Publish(s)
}
