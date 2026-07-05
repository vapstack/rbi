package main

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi"
)

func stressReadGet[K ~string | ~uint64, V any](c *rbi.Collection[K, V], id K) (*V, error) {
	tx := rbi.BeginView()
	v, err := c.Get(tx, id)
	tx.Release()
	return v, err
}

func stressReadScanKeys[K ~string | ~uint64, V any](c *rbi.Collection[K, V], seek K, fn func(K) (bool, error)) error {
	tx := rbi.BeginView()
	defer tx.Release()
	return c.ScanKeys(tx, seek, fn)
}

func stressReadQuery[K ~string | ~uint64, V any](c *rbi.Collection[K, V], q *qx.QX) ([]*V, error) {
	tx := rbi.BeginView()
	defer tx.Release()
	return c.Query(tx, q)
}

func stressReadQueryKeys[K ~string | ~uint64, V any](c *rbi.Collection[K, V], q *qx.QX) ([]K, error) {
	tx := rbi.BeginView()
	defer tx.Release()
	return c.QueryKeys(tx, q)
}

func stressReadCount[K ~string | ~uint64, V any](c *rbi.Collection[K, V], exprs ...qx.Expr) (uint64, error) {
	tx := rbi.BeginIndexView()
	defer tx.Release()
	return c.Count(tx, exprs...)
}

func stressWriteSet[K ~string | ~uint64, V any](c *rbi.Collection[K, V], id K, val *V, opts ...rbi.ExecOption[K, V]) error {
	return rbi.Update(func(tx *rbi.Tx) error {
		return c.Set(tx, id, val, opts...)
	})
}

func stressWriteSets[K ~string | ~uint64, V any](c *rbi.Collection[K, V], ids []K, vals []*V, opts ...rbi.ExecOption[K, V]) error {
	return rbi.Update(func(tx *rbi.Tx) error {
		for i := range ids {
			if err := c.Set(tx, ids[i], vals[i], opts...); err != nil {
				return err
			}
		}
		return nil
	})
}

func stressWritePatch[K ~string | ~uint64, V any](c *rbi.Collection[K, V], id K, patch []rbi.Field, opts ...rbi.ExecOption[K, V]) error {
	return rbi.Update(func(tx *rbi.Tx) error {
		return c.Patch(tx, id, patch, opts...)
	})
}
