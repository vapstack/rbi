package wexec

import (
	"bytes"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

type (
	BeforeProcessHook func(keycodec.DataKey, unsafe.Pointer) error
	BeforeStoreHook   func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error
	BeforeCommitHook  func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error
	CloneFunc         func(keycodec.DataKey, unsafe.Pointer) (unsafe.Pointer, error)
)

type RecordOps struct {
	Encode        func(unsafe.Pointer, *bytes.Buffer) error
	Decode        func([]byte) (unsafe.Pointer, error)
	Release       func(unsafe.Pointer)
	ValidateIndex func(unsafe.Pointer) error
}

type SnapshotOps struct {
	Manager     *snapshot.Registry
	Schema      *schema.Schema
	CacheConfig snapshot.CacheConfig
	PatchFields map[string]*schema.Field
	StrKeyIndex bool
}
