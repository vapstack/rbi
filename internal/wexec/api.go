package wexec

import (
	"bytes"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
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

type ErrorSet struct {
	UniqueViolation error
}

type SnapshotOps struct {
	Enabled     bool
	Manager     *snapshot.Manager
	Schema      *schema.Runtime
	CacheConfig func() snapshot.CacheConfig
	StrMap      *strmap.Mapper
	PatchFields map[string]*schema.Field
}

type IndexPublishOps struct {
	PublishCommitted func(uint64, string, *snapshot.View) error
}
