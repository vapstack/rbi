package wexec

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

func runBeforeProcessHooks(id keycodec.DataKey, newVal unsafe.Pointer, hooks []BeforeProcessHook) error {
	for _, fn := range hooks {
		if err := fn(id, newVal); err != nil {
			return err
		}
	}
	return nil
}

func runBeforeStoreHooks(id keycodec.DataKey, oldVal, newVal unsafe.Pointer, hooks []BeforeStoreHook) error {
	for _, fn := range hooks {
		if err := fn(id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}

func runBeforeCommitHooks(tx *bbolt.Tx, id keycodec.DataKey, oldVal, newVal unsafe.Pointer, hooks []BeforeCommitHook) error {
	for _, fn := range hooks {
		if err := fn(tx, id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}
