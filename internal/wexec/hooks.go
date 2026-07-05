package wexec

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

func runOnChangeHooks(tx unsafe.Pointer, depth uint8, id keycodec.DataKey, oldVal, newVal unsafe.Pointer, hooks []OnChangeHook) error {
	for _, fn := range hooks {
		if err := fn(tx, depth, id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}
