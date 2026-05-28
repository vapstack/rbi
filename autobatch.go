package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/schema"
)

func patchItemsForWrite(fields []Field) []schema.PatchItem {
	// Field and schema.PatchItem are layout-identical; wexec copies this view
	// into request-owned storage immediately.
	return unsafe.Slice((*schema.PatchItem)(unsafe.SliceData(fields)), len(fields))
}
