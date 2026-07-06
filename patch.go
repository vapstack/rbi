package rbi

import (
	"fmt"
	"slices"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/schema"
)

// PatchOption controls MakePatch behaviour.
type PatchOption uint8

const (
	// PatchJSON makes MakePatch emit json tag names when present.
	// Fields without a json tag fall back to their Go struct field name only
	// when that name is an unambiguous patch identifier for the field.
	// A changed field with `json:"-"` or without a safe JSON name makes
	// MakePatch return an error instead of silently omitting the change.
	PatchJSON PatchOption = 1 << iota
)

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name uses the db tag when present.
// Fields without a db tag use Go struct field name when that name is
// an unambiguous patch identifier for the field. If a modified field cannot
// be represented by a safe patch name, MakePatch returns an error.
//
// When PatchJSON is passed, Name uses the json tag when present.
// Fields without an explicit json name use their Go struct field name
// if that name is an unambiguous patch identifier for the field.
// If a modified field cannot be represented by a safe JSON patch name,
// including fields tagged json:"-", MakePatch returns an error.
//
// Values are copied from newVal. Unexported fields are ignored.
//
// If newVal is nil, it returns an empty slice.
func (c *Collection[K, V]) MakePatch(oldVal, newVal *V, opts ...PatchOption) ([]Field, error) {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return c.makePatch(oldVal, newVal, nil, useJSON)
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
// On error, returned slice is reset to length 0.
func (c *Collection[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field, opts ...PatchOption) ([]Field, error) {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return c.makePatch(oldVal, newVal, dst, useJSON)
}

type patchScratch struct {
	seen []bool
}

var patchScratchPool = pooled.Pointers[patchScratch]{
	Cleanup: func(scratch *patchScratch) {
		clear(scratch.seen[:cap(scratch.seen)])
		scratch.seen = scratch.seen[:0]
	},
}

func (c *Collection[K, V]) makePatch(oldVal, newVal *V, target []Field, useJSON bool) ([]Field, error) {
	target = target[:0]

	if newVal == nil {
		return target, nil
	}

	patchAccess := c.schema.Patch.Access
	if c.schema.Patch.Flat {
		newPtr := unsafe.Pointer(newVal)
		oldPtr := unsafe.Pointer(nil)
		if oldVal != nil {
			oldPtr = unsafe.Pointer(oldVal)
		}

		for i := range patchAccess {
			patchAcc := &patchAccess[i]
			if oldVal != nil {
				if patchAcc.ValueEqual(oldPtr, newPtr) {
					continue
				}
			}

			name := patchFieldName(patchAcc.Field, useJSON)
			if useJSON {
				if name == "" {
					return target[:0], fmt.Errorf("field %v with db name %q cannot be emitted with PatchJSON: add an explicit non-empty json tag", patchAcc.Field.Name, patchAcc.Field.DBName)
				}
			} else if name == "" {
				return target[:0], fmt.Errorf("field %v cannot be emitted by MakePatch: add an explicit non-empty db tag", patchAcc.Field.Name)
			}

			target = append(target, Field{
				Name:  name,
				Value: patchAcc.CopyValue(newPtr),
			})
		}

		return target, nil
	}

	scratch := patchScratchPool.Get()
	scratch.seen = slices.Grow(scratch.seen[:0], len(patchAccess))[:len(patchAccess)]
	defer patchScratchPool.Put(scratch)

	newPtr := unsafe.Pointer(newVal)
	oldPtr := unsafe.Pointer(nil)
	if oldVal != nil {
		oldPtr = unsafe.Pointer(oldVal)
	}

	if c.index != nil {
		for _, acc := range c.schema.Indexed {
			if oldVal != nil && !acc.Modified(oldPtr, newPtr) {
				continue
			}
			if acc.PatchOrdinal < 0 {
				continue
			}
			ordinal := patchCoverOrdinal(patchAccess, acc.PatchOrdinal, useJSON)
			if scratch.seen[ordinal] {
				continue
			}
			patchAcc := patchAccess[ordinal]
			name := patchFieldName(patchAcc.Field, useJSON)
			if useJSON {
				if name == "" {
					return target[:0], fmt.Errorf("field %v with db name %q cannot be emitted with PatchJSON: add an explicit non-empty json tag", patchAcc.Field.Name, patchAcc.Field.DBName)
				}
			} else if name == "" {
				return target[:0], fmt.Errorf("field %v cannot be emitted by MakePatch: add an explicit non-empty db tag", patchAcc.Field.Name)
			}
			value := patchAcc.CopyValue(newPtr)
			markPatchSubtreeSeen(scratch.seen, patchAccess, ordinal)
			target = append(target, Field{
				Name:  name,
				Value: value,
			})
		}
	}

	for ordinal, patchAcc := range patchAccess {
		if scratch.seen[ordinal] {
			continue
		}

		if oldVal != nil {
			if patchAcc.ValueEqual(oldPtr, newPtr) {
				continue
			}
		}

		name := patchFieldName(patchAcc.Field, useJSON)
		if useJSON {
			if name == "" {
				return target[:0], fmt.Errorf("field %v with db name %q cannot be emitted with PatchJSON: add an explicit non-empty json tag", patchAcc.Field.Name, patchAcc.Field.DBName)
			}
		} else if name == "" {
			return target[:0], fmt.Errorf("field %v cannot be emitted by MakePatch: add an explicit non-empty db tag", patchAcc.Field.Name)
		}

		target = append(target, Field{
			Name:  name,
			Value: patchAcc.CopyValue(newPtr),
		})
		markPatchSubtreeSeen(scratch.seen, patchAccess, ordinal)
	}

	return target, nil
}

func patchFieldName(f *schema.Field, useJSON bool) string {
	if useJSON {
		return f.JSONName
	}
	return f.DBName
}

func patchCoverOrdinal(access []schema.PatchFieldAccessor, ordinal int, useJSON bool) int {
	index := access[ordinal].Field.Index
	if len(index) == 1 {
		return ordinal
	}

	cover := ordinal
	first := index[0]
	for i := ordinal - 1; i >= 0; i-- {
		parent := access[i].Field.Index
		if parent[0] != first {
			break
		}
		if len(parent) >= len(index) || !slices.Equal(index[:len(parent)], parent) {
			continue
		}
		if patchFieldName(access[i].Field, useJSON) != "" {
			cover = i
		}
	}
	return cover
}

func markPatchSubtreeSeen(seen []bool, access []schema.PatchFieldAccessor, ordinal int) {
	seen[ordinal] = true
	parentIndex := access[ordinal].Field.Index
	for ordinal++; ordinal < len(access); ordinal++ {
		childIndex := access[ordinal].Field.Index
		if len(childIndex) <= len(parentIndex) || !slices.Equal(childIndex[:len(parentIndex)], parentIndex) {
			break
		}
		seen[ordinal] = true
	}
}

// patchItemsForWrite returns request-owned storage; callers must transfer it to
// wexec or release it with schema.ReleasePatchItemSlice.
func (c *Collection[K, V]) patchItemsForWrite(fields []Field, ignoreUnknown bool) ([]schema.PatchItem, error) {
	items := schema.GetPatchItemSlice(len(fields))[:0]
	for i := range fields {
		value, ok, err := c.schema.Patch.CopyItemValue(fields[i].Name, fields[i].Value, ignoreUnknown)
		if err != nil {
			schema.ReleasePatchItemSlice(items)
			return nil, err
		}
		if !ok {
			continue
		}
		items = append(items, schema.PatchItem{
			Name:  fields[i].Name,
			Value: value,
		})
	}
	return items, nil
}
