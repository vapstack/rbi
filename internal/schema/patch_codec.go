package schema

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

type PatchOperation struct {
	scratch *codecScratch
}

func (patch *PatchRuntime) BeginOperation() PatchOperation {
	if patch.usesCodecScratch {
		return PatchOperation{scratch: codecScratchPool.Get()}
	}
	return PatchOperation{}
}

func (patch *PatchRuntime) EndOperation(op *PatchOperation) {
	if op.scratch != nil {
		codecScratchPool.Put(op.scratch)
		op.scratch = nil
	}
}

func (patch *PatchRuntime) Value(acc *PatchFieldAccessor, oldRoot, newRoot unsafe.Pointer, compare bool, op *PatchOperation) (any, bool, error) {
	return acc.value(oldRoot, newRoot, compare, op.scratch)
}

type patchCodecAccessor struct {
	typ    reflect.Type
	offset uintptr
	path   string
	exact  *codecLeaf
	node   *patchCodecNode
	clone  CloneRuntime
}

type patchCodecNode struct {
	typ        reflect.Type
	leaf       *codecLeaf
	elem       *patchCodecNode
	fields     []patchCodecField
	mapType    unsafe.Pointer
	fastString bool
}

type patchCodecField struct {
	index int
	node  *patchCodecNode
}

func compilePatchCodecAccessor(t reflect.Type, offset uintptr, path string) (*patchCodecAccessor, error) {
	node, hasCodec, err := compilePatchCodecNode(t, path)
	if err != nil || !hasCodec {
		return nil, err
	}
	acc := &patchCodecAccessor{typ: t, offset: offset, path: path, node: node}
	if node.leaf != nil {
		acc.exact = node.leaf
	} else {
		compiler := cloneCompiler{stack: make(map[reflect.Type]bool, 8)}
		var plan clonePlan
		if err = compiler.buildPlan(t, 0, path, &plan); err != nil {
			return nil, err
		}
		acc.clone = CloneRuntime{
			spans:    plan.spans,
			steps:    plan.steps,
			errSteps: plan.errSteps,
		}
	}
	return acc, nil
}

func compilePatchCodecNode(t reflect.Type, path string) (*patchCodecNode, bool, error) {
	node := &patchCodecNode{typ: t}
	if t == nativeTimeType {
		return node, false, nil
	}
	leaf, err := resolveCodecLeaf(t, path)
	if err != nil {
		return nil, false, err
	}
	if leaf != nil {
		node.leaf = leaf
		return node, true, nil
	}

	switch t.Kind() {
	case reflect.Pointer, reflect.Array, reflect.Slice:
		var hasCodec bool
		node.elem, hasCodec, err = compilePatchCodecNode(t.Elem(), path+"[]")
		if err != nil {
			return nil, false, err
		}
		return node, hasCodec, nil

	case reflect.Map:
		node.mapType = (*ifaceWords)(unsafe.Pointer(&t)).data
		// The runtime string fast path returns the map slot without dereferencing
		// elements larger than its 128-byte inline limit.
		node.fastString = t.Key().Kind() == reflect.String && t.Elem().Size() <= 128
		var hasCodec bool
		node.elem, hasCodec, err = compilePatchCodecNode(t.Elem(), path+" value")
		if err != nil {
			return nil, false, err
		}
		return node, hasCodec, nil

	case reflect.Struct:
		hasCodec := false
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if !sf.IsExported() || fieldIgnoredByTags(sf) {
				continue
			}
			child, childHasCodec, childErr := compilePatchCodecNode(sf.Type, path+"."+sf.Name)
			if childErr != nil {
				return nil, false, childErr
			}
			node.fields = append(node.fields, patchCodecField{index: i, node: child})
			hasCodec = hasCodec || childHasCodec
		}
		return node, hasCodec, nil
	}
	return node, false, nil
}

func (a *patchCodecAccessor) value(oldRoot, newRoot unsafe.Pointer, compare bool, scratch *codecScratch) (any, bool, error) {
	newPtr := unsafe.Add(newRoot, a.offset)
	if a.exact != nil {
		newPayload, err := patchEncodeLeaf(a.exact, newPtr, &scratch.first)
		if err != nil {
			return nil, false, fmt.Errorf("field %s: %w", a.path, err)
		}
		if compare {
			oldPayload, oldErr := patchEncodeLeaf(a.exact, unsafe.Add(oldRoot, a.offset), &scratch.second)
			if oldErr != nil {
				return nil, false, fmt.Errorf("field %s: %w", a.path, oldErr)
			}
			if bytes.Equal(oldPayload, newPayload) {
				return nil, false, nil
			}
		}
		dst := reflect.New(a.typ)
		if err = a.exact.decode(newPayload, unsafe.Pointer(dst.Pointer())); err != nil {
			return nil, false, fmt.Errorf("field %s: %w", a.path, err)
		}
		return dst.Elem().Interface(), true, nil
	}

	newValue := reflect.NewAt(a.typ, newPtr).Elem()
	if compare {
		oldValue := reflect.NewAt(a.typ, unsafe.Add(oldRoot, a.offset)).Elem()
		equal, err := a.node.equal(oldValue, newValue, scratch)
		if err != nil {
			return nil, false, fmt.Errorf("field %s: %w", a.path, err)
		}
		if equal {
			return nil, false, nil
		}
	}
	dst := reflect.New(a.typ)
	if err := a.clone.cloneInto(newPtr, unsafe.Pointer(dst.Pointer()), scratch); err != nil {
		return nil, false, fmt.Errorf("field %s: %w", a.path, err)
	}
	return dst.Elem().Interface(), true, nil
}

func (a *patchCodecAccessor) copy(src unsafe.Pointer, scratch *codecScratch) (any, error) {
	if a.exact != nil {
		payload, err := patchEncodeLeaf(a.exact, src, &scratch.first)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", a.path, err)
		}
		dst := reflect.New(a.typ)
		if err = a.exact.decode(payload, unsafe.Pointer(dst.Pointer())); err != nil {
			return nil, fmt.Errorf("field %s: %w", a.path, err)
		}
		return dst.Elem().Interface(), nil
	}
	dst := reflect.New(a.typ)
	if err := a.clone.cloneInto(src, unsafe.Pointer(dst.Pointer()), scratch); err != nil {
		return nil, fmt.Errorf("field %s: %w", a.path, err)
	}
	return dst.Elem().Interface(), nil
}

func patchEncodeLeaf(leaf *codecLeaf, value unsafe.Pointer, buf *[]byte) ([]byte, error) {
	*buf = (*buf)[:0]
	payload, err := leaf.encode(value, *buf)
	*buf = payload
	return payload, err
}

func (n *patchCodecNode) equal(lhs, rhs reflect.Value, scratch *codecScratch) (bool, error) {
	if n.leaf != nil {
		leftPayload, err := patchEncodeLeaf(n.leaf, unsafe.Pointer(lhs.UnsafeAddr()), &scratch.first)
		if err != nil {
			return false, err
		}
		rightPayload, err := patchEncodeLeaf(n.leaf, unsafe.Pointer(rhs.UnsafeAddr()), &scratch.second)
		if err != nil {
			return false, err
		}
		return bytes.Equal(leftPayload, rightPayload), nil
	}
	if n.typ == nativeTimeType {
		return lhs.Interface().(time.Time).Equal(rhs.Interface().(time.Time)), nil
	}

	switch n.typ.Kind() {

	case reflect.Bool:
		return lhs.Bool() == rhs.Bool(), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return lhs.Int() == rhs.Int(), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return lhs.Uint() == rhs.Uint(), nil

	case reflect.String:
		return lhs.String() == rhs.String(), nil

	case reflect.Float32, reflect.Float64:
		return floatsEqualForIndex(lhs.Float(), rhs.Float()), nil

	case reflect.Array:
		for i := 0; i < lhs.Len(); i++ {
			equal, err := n.elem.equal(lhs.Index(i), rhs.Index(i), scratch)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil

	case reflect.Pointer:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil(), nil
		}
		return n.elem.equal(lhs.Elem(), rhs.Elem(), scratch)

	case reflect.Slice:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil(), nil
		}
		if lhs.Len() != rhs.Len() {
			return false, nil
		}
		for i := 0; i < lhs.Len(); i++ {
			equal, err := n.elem.equal(lhs.Index(i), rhs.Index(i), scratch)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil

	case reflect.Map:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil(), nil
		}
		if lhs.Len() != rhs.Len() {
			return false, nil
		}
		elemType := n.typ.Elem()
		key := reflect.New(n.typ.Key()).Elem()
		left := reflect.New(elemType).Elem()
		right := reflect.New(elemType).Elem()
		var keyPtr unsafe.Pointer
		if !n.fastString {
			keyPtr = unsafe.Pointer(key.UnsafeAddr())
		}
		rhsMap := rhs.UnsafePointer()
		iter := lhs.MapRange()
		for iter.Next() {
			key.SetIterKey(iter)
			var mapped unsafe.Pointer
			if n.fastString {
				mapped = patchReflectMapAccessFastString(n.mapType, rhsMap, key.String())
			} else {
				mapped = patchReflectMapAccess(n.mapType, rhsMap, keyPtr)
			}
			if mapped == nil {
				return false, nil
			}
			left.SetIterValue(iter)
			right.Set(reflect.NewAt(elemType, mapped).Elem())
			equal, err := n.elem.equal(left, right, scratch)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil

	case reflect.Struct:
		for i := range n.fields {
			field := &n.fields[i]
			equal, err := field.node.equal(lhs.Field(field.index), rhs.Field(field.index), scratch)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil

	default:
		return false, fmt.Errorf("unsupported patch comparison type %s", n.typ)
	}
}

// patchReflectMapAccess returns element storage so map values can be copied into
// reusable addressable memory without reflect.MapIndex allocating a result.
//
//go:noescape
//go:linkname patchReflectMapAccess reflect.mapaccess
func patchReflectMapAccess(t, m, key unsafe.Pointer) unsafe.Pointer

//go:noescape
//go:linkname patchReflectMapAccessFastString reflect.mapaccess_faststr
func patchReflectMapAccessFastString(t, m unsafe.Pointer, key string) unsafe.Pointer
