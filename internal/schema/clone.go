package schema

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"
	"unsafe"
)

type CloneRuntime struct {
	spans    []byteSpan
	steps    []cloneStep
	errSteps []cloneErrStep
}

type byteSpan struct {
	off  uintptr
	size uintptr
}

type (
	cloneStep    func(src unsafe.Pointer, dst unsafe.Pointer)
	cloneErrStep func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error
)

var errInvalidRecordType = errors.New("invalid record type")

func (c *CloneRuntime) CloneInto(src unsafe.Pointer, dst unsafe.Pointer) error {
	if len(c.errSteps) == 0 {
		for i := range c.spans {
			cloneSpan(c.spans[i], src, dst)
		}
		for i := range c.steps {
			c.steps[i](src, dst)
		}
		return nil
	}
	scratch := codecScratchPool.Get()
	err := c.cloneInto(src, dst, scratch)
	codecScratchPool.Put(scratch)
	return err
}

func (c *CloneRuntime) cloneInto(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
	for i := range c.spans {
		cloneSpan(c.spans[i], src, dst)
	}
	for i := range c.steps {
		c.steps[i](src, dst)
	}
	for i := range c.errSteps {
		if err := c.errSteps[i](src, dst, scratch); err != nil {
			return err
		}
	}
	return nil
}

func cloneSpan(span byteSpan, src unsafe.Pointer, dst unsafe.Pointer) {
	copy(
		unsafe.Slice((*byte)(unsafe.Add(dst, span.off)), span.size),
		unsafe.Slice((*byte)(unsafe.Add(src, span.off)), span.size),
	)
}

type clonePlan struct {
	spans    []byteSpan
	steps    []cloneStep
	errSteps []cloneErrStep
}

func (p *clonePlan) addErrStep(offset uintptr, step cloneErrStep) {
	p.errSteps = append(p.errSteps, func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
		return step(unsafe.Add(src, offset), unsafe.Add(dst, offset), scratch)
	})
}

func (p *clonePlan) addSpan(off uintptr, size uintptr) {
	if size == 0 {
		return
	}
	if len(p.spans) != 0 {
		prev := &p.spans[len(p.spans)-1]
		if prev.off+prev.size == off {
			prev.size += size
			return
		}
	}
	p.spans = append(p.spans, byteSpan{off: off, size: size})
}

func (p *clonePlan) addStep(offset uintptr, step cloneStep) {
	if step == nil {
		return
	}
	p.steps = append(p.steps, func(src unsafe.Pointer, dst unsafe.Pointer) {
		step(unsafe.Add(src, offset), unsafe.Add(dst, offset))
	})
}

type cloneCompiler struct {
	stack map[reflect.Type]bool
}

func compileClone(t reflect.Type) (CloneRuntime, error) {
	c := cloneCompiler{stack: make(map[reflect.Type]bool, 8)}
	if err := c.validateRecord(t, t.Name()); err != nil {
		return CloneRuntime{}, err
	}
	var plan clonePlan
	if err := c.buildStructPlan(t, 0, t.Name(), &plan); err != nil {
		return CloneRuntime{}, err
	}
	return CloneRuntime{
		spans:    plan.spans,
		steps:    plan.steps,
		errSteps: plan.errSteps,
	}, nil
}

func (c *cloneCompiler) validateRecord(t reflect.Type, path string) error {
	if c.stack[t] {
		return fmt.Errorf("%w: recursive record field %s", errInvalidRecordType, path)
	}
	c.stack[t] = true
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		if fieldIgnoredByTags(sf) {
			continue
		}
		if err := c.validateType(sf.Type, path+"."+sf.Name, false); err != nil {
			delete(c.stack, t)
			return err
		}
	}
	delete(c.stack, t)
	return nil
}

func (c *cloneCompiler) validateType(t reflect.Type, path string, key bool) error {
	if key {
		switch t.Kind() {

		case reflect.Bool, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return nil

		case reflect.Array:
			if t.Len() == 0 {
				return fmt.Errorf("%w: unsupported zero-length array field %s type %s", errInvalidRecordType, path, t)
			}
			return c.validateNested(t, path, func() error {
				return c.validateType(t.Elem(), path+"[]", true)
			})

		case reflect.Struct:
			return c.validateNested(t, path, func() error {
				for i := 0; i < t.NumField(); i++ {
					sf := t.Field(i)
					if !sf.IsExported() {
						return fmt.Errorf("%w: unsupported map key field %s.%s type %s", errInvalidRecordType, path, sf.Name, t)
					}
					if fieldIgnoredByTags(sf) {
						return fmt.Errorf("%w: unsupported ignored map key field %s.%s type %s", errInvalidRecordType, path, sf.Name, t)
					}
					if err := c.validateType(sf.Type, path+"."+sf.Name, true); err != nil {
						return err
					}
				}
				return nil
			})

		default:
			return fmt.Errorf("%w: unsupported map key field %s type %s", errInvalidRecordType, path, t)
		}
	}
	if t == nativeTimeType {
		return nil
	}
	leaf, err := resolveCodecLeaf(t, path)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidRecordType, err)
	}
	if leaf != nil {
		return nil
	}
	if isNativeTimeWrapperType(t) && (t.Kind() != reflect.Pointer || t.Name() != "") {
		return fmt.Errorf("%w: named time field %s type %s is not supported", errInvalidRecordType, path, t)
	}

	switch t.Kind() {

	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return nil

	case reflect.Uintptr, reflect.UnsafePointer, reflect.Interface, reflect.Func, reflect.Chan,
		reflect.Complex64, reflect.Complex128:
		return fmt.Errorf("%w: unsupported record field %s type %s", errInvalidRecordType, path, t)

	case reflect.Pointer:
		return c.validateNested(t, path, func() error {
			return c.validateType(t.Elem(), path, false)
		})

	case reflect.Slice:
		return c.validateNested(t, path, func() error {
			return c.validateType(t.Elem(), path+"[]", false)
		})

	case reflect.Array:
		if t.Len() == 0 {
			return fmt.Errorf("%w: unsupported zero-length array field %s type %s", errInvalidRecordType, path, t)
		}
		return c.validateNested(t, path, func() error {
			return c.validateType(t.Elem(), path+"[]", key)
		})

	case reflect.Map:
		return c.validateNested(t, path, func() error {
			if err := c.validateType(t.Key(), path+" key", true); err != nil {
				return err
			}
			return c.validateType(t.Elem(), path+" value", false)
		})

	case reflect.Struct:
		return c.validateNested(t, path, func() error {
			for i := 0; i < t.NumField(); i++ {
				sf := t.Field(i)
				if !sf.IsExported() {
					continue
				}
				if fieldIgnoredByTags(sf) {
					continue
				}
				if err := c.validateType(sf.Type, path+"."+sf.Name, false); err != nil {
					return err
				}
			}
			return nil
		})
	default:
		return fmt.Errorf("%w: unsupported record field %s type %s", errInvalidRecordType, path, t)
	}
}

func (c *cloneCompiler) validateNested(t reflect.Type, path string, fn func() error) error {
	if c.stack[t] {
		return fmt.Errorf("%w: recursive record field %s", errInvalidRecordType, path)
	}
	c.stack[t] = true
	err := fn()
	delete(c.stack, t)
	return err
}

func (c *cloneCompiler) buildPlan(t reflect.Type, offset uintptr, path string, plan *clonePlan) error {
	if t == nativeTimeType {
		plan.addStep(offset, valueCloneStep[time.Time]())
		return nil
	}
	leaf, err := resolveCodecLeaf(t, path)
	if err != nil {
		return err
	}
	if leaf != nil {
		plan.addErrStep(offset, codecLeafCloneStep(leaf))
		return nil
	}
	switch t.Kind() {

	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		plan.addSpan(offset, t.Size())
		return nil

	case reflect.String:
		if t == reflect.TypeFor[string]() {
			plan.addStep(offset, valueCloneStep[string]())
		} else {
			plan.addStep(offset, reflectValueCloneStep(t))
		}
		return nil

	case reflect.Struct:
		return c.buildStructPlan(t, offset, path, plan)

	case reflect.Array:
		if t.Len() == 0 {
			return nil
		}
		elem := t.Elem()
		elemPath := path + "[]"
		spanType := elem
		spanPath := elemPath
	LOOP:
		for {
			leaf, err = resolveCodecLeaf(spanType, spanPath)
			if err != nil {
				return err
			}
			if leaf != nil {
				break
			}
			switch spanType.Kind() {
			case reflect.Bool,
				reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				plan.addSpan(offset, t.Size())
				return nil
			case reflect.Array:
				spanType = spanType.Elem()
				spanPath += "[]"
			default:
				break LOOP
			}
		}
		size := elem.Size()
		for i := 0; i < t.Len(); i++ {
			if err := c.buildPlan(elem, offset+uintptr(i)*size, elemPath, plan); err != nil {
				return err
			}
		}
		return nil

	case reflect.Slice:
		elemLeaf, err := resolveCodecLeaf(t.Elem(), path+"[]")
		if err != nil {
			return err
		}
		if elemLeaf == nil {
			if step := makeScalarSliceCloneStep(t); step != nil {
				plan.addStep(offset, step)
				return nil
			}
		}
		elemStep, elemErrStep, err := c.buildValueSteps(t.Elem(), path+"[]")
		if err != nil {
			return err
		}
		if elemErrStep != nil {
			plan.addErrStep(offset, sliceCloneErrStep(t, elemErrStep))
		} else {
			plan.addStep(offset, sliceCloneStep(t, elemStep))
		}
		return nil

	case reflect.Pointer:
		elemLeaf, err := resolveCodecLeaf(t.Elem(), path)
		if err != nil {
			return err
		}
		if elemLeaf == nil {
			if step := makeScalarPointerCloneStep(t); step != nil {
				plan.addStep(offset, step)
				return nil
			}
		}
		elemStep, elemErrStep, err := c.buildValueSteps(t.Elem(), path)
		if err != nil {
			return err
		}
		if elemErrStep != nil {
			plan.addErrStep(offset, pointerCloneErrStep(t.Elem(), elemErrStep))
		} else {
			plan.addStep(offset, pointerCloneStep(t.Elem(), elemStep))
		}
		return nil

	case reflect.Map:
		valueLeaf, err := resolveCodecLeaf(t.Elem(), path+" value")
		if err != nil {
			return err
		}
		if valueLeaf == nil {
			if step := makeScalarMapCloneStep(t); step != nil {
				plan.addStep(offset, step)
				return nil
			}
		}
		valueStep, valueErrStep, err := c.buildValueSteps(t.Elem(), path+" value")
		if err != nil {
			return err
		}
		if valueErrStep != nil {
			plan.addErrStep(offset, mapCloneErrStep(t, valueErrStep))
		} else {
			plan.addStep(offset, mapCloneStep(t, valueStep))
		}
		return nil

	default:
		return fmt.Errorf("%w: unsupported record field %s type %s", errInvalidRecordType, path, t)
	}
}

func (c *cloneCompiler) buildStructPlan(t reflect.Type, offset uintptr, path string, plan *clonePlan) error {
	if c.stack[t] {
		return fmt.Errorf("%w: recursive record field %s", errInvalidRecordType, path)
	}
	c.stack[t] = true
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		if fieldIgnoredByTags(sf) {
			continue
		}
		if err := c.buildPlan(sf.Type, offset+sf.Offset, path+"."+sf.Name, plan); err != nil {
			delete(c.stack, t)
			return err
		}
	}
	delete(c.stack, t)
	return nil
}

func (c *cloneCompiler) buildValueSteps(t reflect.Type, path string) (cloneStep, cloneErrStep, error) {
	var plan clonePlan
	if err := c.buildPlan(t, 0, path, &plan); err != nil {
		return nil, nil, err
	}
	if len(plan.errSteps) != 0 {
		spans := plan.spans
		steps := plan.steps
		errSteps := plan.errSteps
		return nil, func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
			for i := range spans {
				cloneSpan(spans[i], src, dst)
			}
			for i := range steps {
				steps[i](src, dst)
			}
			for i := range errSteps {
				if err := errSteps[i](src, dst, scratch); err != nil {
					return err
				}
			}
			return nil
		}, nil
	}
	if len(plan.spans) == 0 && len(plan.steps) == 0 {
		return nil, nil, nil
	}
	spans := plan.spans
	steps := plan.steps
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		for i := range spans {
			cloneSpan(spans[i], src, dst)
		}
		for i := range steps {
			steps[i](src, dst)
		}
	}, nil, nil
}

func valueCloneStep[T any]() cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		*(*T)(dst) = *(*T)(src)
	}
}

func codecLeafCloneStep(leaf *codecLeaf) cloneErrStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
		payload, err := leaf.encode(src, scratch.first[:0])
		scratch.first = payload
		if err != nil {
			return fmt.Errorf("cloning field %s: %w", leaf.path, err)
		}
		reflect.NewAt(leaf.typ, dst).Elem().SetZero()
		if err = leaf.decode(payload, dst); err != nil {
			return fmt.Errorf("cloning field %s: %w", leaf.path, err)
		}
		return nil
	}
}

func reflectValueCloneStep(t reflect.Type) cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		reflect.NewAt(t, dst).Elem().Set(reflect.NewAt(t, src).Elem())
	}
}

func makeScalarPointerCloneStep(t reflect.Type) cloneStep {
	elem := t.Elem()
	if elem == nativeTimeType {
		return pointerScalarCloneStep[time.Time]()
	}
	switch elem.Kind() {
	case reflect.Bool:
		return pointerScalarCloneStep[bool]()
	case reflect.Int:
		return pointerScalarCloneStep[int]()
	case reflect.Int8:
		return pointerScalarCloneStep[int8]()
	case reflect.Int16:
		return pointerScalarCloneStep[int16]()
	case reflect.Int32:
		return pointerScalarCloneStep[int32]()
	case reflect.Int64:
		return pointerScalarCloneStep[int64]()
	case reflect.Uint:
		return pointerScalarCloneStep[uint]()
	case reflect.Uint8:
		return pointerScalarCloneStep[uint8]()
	case reflect.Uint16:
		return pointerScalarCloneStep[uint16]()
	case reflect.Uint32:
		return pointerScalarCloneStep[uint32]()
	case reflect.Uint64:
		return pointerScalarCloneStep[uint64]()
	case reflect.Float32:
		return pointerScalarCloneStep[float32]()
	case reflect.Float64:
		return pointerScalarCloneStep[float64]()
	case reflect.String:
		return pointerScalarCloneStep[string]()
	default:
		return nil
	}
}

func pointerScalarCloneStep[T any]() cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		s := *(*unsafe.Pointer)(src)
		d := (*unsafe.Pointer)(dst)
		if s == nil {
			*d = nil
			return
		}
		out := new(T)
		*out = *(*T)(s)
		*d = unsafe.Pointer(out)
	}
}

func makeScalarSliceCloneStep(t reflect.Type) cloneStep {
	elem := t.Elem()
	if elem == nativeTimeType {
		return scalarSliceCloneStep[time.Time]()
	}
	switch elem.Kind() {
	case reflect.Bool:
		return scalarSliceCloneStep[bool]()
	case reflect.Int:
		return scalarSliceCloneStep[int]()
	case reflect.Int8:
		return scalarSliceCloneStep[int8]()
	case reflect.Int16:
		return scalarSliceCloneStep[int16]()
	case reflect.Int32:
		return scalarSliceCloneStep[int32]()
	case reflect.Int64:
		return scalarSliceCloneStep[int64]()
	case reflect.Uint:
		return scalarSliceCloneStep[uint]()
	case reflect.Uint8:
		return scalarSliceCloneStep[uint8]()
	case reflect.Uint16:
		return scalarSliceCloneStep[uint16]()
	case reflect.Uint32:
		return scalarSliceCloneStep[uint32]()
	case reflect.Uint64:
		return scalarSliceCloneStep[uint64]()
	case reflect.Float32:
		return scalarSliceCloneStep[float32]()
	case reflect.Float64:
		return scalarSliceCloneStep[float64]()
	case reflect.String:
		return scalarSliceCloneStep[string]()
	default:
		return nil
	}
}

func scalarSliceCloneStep[T any]() cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		s := *(*[]T)(src)
		d := (*[]T)(dst)
		if s == nil {
			*d = nil
			return
		}
		if len(s) == 0 {
			*d = make([]T, 0)
			return
		}
		out := make([]T, len(s))
		copy(out, s)
		*d = out
	}
}

func makeScalarMapCloneStep(t reflect.Type) cloneStep {
	key := t.Key()
	switch key.Kind() {
	case reflect.Bool:
		return makeScalarMapValueCloneStep[bool](t.Elem())
	case reflect.Int:
		return makeScalarMapValueCloneStep[int](t.Elem())
	case reflect.Int8:
		return makeScalarMapValueCloneStep[int8](t.Elem())
	case reflect.Int16:
		return makeScalarMapValueCloneStep[int16](t.Elem())
	case reflect.Int32:
		return makeScalarMapValueCloneStep[int32](t.Elem())
	case reflect.Int64:
		return makeScalarMapValueCloneStep[int64](t.Elem())
	case reflect.Uint:
		return makeScalarMapValueCloneStep[uint](t.Elem())
	case reflect.Uint8:
		return makeScalarMapValueCloneStep[uint8](t.Elem())
	case reflect.Uint16:
		return makeScalarMapValueCloneStep[uint16](t.Elem())
	case reflect.Uint32:
		return makeScalarMapValueCloneStep[uint32](t.Elem())
	case reflect.Uint64:
		return makeScalarMapValueCloneStep[uint64](t.Elem())
	case reflect.String:
		return makeScalarMapValueCloneStep[string](t.Elem())
	default:
		return nil
	}
}

func makeScalarMapValueCloneStep[K comparable](value reflect.Type) cloneStep {
	if value == nativeTimeType {
		return scalarMapCloneStep[K, time.Time]()
	}
	switch value.Kind() {
	case reflect.Bool:
		return scalarMapCloneStep[K, bool]()
	case reflect.Int:
		return scalarMapCloneStep[K, int]()
	case reflect.Int8:
		return scalarMapCloneStep[K, int8]()
	case reflect.Int16:
		return scalarMapCloneStep[K, int16]()
	case reflect.Int32:
		return scalarMapCloneStep[K, int32]()
	case reflect.Int64:
		return scalarMapCloneStep[K, int64]()
	case reflect.Uint:
		return scalarMapCloneStep[K, uint]()
	case reflect.Uint8:
		return scalarMapCloneStep[K, uint8]()
	case reflect.Uint16:
		return scalarMapCloneStep[K, uint16]()
	case reflect.Uint32:
		return scalarMapCloneStep[K, uint32]()
	case reflect.Uint64:
		return scalarMapCloneStep[K, uint64]()
	case reflect.Float32:
		return scalarMapCloneStep[K, float32]()
	case reflect.Float64:
		return scalarMapCloneStep[K, float64]()
	case reflect.String:
		return scalarMapCloneStep[K, string]()
	default:
		return nil
	}
}

func scalarMapCloneStep[K comparable, V any]() cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		s := *(*map[K]V)(src)
		*(*map[K]V)(dst) = maps.Clone(s)
	}
}

func pointerCloneStep(elem reflect.Type, elemStep cloneStep) cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		s := *(*unsafe.Pointer)(src)
		d := (*unsafe.Pointer)(dst)
		if s == nil {
			*d = nil
			return
		}
		out := reflect.New(elem)
		ptr := unsafe.Pointer(out.Pointer())
		if elemStep != nil {
			elemStep(s, ptr)
		}
		*d = ptr
	}
}

func pointerCloneErrStep(elem reflect.Type, elemStep cloneErrStep) cloneErrStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
		s := *(*unsafe.Pointer)(src)
		if s == nil {
			*(*unsafe.Pointer)(dst) = nil
			return nil
		}
		out := reflect.New(elem)
		ptr := unsafe.Pointer(out.Pointer())
		if err := elemStep(s, ptr, scratch); err != nil {
			return err
		}
		*(*unsafe.Pointer)(dst) = ptr
		return nil
	}
}

func sliceCloneStep(t reflect.Type, elemStep cloneStep) cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		sv := reflect.NewAt(t, src).Elem()
		dv := reflect.NewAt(t, dst).Elem()
		if sv.IsNil() {
			dv.SetZero()
			return
		}
		out := reflect.MakeSlice(t, sv.Len(), sv.Len())
		if elemStep != nil {
			for i := 0; i < sv.Len(); i++ {
				elemStep(unsafe.Pointer(sv.Index(i).UnsafeAddr()), unsafe.Pointer(out.Index(i).UnsafeAddr()))
			}
		}
		dv.Set(out)
	}
}

func sliceCloneErrStep(t reflect.Type, elemStep cloneErrStep) cloneErrStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
		sv := reflect.NewAt(t, src).Elem()
		dv := reflect.NewAt(t, dst).Elem()
		if sv.IsNil() {
			dv.SetZero()
			return nil
		}
		out := reflect.MakeSlice(t, sv.Len(), sv.Len())
		for i := 0; i < sv.Len(); i++ {
			if err := elemStep(unsafe.Pointer(sv.Index(i).UnsafeAddr()), unsafe.Pointer(out.Index(i).UnsafeAddr()), scratch); err != nil {
				return err
			}
		}
		dv.Set(out)
		return nil
	}
}

func mapCloneStep(t reflect.Type, valueStep cloneStep) cloneStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer) {
		sv := reflect.NewAt(t, src).Elem()
		dv := reflect.NewAt(t, dst).Elem()
		if sv.IsNil() {
			dv.SetZero()
			return
		}
		out := reflect.MakeMapWithSize(t, sv.Len())
		iter := sv.MapRange()
		elem := t.Elem()
		if valueStep == nil {
			zero := reflect.Zero(elem)
			for iter.Next() {
				out.SetMapIndex(iter.Key(), zero)
			}
			dv.Set(out)
			return
		}
		srcTmp := reflect.New(elem).Elem()
		dstTmp := reflect.New(elem).Elem()
		srcPtr := unsafe.Pointer(srcTmp.UnsafeAddr())
		dstPtr := unsafe.Pointer(dstTmp.UnsafeAddr())
		for iter.Next() {
			srcTmp.Set(iter.Value())
			dstTmp.SetZero()
			valueStep(srcPtr, dstPtr)
			out.SetMapIndex(iter.Key(), dstTmp)
		}
		dv.Set(out)
	}
}

func mapCloneErrStep(t reflect.Type, valueStep cloneErrStep) cloneErrStep {
	return func(src unsafe.Pointer, dst unsafe.Pointer, scratch *codecScratch) error {
		sv := reflect.NewAt(t, src).Elem()
		dv := reflect.NewAt(t, dst).Elem()
		if sv.IsNil() {
			dv.SetZero()
			return nil
		}
		out := reflect.MakeMapWithSize(t, sv.Len())
		iter := sv.MapRange()
		elem := t.Elem()
		srcTmp := reflect.New(elem).Elem()
		dstTmp := reflect.New(elem).Elem()
		srcPtr := unsafe.Pointer(srcTmp.UnsafeAddr())
		dstPtr := unsafe.Pointer(dstTmp.UnsafeAddr())
		for iter.Next() {
			srcTmp.SetIterValue(iter)
			dstTmp.SetZero()
			if err := valueStep(srcPtr, dstPtr, scratch); err != nil {
				return err
			}
			out.SetMapIndex(iter.Key(), dstTmp)
		}
		dv.Set(out)
		return nil
	}
}
