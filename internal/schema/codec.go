package schema

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"time"
	"unsafe"
)

const (
	codecVersion byte = 1

	codecWireBool byte = iota + 1
	codecWireInt
	codecWireUint
	codecWireFloat
	codecWireString
	codecWireTime
	codecWirePointer
	codecWireArray
	codecWireSlice
	codecWireStruct
	codecWireMap
	codecWireBytes
	codecWireCustom
)

const (
	maxIntValue        = int(^uint(0) >> 1)
	codecTimeNsecLimit = int64(1_000_000_000)
)

var errUnsupportedCodecField = errors.New("unsupported field")

type CodecRuntime struct {
	fields    []codecField
	byName    map[string]int
	encodeErr []codecEncodeErrStep
}

type codecField struct {
	name   string
	wire   byte
	encode codecEncodeStep
	decode codecDecodeStep
	zero   codecStep
}

type (
	codecEncodeStep    func(src unsafe.Pointer, dst []byte) []byte
	codecEncodeErrStep func(src unsafe.Pointer, dst []byte) ([]byte, error)
	codecDecodeStep    func(payload []byte, wire byte, dst unsafe.Pointer) error
	codecReadStep      func(src []byte, pos int, dst unsafe.Pointer) (int, error)
	codecStep          func(ptr unsafe.Pointer)
)

type codecValue struct {
	wire      byte
	minSize   int // lower bound for one value; it may be smaller than current encoder output.
	encode    codecEncodeStep
	encodeErr codecEncodeErrStep
	read      codecReadStep
	zero      codecStep
}

type codecCompiler struct {
	stack map[reflect.Type]bool
}

type codecStruct struct {
	fields    []codecField
	encodeErr []codecEncodeErrStep
}

func (s *codecStruct) add(field codecField, encodeErr codecEncodeErrStep) {
	s.fields = append(s.fields, field)
	if encodeErr != nil {
		if s.encodeErr == nil {
			s.encodeErr = make([]codecEncodeErrStep, len(s.fields)-1, len(s.fields))
		}
		s.encodeErr = append(s.encodeErr, encodeErr)
	} else if s.encodeErr != nil {
		s.encodeErr = append(s.encodeErr, nil)
	}
}

func (s *codecStruct) append(other codecStruct) {
	for i := range other.fields {
		var encodeErr codecEncodeErrStep
		if other.encodeErr != nil {
			encodeErr = other.encodeErr[i]
		}
		s.add(other.fields[i], encodeErr)
	}
}

func (c CodecRuntime) Encode(src unsafe.Pointer, dst []byte) ([]byte, error) {
	dst = append(dst, codecVersion)
	dst = codecWriteUvarint(dst, uint64(len(c.fields)))
	if c.encodeErr == nil {
		for i := range c.fields {
			dst = c.fields[i].encode(src, dst)
		}
		return dst, nil
	}
	for i := range c.fields {
		f := &c.fields[i]
		encodeErr := c.encodeErr[i]
		if encodeErr != nil {
			var err error
			dst, err = encodeErr(src, dst)
			if err != nil {
				return dst, fmt.Errorf("encoding field %q: %w", f.name, err)
			}
		} else {
			dst = f.encode(src, dst)
		}
	}
	return dst, nil
}

func (c *CodecRuntime) Decode(src []byte, dst unsafe.Pointer) error {
	if len(src) == 0 {
		return fmt.Errorf("decode: missing version")
	}
	if src[0] != codecVersion {
		return fmt.Errorf("decode: unsupported version %d", src[0])
	}
	for i := range c.fields {
		c.fields[i].zero(dst)
	}
	if err := codecDecodeFields(src[1:], c.fields, c.byName, dst); err != nil {
		return err
	}
	return nil
}

func codecDecodeFields(src []byte, fields []codecField, byName map[string]int, dst unsafe.Pointer) error {
	pos, err := codecReadFields(src, 0, fields, byName, dst)
	if err != nil {
		return err
	}
	if pos != len(src) {
		return fmt.Errorf("decode: trailing bytes")
	}
	return nil
}

func codecReadFields(src []byte, pos int, fields []codecField, byName map[string]int, dst unsafe.Pointer) (int, error) {
	count, pos, ok := codecReadUvarintAt(src, pos)
	if !ok {
		return 0, fmt.Errorf("decode: malformed field count")
	}
	for i := uint64(0); i < count; i++ {
		var nameLen uint64
		nameLen, pos, ok = codecReadUvarintAt(src, pos)
		if !ok || nameLen > uint64(len(src)-pos) {
			return 0, fmt.Errorf("decode: malformed field name")
		}
		name := src[pos : pos+int(nameLen)]
		pos += int(nameLen)
		if pos >= len(src) {
			return 0, fmt.Errorf("decode: missing wire type")
		}
		wire := src[pos]
		pos++
		var payloadLen uint64
		payloadLen, pos, ok = codecReadUvarintAt(src, pos)
		if !ok || payloadLen > uint64(len(src)-pos) {
			return 0, fmt.Errorf("decode: malformed payload length")
		}
		payload := src[pos : pos+int(payloadLen)]
		pos += int(payloadLen)

		nameKey := unsafe.String(unsafe.SliceData(name), len(name))
		idx := -1
		// Records written by this schema keep compiled order; schema-evolved
		// payloads fall through to the name map without weakening name framing.
		if i < uint64(len(fields)) && nameKey == fields[i].name {
			idx = int(i)
		} else if fieldIndex, exists := byName[nameKey]; exists {
			idx = fieldIndex
		}
		if idx >= 0 {
			if err := fields[idx].decode(payload, wire, dst); err != nil {
				return 0, fmt.Errorf("decoding field %q: %w", name, err)
			}
		}
	}
	return pos, nil
}

func compileCodec(t reflect.Type) (CodecRuntime, error) {
	c := codecCompiler{stack: make(map[reflect.Type]bool, 8)}
	compiled, err := c.compileStruct(t, nil, 0, t.Name())
	if err != nil {
		return CodecRuntime{}, err
	}
	fields := compiled.fields
	byName := make(map[string]int, len(fields))
	for i := range fields {
		if _, exists := byName[fields[i].name]; exists {
			return CodecRuntime{}, fmt.Errorf("duplicate field name %q", fields[i].name)
		}
		byName[fields[i].name] = i
	}
	return CodecRuntime{
		fields:    fields,
		byName:    byName,
		encodeErr: compiled.encodeErr,
	}, nil
}

func (c *codecCompiler) compileStruct(t reflect.Type, prefix []byte, base uintptr, path string) (codecStruct, error) {
	if t.Kind() != reflect.Struct {
		return codecStruct{}, fmt.Errorf("%w %s type %s", errUnsupportedCodecField, path, t)
	}
	if c.stack[t] {
		return codecStruct{}, fmt.Errorf("%w %s recursive type %s", errUnsupportedCodecField, path, t)
	}
	c.stack[t] = true
	var fields codecStruct
	var promoted codecStruct
	direct := make(map[string]struct{}, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		if fieldIgnoredByTags(sf) {
			continue
		}
		name := fieldDBName(sf)
		leaf, err := resolveCodecLeaf(sf.Type, path+"."+sf.Name)
		if err != nil {
			delete(c.stack, t)
			return codecStruct{}, err
		}
		if leaf != nil {
			fieldPrefix := codecAppendPath(prefix, name)
			field, encodeErr := makeCustomCodecField(string(fieldPrefix), base+sf.Offset, leaf)
			if _, exists := direct[field.name]; exists {
				delete(c.stack, t)
				return codecStruct{}, fmt.Errorf("duplicate field name %q", field.name)
			}
			direct[field.name] = struct{}{}
			fields.add(field, encodeErr)
			continue
		}
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct && sf.Type != nativeTimeType {
			nested, err := c.compileStruct(sf.Type, prefix, base+sf.Offset, path+"."+sf.Name)
			if err != nil {
				delete(c.stack, t)
				return codecStruct{}, err
			}
			promoted.append(nested)
			continue
		}
		fieldPrefix := codecAppendPath(prefix, name)
		if sf.Type.Kind() == reflect.Struct && sf.Type != nativeTimeType {
			nested, err := c.compileStruct(sf.Type, fieldPrefix, base+sf.Offset, path+"."+sf.Name)
			if err != nil {
				delete(c.stack, t)
				return codecStruct{}, err
			}
			for i := range nested.fields {
				if _, exists := direct[nested.fields[i].name]; exists {
					delete(c.stack, t)
					return codecStruct{}, fmt.Errorf("duplicate field name %q", nested.fields[i].name)
				}
				direct[nested.fields[i].name] = struct{}{}
			}
			fields.append(nested)
			continue
		}
		field, encodeErr, err := c.makeCodecField(sf.Type, string(fieldPrefix), base+sf.Offset, path+"."+sf.Name)
		if err != nil {
			delete(c.stack, t)
			return codecStruct{}, err
		}
		if _, exists := direct[field.name]; exists {
			delete(c.stack, t)
			return codecStruct{}, fmt.Errorf("duplicate field name %q", field.name)
		}
		direct[field.name] = struct{}{}
		fields.add(field, encodeErr)
	}
	delete(c.stack, t)

	if len(promoted.fields) != 0 {
		count := make(map[string]int, len(promoted.fields))
		for i := range promoted.fields {
			count[promoted.fields[i].name]++
		}
		for i := range promoted.fields {
			if _, shadowed := direct[promoted.fields[i].name]; shadowed {
				return codecStruct{}, fmt.Errorf("promoted field %q is shadowed by direct field", promoted.fields[i].name)
			}
			if count[promoted.fields[i].name] != 1 {
				return codecStruct{}, fmt.Errorf("ambiguous promoted field %q", promoted.fields[i].name)
			}
			var encodeErr codecEncodeErrStep
			if promoted.encodeErr != nil {
				encodeErr = promoted.encodeErr[i]
			}
			fields.add(promoted.fields[i], encodeErr)
		}
	}
	return fields, nil
}

func codecAppendPath(prefix []byte, name string) []byte {
	if len(prefix) == 0 {
		out := make([]byte, len(name))
		copy(out, name)
		return out
	}
	out := make([]byte, len(prefix)+1+len(name))
	copy(out, prefix)
	out[len(prefix)] = '.'
	copy(out[len(prefix)+1:], name)
	return out
}

func (c *codecCompiler) makeCodecField(t reflect.Type, name string, offset uintptr, path string) (codecField, codecEncodeErrStep, error) {
	if t == nativeTimeType {
		return codecField{
			name:   name,
			wire:   codecWireTime,
			encode: codecEncodeTimeField(name, offset),
			decode: codecDecodeTimeField(offset),
			zero:   codecZeroField[time.Time](offset),
		}, nil, nil
	}
	leaf, err := resolveCodecLeaf(t, path)
	if err != nil {
		return codecField{}, nil, err
	}
	if leaf != nil {
		field, encodeErr := makeCustomCodecField(name, offset, leaf)
		return field, encodeErr, nil
	}

	switch t.Kind() {

	case reflect.Bool:
		return codecField{
			name:   name,
			wire:   codecWireBool,
			encode: codecEncodeBoolField(name, offset),
			decode: codecDecodeBoolField(offset),
			zero:   codecZeroField[bool](offset),
		}, nil, nil

	case reflect.Int:
		return codecSignedField[int](name, offset), nil, nil
	case reflect.Int8:
		return codecSignedField[int8](name, offset), nil, nil
	case reflect.Int16:
		return codecSignedField[int16](name, offset), nil, nil
	case reflect.Int32:
		return codecSignedField[int32](name, offset), nil, nil
	case reflect.Int64:
		return codecSignedField[int64](name, offset), nil, nil
	case reflect.Uint:
		return codecUnsignedField[uint](name, offset), nil, nil
	case reflect.Uint8:
		return codecUnsignedField[uint8](name, offset), nil, nil
	case reflect.Uint16:
		return codecUnsignedField[uint16](name, offset), nil, nil
	case reflect.Uint32:
		return codecUnsignedField[uint32](name, offset), nil, nil
	case reflect.Uint64:
		return codecUnsignedField[uint64](name, offset), nil, nil
	case reflect.Float32:
		return codecFloatField[float32](name, offset), nil, nil
	case reflect.Float64:
		return codecFloatField[float64](name, offset), nil, nil

	case reflect.String:
		return codecField{
			name:   name,
			wire:   codecWireString,
			encode: codecEncodeStringField(name, offset),
			decode: codecDecodeStringField(offset),
			zero:   codecZeroField[string](offset),
		}, nil, nil

	case reflect.Pointer, reflect.Array, reflect.Slice, reflect.Map:
		value, err := c.makeCodecValue(t, path)
		if err != nil {
			return codecField{}, nil, err
		}
		field := codecField{
			name:   name,
			wire:   value.wire,
			decode: codecDecodeVariableField(offset, value),
			zero:   codecZeroVariableField(offset, value),
		}
		if value.encodeErr != nil {
			return field, codecEncodeVariableFieldErr(name, offset, value), nil
		}
		field.encode = codecEncodeVariableField(name, offset, value)
		return field, nil, nil

	default:
		return codecField{}, nil, fmt.Errorf("%w %s type %s", errUnsupportedCodecField, path, t)
	}
}

func (c *codecCompiler) makeCodecValue(t reflect.Type, path string) (codecValue, error) {
	if t == nativeTimeType {
		return codecValue{
			wire:    codecWireTime,
			minSize: 12,
			encode:  codecEncodeTimeValue(),
			read:    codecReadTimeValue(),
			zero:    codecZeroValue[time.Time](),
		}, nil
	}
	leaf, err := resolveCodecLeaf(t, path)
	if err != nil {
		return codecValue{}, err
	}
	if leaf != nil {
		return codecValue{
			wire:      codecWireCustom,
			minSize:   4,
			encodeErr: codecEncodeCustomValue(leaf),
			read:      codecReadCustomValue(leaf),
			zero:      codecZeroCustomValue(t),
		}, nil
	}

	switch t.Kind() {

	case reflect.Bool:
		return codecValue{
			wire:    codecWireBool,
			minSize: 1,
			encode:  codecEncodeBoolValue(),
			read:    codecReadBoolValue(),
			zero:    codecZeroValue[bool](),
		}, nil

	case reflect.Int:
		return codecSignedValue[int](), nil
	case reflect.Int8:
		return codecSignedValue[int8](), nil
	case reflect.Int16:
		return codecSignedValue[int16](), nil
	case reflect.Int32:
		return codecSignedValue[int32](), nil
	case reflect.Int64:
		return codecSignedValue[int64](), nil
	case reflect.Uint:
		return codecUnsignedValue[uint](), nil
	case reflect.Uint8:
		return codecUnsignedValue[uint8](), nil
	case reflect.Uint16:
		return codecUnsignedValue[uint16](), nil
	case reflect.Uint32:
		return codecUnsignedValue[uint32](), nil
	case reflect.Uint64:
		return codecUnsignedValue[uint64](), nil
	case reflect.Float32:
		return codecFloatValue[float32](), nil
	case reflect.Float64:
		return codecFloatValue[float64](), nil

	case reflect.String:
		return codecValue{
			wire:    codecWireString,
			minSize: 1,
			encode:  codecEncodeStringValue(),
			read:    codecReadStringValue(),
			zero:    codecZeroValue[string](),
		}, nil

	case reflect.Struct:
		return c.makeCodecStructValue(t, path)

	case reflect.Pointer:
		elem, err := c.makeCodecValue(t.Elem(), path)
		if err != nil {
			return codecValue{}, err
		}
		value := codecValue{
			wire:    codecWirePointer,
			minSize: 1,
			read:    codecReadPointerValue(t.Elem(), elem),
			zero:    codecZeroValue[unsafe.Pointer](),
		}
		if elem.encodeErr != nil {
			value.encodeErr = codecEncodePointerValueErr(elem)
		} else {
			value.encode = codecEncodePointerValue(elem)
		}
		return value, nil

	case reflect.Array:
		elem, err := c.makeCodecValue(t.Elem(), path+"[]")
		if err != nil {
			return codecValue{}, err
		}
		value := codecValue{
			wire:    codecWireArray,
			minSize: t.Len() * elem.minSize,
			read:    codecReadArrayValue(t.Elem().Size(), t.Len(), elem),
			zero:    codecZeroArrayValue(t.Elem().Size(), t.Len(), elem),
		}
		if elem.encodeErr != nil {
			value.encodeErr = codecEncodeArrayValueErr(t.Elem().Size(), t.Len(), elem)
		} else {
			value.encode = codecEncodeArrayValue(t.Elem().Size(), t.Len(), elem)
		}
		return value, nil

	case reflect.Slice:
		elem, err := c.makeCodecValue(t.Elem(), path+"[]")
		if err != nil {
			return codecValue{}, err
		}
		if t.Elem().Kind() == reflect.Uint8 && elem.wire == codecWireUint {
			return codecValue{
				wire:    codecWireBytes,
				minSize: 1,
				encode:  codecEncodeBytesValue(),
				read:    codecReadBytesValue(t),
				zero:    codecZeroValue[sliceHeader](),
			}, nil
		}
		value := codecValue{
			wire:    codecWireSlice,
			minSize: 1,
			read:    codecReadSliceValue(t, t.Elem().Size(), elem),
			zero:    codecZeroValue[sliceHeader](),
		}
		if elem.encodeErr != nil {
			value.encodeErr = codecEncodeSliceValueErr(t.Elem().Size(), elem)
		} else {
			value.encode = codecEncodeSliceValue(t.Elem().Size(), elem)
		}
		return value, nil

	case reflect.Map:
		if c.stack[t] {
			return codecValue{}, fmt.Errorf("%w %s recursive type %s", errUnsupportedCodecField, path, t)
		}
		c.stack[t] = true
		key, err := c.makeCodecMapKey(t.Key(), path+" key")
		if err != nil {
			delete(c.stack, t)
			return codecValue{}, err
		}
		elem, err := c.makeCodecValue(t.Elem(), path+" value")
		if err != nil {
			delete(c.stack, t)
			return codecValue{}, err
		}
		delete(c.stack, t)
		value := codecValue{
			wire:    codecWireMap,
			minSize: 1,
			read:    codecReadMapValue(t, key, elem),
			zero:    codecZeroValue[unsafe.Pointer](),
		}
		if elem.encodeErr != nil {
			value.encodeErr = codecEncodeMapValueErr(t, key, elem)
		} else {
			value.encode = codecEncodeMapValue(t, key, elem)
		}
		return value, nil

	default:
		return codecValue{}, fmt.Errorf("%w %s type %s", errUnsupportedCodecField, path, t)
	}
}

func (c *codecCompiler) makeCodecMapKey(t reflect.Type, path string) (codecValue, error) {
	leaf, err := resolveCodecLeaf(t, path)
	if err != nil {
		return codecValue{}, err
	}
	if leaf != nil {
		return codecValue{}, fmt.Errorf("%w %s map key type %s uses RBI codec methods", errUnsupportedCodecField, path, t)
	}
	switch t.Kind() {

	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return c.makeCodecValue(t, path)

	case reflect.Array:
		elem, err := c.makeCodecMapKey(t.Elem(), path+"[]")
		if err != nil {
			return codecValue{}, err
		}
		return codecValue{
			wire:    codecWireArray,
			minSize: t.Len() * elem.minSize,
			encode:  codecEncodeArrayValue(t.Elem().Size(), t.Len(), elem),
			read:    codecReadArrayValue(t.Elem().Size(), t.Len(), elem),
			zero:    codecZeroArrayValue(t.Elem().Size(), t.Len(), elem),
		}, nil

	case reflect.Struct:
		return c.makeCodecMapKeyStruct(t, path)

	default:
		return codecValue{}, fmt.Errorf("%w %s map key type %s", errUnsupportedCodecField, path, t)
	}
}

func (c *codecCompiler) makeCodecMapKeyStruct(t reflect.Type, path string) (codecValue, error) {
	if c.stack[t] {
		return codecValue{}, fmt.Errorf("%w %s recursive type %s", errUnsupportedCodecField, path, t)
	}
	c.stack[t] = true
	var fields []codecField
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			delete(c.stack, t)
			return codecValue{}, fmt.Errorf("%w %s.%s hidden map key field type %s", errUnsupportedCodecField, path, sf.Name, t)
		}
		if fieldIgnoredByTags(sf) {
			delete(c.stack, t)
			return codecValue{}, fmt.Errorf("%w %s.%s ignored map key field type %s", errUnsupportedCodecField, path, sf.Name, t)
		}
		name := fieldDBName(sf)
		value, err := c.makeCodecMapKey(sf.Type, path+"."+sf.Name)
		if err != nil {
			delete(c.stack, t)
			return codecValue{}, err
		}
		fields = append(fields, codecField{
			name:   name,
			wire:   value.wire,
			encode: codecEncodeVariableField(name, sf.Offset, value),
			decode: codecDecodeVariableField(sf.Offset, value),
			zero:   codecZeroVariableField(sf.Offset, value),
		})
	}
	delete(c.stack, t)

	byName := make(map[string]int, len(fields))
	for i := range fields {
		if _, exists := byName[fields[i].name]; exists {
			return codecValue{}, fmt.Errorf("duplicate field name %q", fields[i].name)
		}
		byName[fields[i].name] = i
	}

	value := codecValue{
		wire:    codecWireStruct,
		minSize: 1,
		read:    codecReadStructValue(fields, byName),
		zero:    codecZeroStructValue(fields),
	}
	value.encode = codecEncodeStructValue(fields)
	return value, nil
}

func (c *codecCompiler) makeCodecStructValue(t reflect.Type, path string) (codecValue, error) {
	compiled, err := c.compileStruct(t, nil, 0, path)
	if err != nil {
		return codecValue{}, err
	}
	fields := compiled.fields
	byName := make(map[string]int, len(fields))
	for i := range fields {
		if _, exists := byName[fields[i].name]; exists {
			return codecValue{}, fmt.Errorf("duplicate field name %q", fields[i].name)
		}
		byName[fields[i].name] = i
	}
	value := codecValue{
		wire:    codecWireStruct,
		minSize: 1,
		read:    codecReadStructValue(fields, byName),
		zero:    codecZeroStructValue(fields),
	}
	if compiled.encodeErr != nil {
		value.encodeErr = codecEncodeStructValueErr(fields, compiled.encodeErr)
		return value, nil
	}
	value.encode = codecEncodeStructValue(fields)
	return value, nil
}

func codecSignedField[T codecSigned](name string, offset uintptr) codecField {
	return codecField{
		name:   name,
		wire:   codecWireInt,
		encode: codecEncodeSignedField[T](name, offset),
		decode: codecDecodeSignedField[T](offset),
		zero:   codecZeroField[T](offset),
	}
}

func codecUnsignedField[T codecUnsigned](name string, offset uintptr) codecField {
	return codecField{
		name:   name,
		wire:   codecWireUint,
		encode: codecEncodeUnsignedField[T](name, offset),
		decode: codecDecodeUnsignedField[T](offset),
		zero:   codecZeroField[T](offset),
	}
}

func codecFloatField[T codecFloat](name string, offset uintptr) codecField {
	return codecField{
		name:   name,
		wire:   codecWireFloat,
		encode: codecEncodeFloatField[T](name, offset),
		decode: codecDecodeFloatField[T](offset),
		zero:   codecZeroField[T](offset),
	}
}

func codecSignedValue[T codecSigned]() codecValue {
	return codecValue{
		wire:    codecWireInt,
		minSize: 9,
		encode:  codecEncodeSignedValue[T](),
		read:    codecReadSignedValue[T](),
		zero:    codecZeroValue[T](),
	}
}

func codecUnsignedValue[T codecUnsigned]() codecValue {
	return codecValue{
		wire:    codecWireUint,
		minSize: 9,
		encode:  codecEncodeUnsignedValue[T](),
		read:    codecReadUnsignedValue[T](),
		zero:    codecZeroValue[T](),
	}
}

func codecFloatValue[T codecFloat]() codecValue {
	return codecValue{
		wire:    codecWireFloat,
		minSize: 9,
		encode:  codecEncodeFloatValue[T](),
		read:    codecReadFloatValue[T](),
		zero:    codecZeroValue[T](),
	}
}

type codecSigned interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type codecUnsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type codecFloat interface {
	~float32 | ~float64
}

func codecZeroField[T any](offset uintptr) codecStep {
	return func(ptr unsafe.Pointer) {
		var zero T
		*(*T)(unsafe.Add(ptr, offset)) = zero
	}
}

func codecZeroValue[T any]() codecStep {
	return func(ptr unsafe.Pointer) {
		var zero T
		*(*T)(ptr) = zero
	}
}

func makeCustomCodecField(name string, offset uintptr, leaf *codecLeaf) (codecField, codecEncodeErrStep) {
	return codecField{
		name:   name,
		wire:   codecWireCustom,
		decode: codecDecodeCustomField(offset, leaf),
		zero:   codecZeroCustomField(offset, leaf.typ),
	}, codecEncodeCustomField(name, offset, leaf)
}

func codecEncodeCustomField(name string, offset uintptr, leaf *codecLeaf) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		dst, lenPos, payloadStart := codecWriteFieldHeaderReserve(dst, name, codecWireCustom)
		payload, err := leaf.encode(unsafe.Add(src, offset), dst)
		dst = payload
		if err != nil {
			return dst, err
		}
		return codecBackfillFieldLength(dst, lenPos, payloadStart), nil
	}
}

func codecDecodeCustomField(offset uintptr, leaf *codecLeaf) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		if wire != codecWireCustom {
			return fmt.Errorf("expected custom payload")
		}
		return leaf.decode(payload, unsafe.Add(dst, offset))
	}
}

func codecZeroCustomField(offset uintptr, t reflect.Type) codecStep {
	return func(ptr unsafe.Pointer) {
		reflect.NewAt(t, unsafe.Add(ptr, offset)).Elem().SetZero()
	}
}

func codecEncodeCustomValue(leaf *codecLeaf) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		start := len(dst)
		dst = append(dst, 0, 0, 0, 0)
		payload, err := leaf.encode(src, dst)
		dst = payload
		if err != nil {
			return dst, err
		}
		length := len(dst) - start - 4
		if uint64(length) > math.MaxUint32 {
			return dst, fmt.Errorf("custom payload length %d overflows uint32", length)
		}
		binary.LittleEndian.PutUint32(dst[start:start+4], uint32(length))
		return dst, nil
	}
}

func codecReadCustomValue(leaf *codecLeaf) codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if len(src)-pos < 4 {
			return 0, fmt.Errorf("malformed custom payload length")
		}
		length := uint64(binary.LittleEndian.Uint32(src[pos : pos+4]))
		pos += 4
		if length > uint64(len(src)-pos) {
			return 0, fmt.Errorf("malformed custom payload length")
		}
		end := pos + int(length)
		if err := leaf.decode(src[pos:end], dst); err != nil {
			return 0, err
		}
		return end, nil
	}
}

func codecZeroCustomValue(t reflect.Type) codecStep {
	return func(ptr unsafe.Pointer) {
		reflect.NewAt(t, ptr).Elem().SetZero()
	}
}

func codecEncodeVariableField(name string, offset uintptr, value codecValue) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		dst, lenPos, payloadStart := codecWriteFieldHeaderReserve(dst, name, value.wire)
		dst = value.encode(unsafe.Add(src, offset), dst)
		return codecBackfillFieldLength(dst, lenPos, payloadStart)
	}
}

func codecEncodeVariableFieldErr(name string, offset uintptr, value codecValue) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		dst, lenPos, payloadStart := codecWriteFieldHeaderReserve(dst, name, value.wire)
		var err error
		dst, err = value.encodeErr(unsafe.Add(src, offset), dst)
		if err != nil {
			return dst, err
		}
		return codecBackfillFieldLength(dst, lenPos, payloadStart), nil
	}
}

func codecBackfillFieldLength(dst []byte, lenPos, payloadStart int) []byte {
	payloadLen := len(dst) - payloadStart
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(payloadLen))
	copy(dst[lenPos:], scratch[:n])
	if n != binary.MaxVarintLen64 {
		end := len(dst)
		copy(dst[lenPos+n:], dst[payloadStart:end])
		dst = dst[:end-(binary.MaxVarintLen64-n)]
	}
	return dst
}

func codecDecodeVariableField(offset uintptr, value codecValue) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		if wire != value.wire {
			return fmt.Errorf("expected composite payload")
		}
		pos, err := value.read(payload, 0, unsafe.Add(dst, offset))
		if err != nil {
			return err
		}
		if pos != len(payload) {
			return fmt.Errorf("trailing composite payload")
		}
		return nil
	}
}

func codecZeroVariableField(offset uintptr, value codecValue) codecStep {
	return func(ptr unsafe.Pointer) {
		value.zero(unsafe.Add(ptr, offset))
	}
}

func codecEncodeBoolField(name string, offset uintptr) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		dst = codecWriteFieldHeader(dst, name, codecWireBool, 1)
		if *(*bool)(unsafe.Add(src, offset)) {
			return append(dst, 1)
		}
		return append(dst, 0)
	}
}

func codecEncodeBoolValue() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		if *(*bool)(src) {
			return append(dst, 1)
		}
		return append(dst, 0)
	}
}

func codecDecodeBoolField(offset uintptr) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		if wire != codecWireBool || len(payload) != 1 {
			return fmt.Errorf("expected bool payload")
		}
		switch payload[0] {
		case 0:
			*(*bool)(unsafe.Add(dst, offset)) = false
		case 1:
			*(*bool)(unsafe.Add(dst, offset)) = true
		default:
			return fmt.Errorf("invalid bool value %d", payload[0])
		}
		return nil
	}
}

func codecReadBoolValue() codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if pos >= len(src) {
			return 0, fmt.Errorf("malformed bool payload")
		}
		switch src[pos] {
		case 0:
			*(*bool)(dst) = false
		case 1:
			*(*bool)(dst) = true
		default:
			return 0, fmt.Errorf("invalid bool value %d", src[pos])
		}
		return pos + 1, nil
	}
}

func codecEncodeSignedField[T codecSigned](name string, offset uintptr) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		dst = codecWriteFieldHeader(dst, name, codecWireInt, 8)
		var scratch [8]byte
		binary.LittleEndian.PutUint64(scratch[:], uint64(int64(*(*T)(unsafe.Add(src, offset)))))
		return append(dst, scratch[:]...)
	}
}

func codecEncodeSignedValue[T codecSigned]() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		var scratch [8]byte
		dst = append(dst, codecWireInt)
		binary.LittleEndian.PutUint64(scratch[:], uint64(int64(*(*T)(src))))
		return append(dst, scratch[:]...)
	}
}

func codecDecodeSignedField[T codecSigned](offset uintptr) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		value, err := codecSignedValueFromWire[T](payload, wire)
		if err != nil {
			return err
		}
		*(*T)(unsafe.Add(dst, offset)) = T(value)
		return nil
	}
}

func codecReadSignedValue[T codecSigned]() codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if len(src)-pos < 9 {
			return 0, fmt.Errorf("malformed numeric payload")
		}
		wire := src[pos]
		pos++
		value, err := codecSignedValueFromWire[T](src[pos:pos+8], wire)
		if err != nil {
			return 0, err
		}
		pos += 8
		*(*T)(dst) = T(value)
		return pos, nil
	}
}

func codecEncodeUnsignedField[T codecUnsigned](name string, offset uintptr) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		dst = codecWriteFieldHeader(dst, name, codecWireUint, 8)
		var scratch [8]byte
		binary.LittleEndian.PutUint64(scratch[:], uint64(*(*T)(unsafe.Add(src, offset))))
		return append(dst, scratch[:]...)
	}
}

func codecEncodeUnsignedValue[T codecUnsigned]() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		var scratch [8]byte
		dst = append(dst, codecWireUint)
		binary.LittleEndian.PutUint64(scratch[:], uint64(*(*T)(src)))
		return append(dst, scratch[:]...)
	}
}

func codecDecodeUnsignedField[T codecUnsigned](offset uintptr) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		value, err := codecUnsignedValueFromWire[T](payload, wire)
		if err != nil {
			return err
		}
		*(*T)(unsafe.Add(dst, offset)) = T(value)
		return nil
	}
}

func codecReadUnsignedValue[T codecUnsigned]() codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if len(src)-pos < 9 {
			return 0, fmt.Errorf("malformed numeric payload")
		}
		wire := src[pos]
		pos++
		value, err := codecUnsignedValueFromWire[T](src[pos:pos+8], wire)
		if err != nil {
			return 0, err
		}
		pos += 8
		*(*T)(dst) = T(value)
		return pos, nil
	}
}

func codecEncodeFloatField[T codecFloat](name string, offset uintptr) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		dst = codecWriteFieldHeader(dst, name, codecWireFloat, 8)
		var scratch [8]byte
		binary.LittleEndian.PutUint64(scratch[:], math.Float64bits(float64(*(*T)(unsafe.Add(src, offset)))))
		return append(dst, scratch[:]...)
	}
}

func codecEncodeFloatValue[T codecFloat]() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		var scratch [8]byte
		dst = append(dst, codecWireFloat)
		binary.LittleEndian.PutUint64(scratch[:], math.Float64bits(float64(*(*T)(src))))
		return append(dst, scratch[:]...)
	}
}

func codecDecodeFloatField[T codecFloat](offset uintptr) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		value, err := codecFloatValueFromWire[T](payload, wire)
		if err != nil {
			return err
		}
		*(*T)(unsafe.Add(dst, offset)) = T(value)
		return nil
	}
}

func codecReadFloatValue[T codecFloat]() codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if len(src)-pos < 9 {
			return 0, fmt.Errorf("malformed float payload")
		}
		wire := src[pos]
		pos++
		value, err := codecFloatValueFromWire[T](src[pos:pos+8], wire)
		if err != nil {
			return 0, err
		}
		pos += 8
		*(*T)(dst) = T(value)
		return pos, nil
	}
}

func codecEncodeStringField(name string, offset uintptr) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		value := *(*string)(unsafe.Add(src, offset))
		dst = codecWriteFieldHeader(dst, name, codecWireString, uint64(len(value)))
		return append(dst, value...)
	}
}

func codecEncodeStringValue() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		value := *(*string)(src)
		dst = codecWriteUvarint(dst, uint64(len(value)))
		return append(dst, value...)
	}
}

func codecDecodeStringField(offset uintptr) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		if wire != codecWireString {
			return fmt.Errorf("expected string payload")
		}
		*(*string)(unsafe.Add(dst, offset)) = string(payload)
		return nil
	}
}

func codecReadStringValue() codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		n, pos, ok := codecReadUvarintAt(src, pos)
		if !ok || n > uint64(len(src)-pos) {
			return 0, fmt.Errorf("malformed string payload")
		}
		*(*string)(dst) = string(src[pos : pos+int(n)])
		return pos + int(n), nil
	}
}

func codecEncodeTimeField(name string, offset uintptr) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		value := *(*time.Time)(unsafe.Add(src, offset))
		dst = codecWriteFieldHeader(dst, name, codecWireTime, 12)
		var scratch [12]byte
		binary.LittleEndian.PutUint64(scratch[:8], uint64(value.Unix()))
		binary.LittleEndian.PutUint32(scratch[8:], uint32(value.Nanosecond()))
		return append(dst, scratch[:]...)
	}
}

func codecEncodeTimeValue() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		value := *(*time.Time)(src)
		var scratch [12]byte
		binary.LittleEndian.PutUint64(scratch[:8], uint64(value.Unix()))
		binary.LittleEndian.PutUint32(scratch[8:], uint32(value.Nanosecond()))
		return append(dst, scratch[:]...)
	}
}

func codecDecodeTimeField(offset uintptr) codecDecodeStep {
	return func(payload []byte, wire byte, dst unsafe.Pointer) error {
		if wire != codecWireTime || len(payload) != 12 {
			return fmt.Errorf("expected time payload")
		}
		sec := int64(binary.LittleEndian.Uint64(payload[:8]))
		nsec := int64(binary.LittleEndian.Uint32(payload[8:]))
		if nsec >= codecTimeNsecLimit {
			return fmt.Errorf("malformed time payload")
		}
		*(*time.Time)(unsafe.Add(dst, offset)) = time.Unix(sec, nsec).UTC()
		return nil
	}
}

func codecReadTimeValue() codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if len(src)-pos < 12 {
			return 0, fmt.Errorf("malformed time payload")
		}
		sec := int64(binary.LittleEndian.Uint64(src[pos:]))
		nsec := int64(binary.LittleEndian.Uint32(src[pos+8:]))
		if nsec >= codecTimeNsecLimit {
			return 0, fmt.Errorf("malformed time payload")
		}
		pos += 12
		*(*time.Time)(dst) = time.Unix(sec, nsec).UTC()
		return pos, nil
	}
}

func codecEncodePointerValue(elem codecValue) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		ptr := *(*unsafe.Pointer)(src)
		if ptr == nil {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		return elem.encode(ptr, dst)
	}
}

func codecEncodePointerValueErr(elem codecValue) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		ptr := *(*unsafe.Pointer)(src)
		if ptr == nil {
			return append(dst, 0), nil
		}
		dst = append(dst, 1)
		return elem.encodeErr(ptr, dst)
	}
}

func codecReadPointerValue(elemType reflect.Type, elem codecValue) codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if pos >= len(src) {
			return 0, fmt.Errorf("malformed pointer payload")
		}
		switch src[pos] {
		case 0:
			*(*unsafe.Pointer)(dst) = nil
			return pos + 1, nil

		case 1:
			pos++
			out := reflect.New(elemType)
			ptr := unsafe.Pointer(out.Pointer())
			next, err := elem.read(src, pos, ptr)
			if err != nil {
				return 0, err
			}
			*(*unsafe.Pointer)(dst) = ptr
			return next, nil

		default:
			return 0, fmt.Errorf("invalid pointer marker %d", src[pos])
		}
	}
}

func codecEncodeArrayValue(elemSize uintptr, length int, elem codecValue) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		for i := 0; i < length; i++ {
			dst = elem.encode(unsafe.Add(src, uintptr(i)*elemSize), dst)
		}
		return dst
	}
}

func codecEncodeArrayValueErr(elemSize uintptr, length int, elem codecValue) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		for i := 0; i < length; i++ {
			var err error
			dst, err = elem.encodeErr(unsafe.Add(src, uintptr(i)*elemSize), dst)
			if err != nil {
				return dst, err
			}
		}
		return dst, nil
	}
}

func codecReadArrayValue(elemSize uintptr, length int, elem codecValue) codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		for i := 0; i < length; i++ {
			ptr := unsafe.Add(dst, uintptr(i)*elemSize)
			next, err := elem.read(src, pos, ptr)
			if err != nil {
				return 0, err
			}
			pos = next
		}
		return pos, nil
	}
}

func codecZeroArrayValue(elemSize uintptr, length int, elem codecValue) codecStep {
	return func(ptr unsafe.Pointer) {
		for i := 0; i < length; i++ {
			elem.zero(unsafe.Add(ptr, uintptr(i)*elemSize))
		}
	}
}

type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

func codecEncodeBytesValue() codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		h := (*sliceHeader)(src)
		if h.data == nil && h.len == 0 && h.cap == 0 {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		dst = codecWriteUvarint(dst, uint64(h.len))
		if h.len != 0 {
			dst = append(dst, unsafe.Slice((*byte)(h.data), h.len)...)
		}
		return dst
	}
}

func codecReadBytesValue(sliceType reflect.Type) codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if pos >= len(src) {
			return 0, fmt.Errorf("malformed bytes payload")
		}
		switch src[pos] {
		case 0:
			*(*sliceHeader)(dst) = sliceHeader{}
			return pos + 1, nil

		case 1:
			pos++
			n, next, ok := codecReadUvarintAt(src, pos)
			if !ok || n > uint64(len(src)-next) || n > uint64(maxIntValue) {
				return 0, fmt.Errorf("malformed bytes length")
			}
			pos = next
			out := reflect.MakeSlice(sliceType, int(n), int(n))
			if n != 0 {
				copy(unsafe.Slice((*byte)(unsafe.Pointer(out.Index(0).UnsafeAddr())), int(n)), src[pos:pos+int(n)])
			}
			reflect.NewAt(sliceType, dst).Elem().Set(out)
			return pos + int(n), nil

		default:
			return 0, fmt.Errorf("invalid bytes marker %d", src[pos])
		}
	}
}

func codecEncodeSliceValue(elemSize uintptr, elem codecValue) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		h := (*sliceHeader)(src)
		if h.data == nil && h.len == 0 && h.cap == 0 {
			return append(dst, 0)
		}
		dst = append(dst, 1)
		dst = codecWriteUvarint(dst, uint64(h.len))
		for i := 0; i < h.len; i++ {
			dst = elem.encode(unsafe.Add(h.data, uintptr(i)*elemSize), dst)
		}
		return dst
	}
}

func codecEncodeSliceValueErr(elemSize uintptr, elem codecValue) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		h := (*sliceHeader)(src)
		if h.data == nil && h.len == 0 && h.cap == 0 {
			return append(dst, 0), nil
		}
		dst = append(dst, 1)
		dst = codecWriteUvarint(dst, uint64(h.len))
		for i := 0; i < h.len; i++ {
			var err error
			dst, err = elem.encodeErr(unsafe.Add(h.data, uintptr(i)*elemSize), dst)
			if err != nil {
				return dst, err
			}
		}
		return dst, nil
	}
}

func codecReadSliceValue(sliceType reflect.Type, elemSize uintptr, elem codecValue) codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if pos >= len(src) {
			return 0, fmt.Errorf("malformed slice payload")
		}
		switch src[pos] {
		case 0:
			*(*sliceHeader)(dst) = sliceHeader{}
			return pos + 1, nil

		case 1:
			pos++
			n, next, ok := codecReadUvarintAt(src, pos)
			if !ok || n > uint64(maxIntValue) || elem.minSize != 0 && n > uint64((len(src)-next)/elem.minSize) {
				return 0, fmt.Errorf("malformed slice length")
			}
			pos = next
			out := reflect.MakeSlice(sliceType, int(n), int(n))
			var data unsafe.Pointer
			if n != 0 {
				data = unsafe.Pointer(out.Index(0).UnsafeAddr())
			}
			for i := 0; i < int(n); i++ {
				ptr := unsafe.Add(data, uintptr(i)*elemSize)
				next, err := elem.read(src, pos, ptr)
				if err != nil {
					return 0, err
				}
				pos = next
			}
			reflect.NewAt(sliceType, dst).Elem().Set(out)
			return pos, nil

		default:
			return 0, fmt.Errorf("invalid slice marker %d", src[pos])
		}
	}
}

func codecEncodeMapValue(mapType reflect.Type, key codecValue, elem codecValue) codecEncodeStep {
	keyType := mapType.Key()
	elemType := mapType.Elem()
	return func(src unsafe.Pointer, dst []byte) []byte {
		if *(*unsafe.Pointer)(src) == nil {
			return append(dst, 0)
		}
		dst = append(dst, 1)

		value := reflect.NewAt(mapType, src).Elem()
		dst = codecWriteUvarint(dst, uint64(value.Len()))

		keyTmp := reflect.New(keyType).Elem()
		elemTmp := reflect.New(elemType).Elem()
		keyPtr := unsafe.Pointer(keyTmp.UnsafeAddr())
		elemPtr := unsafe.Pointer(elemTmp.UnsafeAddr())

		iter := value.MapRange()
		for iter.Next() {
			keyTmp.SetIterKey(iter)
			dst = key.encode(keyPtr, dst)
			elemTmp.SetIterValue(iter)
			dst = elem.encode(elemPtr, dst)
		}
		return dst
	}
}

func codecEncodeMapValueErr(mapType reflect.Type, key codecValue, elem codecValue) codecEncodeErrStep {
	keyType := mapType.Key()
	elemType := mapType.Elem()
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		if *(*unsafe.Pointer)(src) == nil {
			return append(dst, 0), nil
		}
		dst = append(dst, 1)
		value := reflect.NewAt(mapType, src).Elem()
		dst = codecWriteUvarint(dst, uint64(value.Len()))

		keyTmp := reflect.New(keyType).Elem()
		elemTmp := reflect.New(elemType).Elem()
		keyPtr := unsafe.Pointer(keyTmp.UnsafeAddr())
		elemPtr := unsafe.Pointer(elemTmp.UnsafeAddr())
		iter := value.MapRange()
		for iter.Next() {
			keyTmp.SetIterKey(iter)
			dst = key.encode(keyPtr, dst)
			elemTmp.SetIterValue(iter)
			var err error
			dst, err = elem.encodeErr(elemPtr, dst)
			if err != nil {
				return dst, err
			}
		}
		return dst, nil
	}
}

func codecReadMapValue(mapType reflect.Type, key codecValue, elem codecValue) codecReadStep {
	keyType := mapType.Key()
	elemType := mapType.Elem()
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		if pos >= len(src) {
			return 0, fmt.Errorf("malformed map payload")
		}
		switch src[pos] {
		case 0:
			*(*unsafe.Pointer)(dst) = nil
			return pos + 1, nil

		case 1:
			pos++
			n, next, ok := codecReadUvarintAt(src, pos)
			if !ok || n > uint64(maxIntValue) {
				return 0, fmt.Errorf("malformed map length")
			}
			if keyType.Size() == 0 && elemType.Size() == 0 {
				if n > 1 {
					return 0, fmt.Errorf("malformed map length")
				}
			} else if n > uint64(len(src)-next) {
				return 0, fmt.Errorf("malformed map length")
			}
			pos = next

			out := reflect.MakeMapWithSize(mapType, int(n))
			keyTmp := reflect.New(keyType).Elem()
			elemTmp := reflect.New(elemType).Elem()
			keyPtr := unsafe.Pointer(keyTmp.UnsafeAddr())
			elemPtr := unsafe.Pointer(elemTmp.UnsafeAddr())

			for i := 0; i < int(n); i++ {
				key.zero(keyPtr)
				next, err := key.read(src, pos, keyPtr)
				if err != nil {
					return 0, err
				}
				pos = next
				elem.zero(elemPtr)
				next, err = elem.read(src, pos, elemPtr)
				if err != nil {
					return 0, err
				}
				pos = next
				out.SetMapIndex(keyTmp, elemTmp)
			}
			reflect.NewAt(mapType, dst).Elem().Set(out)
			return pos, nil

		default:
			return 0, fmt.Errorf("invalid map marker %d", src[pos])
		}
	}
}

func codecEncodeStructValue(fields []codecField) codecEncodeStep {
	return func(src unsafe.Pointer, dst []byte) []byte {
		dst = codecWriteUvarint(dst, uint64(len(fields)))
		for i := range fields {
			dst = fields[i].encode(src, dst)
		}
		return dst
	}
}

func codecEncodeStructValueErr(fields []codecField, encodeErr []codecEncodeErrStep) codecEncodeErrStep {
	return func(src unsafe.Pointer, dst []byte) ([]byte, error) {
		dst = codecWriteUvarint(dst, uint64(len(fields)))
		for i := range fields {
			field := &fields[i]
			if encodeErr[i] != nil {
				var err error
				dst, err = encodeErr[i](src, dst)
				if err != nil {
					return dst, fmt.Errorf("encoding field %q: %w", field.name, err)
				}
			} else {
				dst = field.encode(src, dst)
			}
		}
		return dst, nil
	}
}

func codecReadStructValue(fields []codecField, byName map[string]int) codecReadStep {
	return func(src []byte, pos int, dst unsafe.Pointer) (int, error) {
		return codecReadFields(src, pos, fields, byName, dst)
	}
}

func codecZeroStructValue(fields []codecField) codecStep {
	return func(ptr unsafe.Pointer) {
		for i := range fields {
			fields[i].zero(ptr)
		}
	}
}

func codecSignedFits[T codecSigned](value int64) bool {
	var zero T
	switch any(zero).(type) {
	case int:
		return value >= int64(math.MinInt) && value <= int64(math.MaxInt)
	case int8:
		return value >= math.MinInt8 && value <= math.MaxInt8
	case int16:
		return value >= math.MinInt16 && value <= math.MaxInt16
	case int32:
		return value >= math.MinInt32 && value <= math.MaxInt32
	default:
		return true
	}
}

func codecUnsignedFits[T codecUnsigned](value uint64) bool {
	var zero T
	switch any(zero).(type) {
	case uint:
		return value <= uint64(math.MaxUint)
	case uint8:
		return value <= math.MaxUint8
	case uint16:
		return value <= math.MaxUint16
	case uint32:
		return value <= math.MaxUint32
	default:
		return true
	}
}

func codecSignedValueFromWire[T codecSigned](payload []byte, wire byte) (int64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("expected numeric payload")
	}
	var value int64
	switch wire {
	case codecWireInt:
		value = int64(binary.LittleEndian.Uint64(payload))

	case codecWireUint:
		u := binary.LittleEndian.Uint64(payload)
		if u > uint64(math.MaxInt64) {
			return 0, fmt.Errorf("uint value %d overflows int64", u)
		}
		value = int64(u)

	case codecWireFloat:
		f := math.Float64frombits(binary.LittleEndian.Uint64(payload))
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
			return 0, fmt.Errorf("float value %v cannot be represented exactly by destination integer", f)
		}
		if f < -math.Ldexp(1, 63) || f >= math.Ldexp(1, 63) {
			return 0, fmt.Errorf("float value %v overflows int64", f)
		}
		value = int64(f)

	default:
		return 0, fmt.Errorf("expected numeric wire type")
	}

	if !codecSignedFits[T](value) {
		return 0, fmt.Errorf("value %d overflows destination", value)
	}
	return value, nil
}

func codecUnsignedValueFromWire[T codecUnsigned](payload []byte, wire byte) (uint64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("expected numeric payload")
	}
	var value uint64
	switch wire {
	case codecWireUint:
		value = binary.LittleEndian.Uint64(payload)

	case codecWireInt:
		s := int64(binary.LittleEndian.Uint64(payload))
		if s < 0 {
			return 0, fmt.Errorf("negative value %d overflows unsigned destination", s)
		}
		value = uint64(s)

	case codecWireFloat:
		f := math.Float64frombits(binary.LittleEndian.Uint64(payload))
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
			return 0, fmt.Errorf("float value %v cannot be represented exactly by destination integer", f)
		}
		if f < 0 {
			return 0, fmt.Errorf("negative float value %v overflows unsigned destination", f)
		}
		if f >= math.Ldexp(1, 64) {
			return 0, fmt.Errorf("float value %v overflows uint64", f)
		}
		value = uint64(f)

	default:
		return 0, fmt.Errorf("expected numeric wire type")
	}

	if !codecUnsignedFits[T](value) {
		return 0, fmt.Errorf("value %d overflows destination", value)
	}
	return value, nil
}

func codecFloatValueFromWire[T codecFloat](payload []byte, wire byte) (float64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("expected numeric float payload")
	}
	var value float64
	switch wire {
	case codecWireFloat:
		value = math.Float64frombits(binary.LittleEndian.Uint64(payload))

	case codecWireInt:
		s := int64(binary.LittleEndian.Uint64(payload))
		if !codecSignedIntegerFitsFloat[T](s) {
			return 0, fmt.Errorf("integer value %d cannot be represented exactly by destination float", s)
		}
		value = float64(s)

	case codecWireUint:
		u := binary.LittleEndian.Uint64(payload)
		if !codecUnsignedIntegerFitsFloatPrecision(u, codecFloatPrecision[T]()) {
			return 0, fmt.Errorf("integer value %d cannot be represented exactly by destination float", u)
		}
		value = float64(u)

	default:
		return 0, fmt.Errorf("expected numeric wire type")
	}

	if !codecFloatFits[T](value) {
		return 0, fmt.Errorf("value %v cannot be represented exactly by destination float", value)
	}
	return value, nil
}

func codecSignedIntegerFitsFloat[T codecFloat](value int64) bool {
	var mag uint64
	if value < 0 {
		mag = uint64(-(value + 1)) + 1
	} else {
		mag = uint64(value)
	}
	return codecUnsignedIntegerFitsFloatPrecision(mag, codecFloatPrecision[T]())
}

func codecUnsignedIntegerFitsFloatPrecision(value uint64, precision int) bool {
	if value == 0 {
		return true
	}
	length := bits.Len64(value)
	return length <= precision || bits.TrailingZeros64(value) >= length-precision
}

func codecFloatPrecision[T codecFloat]() int {
	var zero T
	switch any(zero).(type) {
	case float32:
		return 24
	default:
		return 53
	}
}

func codecFloatFits[T codecFloat](value float64) bool {
	var zero T
	switch any(zero).(type) {
	case float32:
		return value == 0 || math.IsNaN(value) || math.IsInf(value, 0) || float64(float32(value)) == value
	default:
		return true
	}
}

func codecWriteFieldHeader(dst []byte, name string, wire byte, payloadLen uint64) []byte {
	dst = codecWriteUvarint(dst, uint64(len(name)))
	dst = append(dst, name...)
	dst = append(dst, wire)
	return codecWriteUvarint(dst, payloadLen)
}

func codecWriteFieldHeaderReserve(dst []byte, name string, wire byte) ([]byte, int, int) {
	dst = codecWriteUvarint(dst, uint64(len(name)))
	dst = append(dst, name...)
	dst = append(dst, wire)
	lenPos := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	return dst, lenPos, len(dst)
}

func codecWriteUvarint(dst []byte, value uint64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	return append(dst, scratch[:n]...)
}

func codecReadUvarintAt(src []byte, pos int) (uint64, int, bool) {
	value, n := binary.Uvarint(src[pos:])
	if n <= 0 {
		return 0, pos, false
	}
	return value, pos + n, true
}
