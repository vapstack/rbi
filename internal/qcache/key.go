package qcache

import (
	"strconv"
	"strings"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

type materializedPredKeyKind uint8

const (
	materializedPredKeyNone materializedPredKeyKind = iota
	materializedPredKeyOpaque
	materializedPredKeyScalar
	materializedPredKeyScalarComplement
	materializedPredKeyExactScalarRange
	materializedPredKeyExactScalarRangeComplement
	materializedPredKeyNumericBucketSpan
	materializedPredKeyDistinctSet
)

const (
	materializedPredKeyHasLo uint8 = 1 << iota
	materializedPredKeyHasHi
	materializedPredKeyLoInc
	materializedPredKeyHiInc
	materializedPredKeyLoNumeric
	materializedPredKeyHiNumeric
	materializedPredKeyScalarNumeric
	materializedPredKeySetHasNil
)

// Keep small exact-set cache keys inline so hot-path key construction stays
// allocation-free; wider sets simply skip this cache class.
const materializedPredKeyDistinctSetInlineMax = 4

const materializedPredKeySlicePoolMaxCap = 512

var materializedPredKeySlicePool = pooled.NewSlicePool[MaterializedPredKey](materializedPredKeySlicePoolMaxCap, pooled.ClearCap)

type MaterializedPredKey struct {
	kind        materializedPredKeyKind
	raw         string
	field       string
	op          qir.Op
	key         string
	keyIndex    keycodec.IndexKey
	loKey       string
	hiKey       string
	loIndex     keycodec.IndexKey
	hiIndex     keycodec.IndexKey
	flags       uint8
	startBucket int
	endBucket   int
	setTerms    [materializedPredKeyDistinctSetInlineMax]string
	setValueCnt uint8
}

func (key MaterializedPredKey) IsZero() bool {
	switch key.kind {
	case materializedPredKeyNone:
		return true
	case materializedPredKeyOpaque:
		return key.raw == ""
	default:
		return key.field == ""
	}
}

func (key MaterializedPredKey) Field() string {
	return key.field
}

func (key MaterializedPredKey) String() string {
	if key.IsZero() {
		return ""
	}
	switch key.kind {

	case materializedPredKeyOpaque:
		return key.raw

	case materializedPredKeyDistinctSet:
		var buf strings.Builder
		buf.WriteString(key.field)
		buf.WriteByte('\x1f')
		buf.WriteString("set_exact")
		buf.WriteByte('\x1f')
		buf.WriteString(strconv.Itoa(int(key.op)))
		buf.WriteByte('\x1f')

		if key.flags&materializedPredKeySetHasNil != 0 {
			buf.WriteByte('1')
		} else {
			buf.WriteByte('0')
		}

		var numBuf [24]byte
		for i := 0; i < int(key.setValueCnt); i++ {
			v := key.setTerms[i]
			buf.WriteByte('\x1e')
			buf.Write(strconv.AppendInt(numBuf[:0], int64(len(v)), 10))
			buf.WriteByte('\x1f')
			buf.WriteString(v)
		}
		return buf.String()

	case materializedPredKeyScalar:
		if key.flags&materializedPredKeyScalarNumeric != 0 {
			var raw [8]byte
			var buf strings.Builder
			buf.Grow(len(key.field) + 8 + 8)
			buf.WriteString(key.field)
			buf.WriteByte('\x1f')
			buf.WriteString(strconv.Itoa(int(key.op)))
			buf.WriteString("\x1fn\x1f")
			buf.Write(key.keyIndex.AppendBytes(raw[:0]))
			return buf.String()
		}
		return key.field + "\x1f" + strconv.Itoa(int(key.op)) + "\x1f" + key.key

	case materializedPredKeyScalarComplement:
		if key.flags&materializedPredKeyScalarNumeric != 0 {
			var raw [8]byte
			var buf strings.Builder
			buf.Grow(len(key.field) + len("count_range_complement") + 8 + 8)
			buf.WriteString(key.field)
			buf.WriteByte('\x1f')
			buf.WriteString("count_range_complement")
			buf.WriteByte('\x1f')
			buf.WriteString(strconv.Itoa(int(key.op)))
			buf.WriteString("\x1fn\x1f")
			buf.Write(key.keyIndex.AppendBytes(raw[:0]))
			return buf.String()
		}
		return key.field + "\x1f" + "count_range_complement" + "\x1f" +
			strconv.Itoa(int(key.op)) + "\x1f" + key.key

	case materializedPredKeyExactScalarRange, materializedPredKeyExactScalarRangeComplement:
		loTag := ""
		loVal := ""
		hiTag := ""
		hiVal := ""
		if key.flags&materializedPredKeyHasLo != 0 {
			if key.flags&materializedPredKeyLoNumeric != 0 {
				loTag = "n"
			} else {
				loTag = "s"
				loVal = key.loKey
			}
			if key.flags&materializedPredKeyLoInc != 0 {
				loTag += "["
			} else {
				loTag += "("
			}
		}
		if key.flags&materializedPredKeyHasHi != 0 {
			if key.flags&materializedPredKeyHiNumeric != 0 {
				hiTag = "n"
			} else {
				hiTag = "s"
				hiVal = key.hiKey
			}
			if key.flags&materializedPredKeyHiInc != 0 {
				hiTag += "]"
			} else {
				hiTag += ")"
			}
		}
		head := "range_exact"
		if key.kind == materializedPredKeyExactScalarRangeComplement {
			head = "count_range_exact_complement"
		}
		var raw [8]byte
		var buf strings.Builder
		buf.Grow(len(key.field) + len(head) + len(loTag) + len(loVal) + len(hiTag) + len(hiVal) + 40)
		buf.WriteString(key.field)
		buf.WriteByte('\x1f')
		buf.WriteString(head)
		buf.WriteByte('\x1f')
		buf.WriteString(loTag)
		buf.WriteByte('\x1f')
		if key.flags&materializedPredKeyLoNumeric != 0 {
			buf.Write(key.loIndex.AppendBytes(raw[:0]))
		} else {
			buf.WriteString(loVal)
		}
		buf.WriteByte('\x1f')
		buf.WriteString(hiTag)
		buf.WriteByte('\x1f')
		if key.flags&materializedPredKeyHiNumeric != 0 {
			buf.Write(key.hiIndex.AppendBytes(raw[:0]))
		} else {
			buf.WriteString(hiVal)
		}
		return buf.String()

	case materializedPredKeyNumericBucketSpan:
		return key.field + "\x1f" + "range_bucket" + "\x1f" +
			strconv.Itoa(key.startBucket) + "\x1f" + strconv.Itoa(key.endBucket)

	default:
		return ""
	}
}

func MaterializedPredKeyForScalar(field string, op qir.Op, key string) MaterializedPredKey {
	if field == "" || !op.IsMaterializedScalarCache() {
		return MaterializedPredKey{}
	}
	return MaterializedPredKey{
		kind:  materializedPredKeyScalar,
		field: field,
		op:    op,
		key:   key,
	}
}

func MaterializedPredKeyForNumericScalar(field string, op qir.Op, key keycodec.IndexKey) MaterializedPredKey {
	if field == "" || !key.IsNumeric() || !op.IsMaterializedScalarCache() {
		return MaterializedPredKey{}
	}
	return MaterializedPredKey{
		kind:     materializedPredKeyScalar,
		field:    field,
		op:       op,
		keyIndex: key,
		flags:    materializedPredKeyScalarNumeric,
	}
}

func MaterializedPredKeyForLookupKey(field string, op qir.Op, key keycodec.IndexLookupKey) MaterializedPredKey {
	if key.IsNumeric() {
		return MaterializedPredKeyForNumericScalar(field, op, key.IndexKey())
	}
	return MaterializedPredKeyForScalar(field, op, key.StringKey())
}

func MaterializedPredComplementKeyForScalar(field string, op qir.Op, key string) MaterializedPredKey {
	if field == "" || key == "" || !op.IsNumericRange() {
		return MaterializedPredKey{}
	}
	return MaterializedPredKey{
		kind:  materializedPredKeyScalarComplement,
		field: field,
		op:    op,
		key:   key,
	}
}

func MaterializedPredComplementKeyForNumericScalar(field string, op qir.Op, key keycodec.IndexKey) MaterializedPredKey {
	if field == "" || !key.IsNumeric() || !op.IsNumericRange() {
		return MaterializedPredKey{}
	}
	return MaterializedPredKey{
		kind:     materializedPredKeyScalarComplement,
		field:    field,
		op:       op,
		keyIndex: key,
		flags:    materializedPredKeyScalarNumeric,
	}
}

func MaterializedPredComplementKeyForLookupKey(field string, op qir.Op, key keycodec.IndexLookupKey) MaterializedPredKey {
	if key.IsNumeric() {
		return MaterializedPredComplementKeyForNumericScalar(field, op, key.IndexKey())
	}
	return MaterializedPredComplementKeyForScalar(field, op, key.StringKey())
}

func materializedPredKeyForExactScalarRangeKind(kind materializedPredKeyKind, field string, bounds indexdata.Bounds) MaterializedPredKey {
	if field == "" || bounds.Empty || (!bounds.HasLo && !bounds.HasHi) {
		return MaterializedPredKey{}
	}
	var flags uint8
	if bounds.HasLo {
		flags |= materializedPredKeyHasLo
		if bounds.LoInc {
			flags |= materializedPredKeyLoInc
		}
		if bounds.LoNumeric {
			flags |= materializedPredKeyLoNumeric
		}
	}
	if bounds.HasHi {
		flags |= materializedPredKeyHasHi
		if bounds.HiInc {
			flags |= materializedPredKeyHiInc
		}
		if bounds.HiNumeric {
			flags |= materializedPredKeyHiNumeric
		}
	}
	key := MaterializedPredKey{
		kind:  kind,
		field: field,
		flags: flags,
	}
	if bounds.HasLo {
		if bounds.LoNumeric {
			key.loIndex = bounds.LoIndex
		} else {
			key.loKey = bounds.LoKey
		}
	}
	if bounds.HasHi {
		if bounds.HiNumeric {
			key.hiIndex = bounds.HiIndex
		} else {
			key.hiKey = bounds.HiKey
		}
	}
	return key
}

func MaterializedPredKeyForExactScalarRange(field string, bounds indexdata.Bounds) MaterializedPredKey {
	return materializedPredKeyForExactScalarRangeKind(materializedPredKeyExactScalarRange, field, bounds)
}

func MaterializedPredComplementKeyForExactScalarRange(field string, bounds indexdata.Bounds) MaterializedPredKey {
	return materializedPredKeyForExactScalarRangeKind(materializedPredKeyExactScalarRangeComplement, field, bounds)
}

func parseMaterializedPredKeyExactScalarRange(kind materializedPredKeyKind, field, tail string) (MaterializedPredKey, bool) {
	if loTag, rest, ok := strings.Cut(tail, "\x1f"); ok {
		if loVal, rest, ok := strings.Cut(rest, "\x1f"); ok {
			if hiTag, hiVal, ok := strings.Cut(rest, "\x1f"); ok {
				out := MaterializedPredKey{
					kind:  kind,
					field: field,
				}
				if loTag != "" {
					if len(loTag) != 2 {
						return MaterializedPredKey{}, false
					}
					switch loTag[1] {
					case '[':
						out.flags |= materializedPredKeyHasLo | materializedPredKeyLoInc
					case '(':
						out.flags |= materializedPredKeyHasLo
					default:
						return MaterializedPredKey{}, false
					}
					switch loTag[0] {
					case 'n':
						if len(loVal) != 8 {
							return MaterializedPredKey{}, false
						}
						out.flags |= materializedPredKeyLoNumeric
						out.loIndex = keycodec.FromU64(keycodec.Fixed8StringToU64(loVal))
					case 's':
						out.loKey = loVal
					default:
						return MaterializedPredKey{}, false
					}
				}
				if hiTag != "" {
					if len(hiTag) != 2 {
						return MaterializedPredKey{}, false
					}
					switch hiTag[1] {
					case ']':
						out.flags |= materializedPredKeyHasHi | materializedPredKeyHiInc
					case ')':
						out.flags |= materializedPredKeyHasHi
					default:
						return MaterializedPredKey{}, false
					}
					switch hiTag[0] {
					case 'n':
						if len(hiVal) != 8 {
							return MaterializedPredKey{}, false
						}
						out.flags |= materializedPredKeyHiNumeric
						out.hiIndex = keycodec.FromU64(keycodec.Fixed8StringToU64(hiVal))
					case 's':
						out.hiKey = hiVal
					default:
						return MaterializedPredKey{}, false
					}
				}
				if out.flags&(materializedPredKeyHasLo|materializedPredKeyHasHi) == 0 {
					return MaterializedPredKey{}, false
				}
				return out, true
			}
		}
	}
	loRaw, hiRaw, ok := strings.Cut(tail, "\x1f")
	if !ok {
		return MaterializedPredKey{}, false
	}
	out := MaterializedPredKey{
		kind:  kind,
		field: field,
	}
	if loRaw != "" {
		switch loRaw[0] {
		case '[':
			out.flags |= materializedPredKeyHasLo | materializedPredKeyLoInc
		case '(':
			out.flags |= materializedPredKeyHasLo
		default:
			return MaterializedPredKey{}, false
		}
		out.loKey = loRaw[1:]
	}
	if hiRaw != "" {
		switch hiRaw[0] {
		case ']':
			out.flags |= materializedPredKeyHasHi | materializedPredKeyHiInc
		case ')':
			out.flags |= materializedPredKeyHasHi
		default:
			return MaterializedPredKey{}, false
		}
		out.hiKey = hiRaw[1:]
	}
	if out.flags&(materializedPredKeyHasLo|materializedPredKeyHasHi) == 0 {
		return MaterializedPredKey{}, false
	}
	return out, true
}

func MaterializedPredKeyForNumericBucketSpan(field string, startBucket, endBucket int) MaterializedPredKey {
	if field == "" || startBucket < 0 || endBucket < startBucket {
		return MaterializedPredKey{}
	}
	return MaterializedPredKey{
		kind:        materializedPredKeyNumericBucketSpan,
		field:       field,
		startBucket: startBucket,
		endBucket:   endBucket,
	}
}

func MaterializedPredKeyForDistinctSetTerms(field string, op qir.Op, vals []string, includeNil bool) MaterializedPredKey {
	termCount := len(vals)
	if includeNil {
		termCount++
	}
	if field == "" || termCount < 2 {
		return MaterializedPredKey{}
	}
	switch op {
	case qir.OpIN, qir.OpHASANY, qir.OpHASALL:
	default:
		return MaterializedPredKey{}
	}
	if len(vals) > materializedPredKeyDistinctSetInlineMax {
		return MaterializedPredKey{}
	}
	key := MaterializedPredKey{
		kind:  materializedPredKeyDistinctSet,
		field: field,
		op:    op,
	}
	key.setValueCnt = uint8(len(vals))
	for i, v := range vals {
		key.setTerms[i] = v
	}
	if includeNil {
		key.flags |= materializedPredKeySetHasNil
	}
	return key
}

func MaterializedPredKeyFromEncoded(key string) (MaterializedPredKey, bool) {
	if key == "" {
		return MaterializedPredKey{}, false
	}
	f, rem, ok := strings.Cut(key, "\x1f")
	if !ok {
		return MaterializedPredKey{
			kind: materializedPredKeyOpaque,
			raw:  key,
		}, true
	}
	if f == "" {
		return MaterializedPredKey{}, false
	}
	head, tail, ok := strings.Cut(rem, "\x1f")
	if !ok {
		return MaterializedPredKey{}, false
	}

	switch head {
	case "set_exact":
		opStr, rest, ok := strings.Cut(tail, "\x1f")
		if !ok {
			return MaterializedPredKey{}, false
		}
		op, err := strconv.Atoi(opStr)
		if err != nil {
			return MaterializedPredKey{}, false
		}
		if rest == "" {
			return MaterializedPredKey{}, false
		}
		out := MaterializedPredKey{
			kind:  materializedPredKeyDistinctSet,
			field: f,
			op:    qir.Op(op),
		}
		switch rest[0] {
		case '0':
		case '1':
			out.flags |= materializedPredKeySetHasNil
		default:
			return MaterializedPredKey{}, false
		}
		rest = rest[1:]
		if len(rest) > 0 && rest[0] == '\x1f' {
			rest = rest[1:]
		}
		for rest != "" {
			if int(out.setValueCnt) >= len(out.setTerms) {
				return MaterializedPredKey{}, false
			}
			if rest[0] != '\x1e' {
				return MaterializedPredKey{}, false
			}
			rest = rest[1:]
			lenStr, next, ok := strings.Cut(rest, "\x1f")
			if !ok {
				return MaterializedPredKey{}, false
			}
			n, err := strconv.Atoi(lenStr)
			if err != nil || n < 0 || len(next) < n {
				return MaterializedPredKey{}, false
			}
			out.setTerms[out.setValueCnt] = next[:n]
			out.setValueCnt++
			rest = next[n:]
		}
		termCount := int(out.setValueCnt)
		if out.flags&materializedPredKeySetHasNil != 0 {
			termCount++
		}
		if termCount < 2 {
			return MaterializedPredKey{}, false
		}
		return out, true

	case "count_range_complement":
		opStr, scalarKey, ok := strings.Cut(tail, "\x1f")
		if !ok {
			return MaterializedPredKey{}, false
		}
		op, err := strconv.Atoi(opStr)
		if err != nil {
			return MaterializedPredKey{}, false
		}
		if tag, val, ok := strings.Cut(scalarKey, "\x1f"); ok {
			if tag != "n" || len(val) != 8 {
				return MaterializedPredKey{}, false
			}
			return MaterializedPredKey{
				kind:     materializedPredKeyScalarComplement,
				field:    f,
				op:       qir.Op(op),
				keyIndex: keycodec.FromU64(keycodec.Fixed8StringToU64(val)),
				flags:    materializedPredKeyScalarNumeric,
			}, true
		}
		return MaterializedPredKey{
			kind:  materializedPredKeyScalarComplement,
			field: f,
			op:    qir.Op(op),
			key:   scalarKey,
		}, true

	case "range_exact":
		return parseMaterializedPredKeyExactScalarRange(materializedPredKeyExactScalarRange, f, tail)

	case "count_range_exact_complement":
		return parseMaterializedPredKeyExactScalarRange(materializedPredKeyExactScalarRangeComplement, f, tail)

	case "range_bucket":
		startStr, endStr, ok := strings.Cut(tail, "\x1f")
		if !ok {
			return MaterializedPredKey{}, false
		}
		startBucket, err := strconv.Atoi(startStr)
		if err != nil {
			return MaterializedPredKey{}, false
		}
		endBucket, err := strconv.Atoi(endStr)
		if err != nil {
			return MaterializedPredKey{}, false
		}
		out := MaterializedPredKeyForNumericBucketSpan(f, startBucket, endBucket)
		return out, !out.IsZero()

	default:
		op, err := strconv.Atoi(head)
		if err != nil {
			return MaterializedPredKey{}, false
		}
		if tag, val, ok := strings.Cut(tail, "\x1f"); ok {
			if tag != "n" || len(val) != 8 {
				return MaterializedPredKey{}, false
			}
			return MaterializedPredKey{
				kind:     materializedPredKeyScalar,
				field:    f,
				op:       qir.Op(op),
				keyIndex: keycodec.FromU64(keycodec.Fixed8StringToU64(val)),
				flags:    materializedPredKeyScalarNumeric,
			}, true
		}
		return MaterializedPredKey{
			kind:  materializedPredKeyScalar,
			field: f,
			op:    qir.Op(op),
			key:   tail,
		}, true
	}
}

func MaterializedPredKeyFromOpaque(raw string) MaterializedPredKey {
	if raw == "" {
		return MaterializedPredKey{}
	}
	return MaterializedPredKey{
		kind: materializedPredKeyOpaque,
		raw:  raw,
	}
}

func GetMaterializedPredKeySlice(capHint int) []MaterializedPredKey {
	return materializedPredKeySlicePool.Get(capHint)
}

func ReleaseMaterializedPredKeySlice(s []MaterializedPredKey) {
	materializedPredKeySlicePool.Put(s)
}
