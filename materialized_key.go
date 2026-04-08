package rbi

import (
	"strconv"
	"strings"

	"github.com/vapstack/qx"
)

type materializedPredKeyKind uint8

const (
	materializedPredKeyNone materializedPredKeyKind = iota
	materializedPredKeyOpaque
	materializedPredKeyScalar
	materializedPredKeyScalarComplement
	materializedPredKeyExactScalarRange
	materializedPredKeyNumericBucketSpan
)

const (
	materializedPredKeyHasLo uint8 = 1 << iota
	materializedPredKeyHasHi
	materializedPredKeyLoInc
	materializedPredKeyHiInc
	materializedPredKeyLoNumeric
	materializedPredKeyHiNumeric
	materializedPredKeyScalarNumeric
)

type materializedPredKey struct {
	kind        materializedPredKeyKind
	raw         string
	field       string
	op          qx.Op
	key         string
	keyIndex    indexKey
	loKey       string
	hiKey       string
	loIndex     indexKey
	hiIndex     indexKey
	flags       uint8
	startBucket int
	endBucket   int
}

func (key materializedPredKey) isZero() bool {
	switch key.kind {
	case materializedPredKeyNone:
		return true
	case materializedPredKeyOpaque:
		return key.raw == ""
	default:
		return key.field == ""
	}
}

func (key materializedPredKey) String() string {
	if key.isZero() {
		return ""
	}
	switch key.kind {
	case materializedPredKeyOpaque:
		return key.raw
	case materializedPredKeyScalar:
		if key.flags&materializedPredKeyScalarNumeric != 0 {
			return key.field + "\x1f" + strconv.Itoa(int(key.op)) + "\x1f" + "n" + "\x1f" + key.keyIndex.asUnsafeString()
		}
		return key.field + "\x1f" + strconv.Itoa(int(key.op)) + "\x1f" + key.key
	case materializedPredKeyScalarComplement:
		if key.flags&materializedPredKeyScalarNumeric != 0 {
			return key.field + "\x1f" + "count_range_complement" + "\x1f" +
				strconv.Itoa(int(key.op)) + "\x1f" + "n" + "\x1f" + key.keyIndex.asUnsafeString()
		}
		return key.field + "\x1f" + "count_range_complement" + "\x1f" +
			strconv.Itoa(int(key.op)) + "\x1f" + key.key
	case materializedPredKeyExactScalarRange:
		loTag := ""
		loVal := ""
		hiTag := ""
		hiVal := ""
		if key.flags&materializedPredKeyHasLo != 0 {
			if key.flags&materializedPredKeyLoNumeric != 0 {
				loTag = "n"
				loVal = key.loIndex.asUnsafeString()
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
				hiVal = key.hiIndex.asUnsafeString()
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
		return key.field + "\x1f" + "range_exact" + "\x1f" +
			loTag + "\x1f" + loVal + "\x1f" + hiTag + "\x1f" + hiVal
	case materializedPredKeyNumericBucketSpan:
		return key.field + "\x1f" + "range_bucket" + "\x1f" +
			strconv.Itoa(key.startBucket) + "\x1f" + strconv.Itoa(key.endBucket)
	default:
		return ""
	}
}

func materializedPredKeyForScalar(field string, op qx.Op, key string) materializedPredKey {
	if field == "" || !isMaterializedScalarCacheOp(op) {
		return materializedPredKey{}
	}
	return materializedPredKey{
		kind:  materializedPredKeyScalar,
		field: field,
		op:    op,
		key:   key,
	}
}

func materializedPredKeyForNumericScalar(field string, op qx.Op, key indexKey) materializedPredKey {
	if field == "" || !key.isNumeric() || !isMaterializedScalarCacheOp(op) {
		return materializedPredKey{}
	}
	return materializedPredKey{
		kind:     materializedPredKeyScalar,
		field:    field,
		op:       op,
		keyIndex: key,
		flags:    materializedPredKeyScalarNumeric,
	}
}

func materializedPredComplementKeyForScalar(field string, op qx.Op, key string) materializedPredKey {
	if field == "" || key == "" || !isNumericRangeOp(op) {
		return materializedPredKey{}
	}
	return materializedPredKey{
		kind:  materializedPredKeyScalarComplement,
		field: field,
		op:    op,
		key:   key,
	}
}

func materializedPredComplementKeyForNumericScalar(field string, op qx.Op, key indexKey) materializedPredKey {
	if field == "" || !key.isNumeric() || !isNumericRangeOp(op) {
		return materializedPredKey{}
	}
	return materializedPredKey{
		kind:     materializedPredKeyScalarComplement,
		field:    field,
		op:       op,
		keyIndex: key,
		flags:    materializedPredKeyScalarNumeric,
	}
}

func materializedPredKeyForExactScalarRange(field string, bounds rangeBounds) materializedPredKey {
	if field == "" || bounds.empty || (!bounds.hasLo && !bounds.hasHi) {
		return materializedPredKey{}
	}
	var flags uint8
	if bounds.hasLo {
		flags |= materializedPredKeyHasLo
		if bounds.loInc {
			flags |= materializedPredKeyLoInc
		}
		if bounds.loNumeric {
			flags |= materializedPredKeyLoNumeric
		}
	}
	if bounds.hasHi {
		flags |= materializedPredKeyHasHi
		if bounds.hiInc {
			flags |= materializedPredKeyHiInc
		}
		if bounds.hiNumeric {
			flags |= materializedPredKeyHiNumeric
		}
	}
	key := materializedPredKey{
		kind:  materializedPredKeyExactScalarRange,
		field: field,
		flags: flags,
	}
	if bounds.hasLo {
		if bounds.loNumeric {
			key.loIndex = bounds.loIndex
		} else {
			key.loKey = bounds.loKey
		}
	}
	if bounds.hasHi {
		if bounds.hiNumeric {
			key.hiIndex = bounds.hiIndex
		} else {
			key.hiKey = bounds.hiKey
		}
	}
	return key
}

func materializedPredKeyForNumericBucketSpan(field string, startBucket, endBucket int) materializedPredKey {
	if field == "" || startBucket < 0 || endBucket < startBucket {
		return materializedPredKey{}
	}
	return materializedPredKey{
		kind:        materializedPredKeyNumericBucketSpan,
		field:       field,
		startBucket: startBucket,
		endBucket:   endBucket,
	}
}

func materializedPredKeyFromEncoded(key string) (materializedPredKey, bool) {
	if key == "" {
		return materializedPredKey{}, false
	}
	field, rest, ok := strings.Cut(key, "\x1f")
	if !ok {
		return materializedPredKey{
			kind: materializedPredKeyOpaque,
			raw:  key,
		}, true
	}
	if field == "" {
		return materializedPredKey{}, false
	}
	head, tail, ok := strings.Cut(rest, "\x1f")
	if !ok {
		return materializedPredKey{}, false
	}
	switch head {
	case "count_range_complement":
		opStr, scalarKey, ok := strings.Cut(tail, "\x1f")
		if !ok {
			return materializedPredKey{}, false
		}
		op, err := strconv.Atoi(opStr)
		if err != nil {
			return materializedPredKey{}, false
		}
		if tag, val, ok := strings.Cut(scalarKey, "\x1f"); ok {
			if tag != "n" || len(val) != 8 {
				return materializedPredKey{}, false
			}
			return materializedPredKey{
				kind:     materializedPredKeyScalarComplement,
				field:    field,
				op:       qx.Op(op),
				keyIndex: indexKeyFromU64(fixed8StringToU64(val)),
				flags:    materializedPredKeyScalarNumeric,
			}, true
		}
		return materializedPredKey{
			kind:  materializedPredKeyScalarComplement,
			field: field,
			op:    qx.Op(op),
			key:   scalarKey,
		}, true
	case "range_exact":
		if loTag, rest, ok := strings.Cut(tail, "\x1f"); ok {
			if loVal, rest, ok := strings.Cut(rest, "\x1f"); ok {
				if hiTag, hiVal, ok := strings.Cut(rest, "\x1f"); ok {
					out := materializedPredKey{
						kind:  materializedPredKeyExactScalarRange,
						field: field,
					}
					if loTag != "" {
						if len(loTag) != 2 {
							return materializedPredKey{}, false
						}
						switch loTag[1] {
						case '[':
							out.flags |= materializedPredKeyHasLo | materializedPredKeyLoInc
						case '(':
							out.flags |= materializedPredKeyHasLo
						default:
							return materializedPredKey{}, false
						}
						switch loTag[0] {
						case 'n':
							if len(loVal) != 8 {
								return materializedPredKey{}, false
							}
							out.flags |= materializedPredKeyLoNumeric
							out.loIndex = indexKeyFromU64(fixed8StringToU64(loVal))
						case 's':
							out.loKey = loVal
						default:
							return materializedPredKey{}, false
						}
					}
					if hiTag != "" {
						if len(hiTag) != 2 {
							return materializedPredKey{}, false
						}
						switch hiTag[1] {
						case ']':
							out.flags |= materializedPredKeyHasHi | materializedPredKeyHiInc
						case ')':
							out.flags |= materializedPredKeyHasHi
						default:
							return materializedPredKey{}, false
						}
						switch hiTag[0] {
						case 'n':
							if len(hiVal) != 8 {
								return materializedPredKey{}, false
							}
							out.flags |= materializedPredKeyHiNumeric
							out.hiIndex = indexKeyFromU64(fixed8StringToU64(hiVal))
						case 's':
							out.hiKey = hiVal
						default:
							return materializedPredKey{}, false
						}
					}
					if out.flags&(materializedPredKeyHasLo|materializedPredKeyHasHi) == 0 {
						return materializedPredKey{}, false
					}
					return out, true
				}
			}
		}
		loRaw, hiRaw, ok := strings.Cut(tail, "\x1f")
		if !ok {
			return materializedPredKey{}, false
		}
		out := materializedPredKey{
			kind:  materializedPredKeyExactScalarRange,
			field: field,
		}
		if loRaw != "" {
			switch loRaw[0] {
			case '[':
				out.flags |= materializedPredKeyHasLo | materializedPredKeyLoInc
			case '(':
				out.flags |= materializedPredKeyHasLo
			default:
				return materializedPredKey{}, false
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
				return materializedPredKey{}, false
			}
			out.hiKey = hiRaw[1:]
		}
		if out.flags&(materializedPredKeyHasLo|materializedPredKeyHasHi) == 0 {
			return materializedPredKey{}, false
		}
		return out, true
	case "range_bucket":
		startStr, endStr, ok := strings.Cut(tail, "\x1f")
		if !ok {
			return materializedPredKey{}, false
		}
		startBucket, err := strconv.Atoi(startStr)
		if err != nil {
			return materializedPredKey{}, false
		}
		endBucket, err := strconv.Atoi(endStr)
		if err != nil {
			return materializedPredKey{}, false
		}
		out := materializedPredKeyForNumericBucketSpan(field, startBucket, endBucket)
		return out, !out.isZero()
	default:
		op, err := strconv.Atoi(head)
		if err != nil {
			return materializedPredKey{}, false
		}
		if tag, val, ok := strings.Cut(tail, "\x1f"); ok {
			if tag != "n" || len(val) != 8 {
				return materializedPredKey{}, false
			}
			return materializedPredKey{
				kind:     materializedPredKeyScalar,
				field:    field,
				op:       qx.Op(op),
				keyIndex: indexKeyFromU64(fixed8StringToU64(val)),
				flags:    materializedPredKeyScalarNumeric,
			}, true
		}
		return materializedPredKey{
			kind:  materializedPredKeyScalar,
			field: field,
			op:    qx.Op(op),
			key:   tail,
		}, true
	}
}
