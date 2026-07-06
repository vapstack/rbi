package indexdata

import (
	"math/bits"
	"slices"
	"time"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	fieldIndexSingleChunkCap                 = 32768
	fieldIndexCompactAsSinglesAdaptiveMaxLen = 200_000
	fieldIndexSingleInlineCap                = 16
)

type fieldIndexPostingUnionBuilder struct {
	ids             posting.List
	singleIDs       posting.List
	singles         []uint64
	inlineSingles   [fieldIndexSingleInlineCap]uint64
	inlineLen       int
	lastSingle      uint64
	maxSingle       uint64
	singlesUnsorted bool
	batchCompact    bool
}

type RangePostingsUnionCancel struct {
	CurrentSeq    func() uint64
	Seq           uint64
	NextProbe     int64
	ProbeInterval int64
	Probes        uint32
}

func fieldIndexPostingBatchCompactEnabled(capHint uint64) bool {
	return capHint > posting.SmallCap && capHint <= fieldIndexCompactAsSinglesAdaptiveMaxLen
}

func (c *RangePostingsUnionCancel) Check() bool {
	c.Probes++
	if c.Probes&63 != 0 {
		return false
	}
	now := time.Now().UnixNano()
	if now < c.NextProbe {
		return false
	}
	c.NextProbe = now + c.ProbeInterval
	return c.Canceled()
}

func (c *RangePostingsUnionCancel) Canceled() bool {
	return c.CurrentSeq() != c.Seq
}

func newFieldIndexPostingUnionBuilder(batchCompact bool) fieldIndexPostingUnionBuilder {
	return fieldIndexPostingUnionBuilder{batchCompact: batchCompact}
}

func (b *fieldIndexPostingUnionBuilder) flushSingles() {
	if b.singles != nil {
		if len(b.singles) == 0 {
			return
		}
		if b.singlesUnsorted {
			sortFieldIndexSingles(b.singles, b.maxSingle)
		}
		b.singleIDs = b.singleIDs.BuildAddedMany(b.singles)
		b.singles = b.singles[:0]
		b.lastSingle = 0
		b.maxSingle = 0
		b.singlesUnsorted = false

	} else {
		if b.inlineLen == 0 {
			return
		}
		singles := b.inlineSingles[:b.inlineLen]
		if b.singlesUnsorted {
			sortFieldIndexSingles(singles, b.maxSingle)
		}
		b.singleIDs = b.singleIDs.BuildAddedMany(singles)
		b.inlineLen = 0
		b.lastSingle = 0
		b.maxSingle = 0
		b.singlesUnsorted = false
	}
}

func (b *fieldIndexPostingUnionBuilder) flushSinglesUntil(cancel *RangePostingsUnionCancel) bool {
	if b.singles != nil {
		if len(b.singles) == 0 {
			return true
		}
		if b.singlesUnsorted && !sortFieldIndexSinglesUntil(b.singles, b.maxSingle, cancel) {
			return false
		}
		b.singleIDs = b.singleIDs.BuildAddedMany(b.singles)
		if cancel.Canceled() {
			return false
		}
		b.singles = b.singles[:0]
		b.lastSingle = 0
		b.maxSingle = 0
		b.singlesUnsorted = false
		return true
	}

	if b.inlineLen == 0 {
		return true
	}
	singles := b.inlineSingles[:b.inlineLen]
	if b.singlesUnsorted && !sortFieldIndexSinglesUntil(singles, b.maxSingle, cancel) {
		return false
	}
	b.singleIDs = b.singleIDs.BuildAddedMany(singles)
	if cancel.Canceled() {
		return false
	}
	b.inlineLen = 0
	b.lastSingle = 0
	b.maxSingle = 0
	b.singlesUnsorted = false
	return true
}

func (b *fieldIndexPostingUnionBuilder) addSingle(idx uint64) {
	if b.inlineLen != 0 || len(b.singles) != 0 {
		b.singlesUnsorted = b.singlesUnsorted || idx < b.lastSingle
	}
	b.lastSingle = idx
	if idx > b.maxSingle {
		b.maxSingle = idx
	}

	if b.singles == nil {
		if b.inlineLen < len(b.inlineSingles) {
			b.inlineSingles[b.inlineLen] = idx
			b.inlineLen++
			return
		}
		b.singles = pooled.GetUint64Slice(fieldIndexSingleChunkCap)
		b.singles = append(b.singles, b.inlineSingles[:]...)
		b.inlineLen = 0
	}
	b.singles = append(b.singles, idx)
	if len(b.singles) == cap(b.singles) {
		b.flushSingles()
	}
}

func (b *fieldIndexPostingUnionBuilder) addPosting(ids posting.List) {
	if ids.IsEmpty() {
		return
	}
	if b.batchCompact {
		var compact [posting.MidCap]uint64
		if values, ok := ids.TryAppendCompactTo(compact[:0]); ok {
			for _, idx := range values {
				b.addSingle(idx)
			}
			return
		}
	}
	if idx, ok := ids.TrySingle(); ok {
		b.addSingle(idx)
		return
	}
	if b.ids.IsEmpty() {
		b.ids = ids.Borrow()
		return
	}
	b.ids = b.ids.BuildOr(ids)
}

func (b *fieldIndexPostingUnionBuilder) finish(optimize bool) posting.List {
	b.flushSingles()
	out := b.ids
	if !b.singleIDs.IsEmpty() {
		singleIDs := b.singleIDs
		b.singleIDs = posting.List{}
		if out.IsEmpty() {
			out = singleIDs
		} else {
			out = out.BuildOr(singleIDs)
			singleIDs.Release()
		}
	}
	if out.IsBorrowed() {
		out = out.Clone()
	}
	b.ids = posting.List{}
	b.inlineLen = 0
	b.lastSingle = 0
	b.maxSingle = 0
	b.singlesUnsorted = false
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}
	if optimize {
		return out.BuildOptimized()
	}
	return out
}

func (b *fieldIndexPostingUnionBuilder) finishUntil(optimize bool, cancel *RangePostingsUnionCancel) (posting.List, bool) {
	if cancel.Canceled() {
		b.release()
		return posting.List{}, false
	}
	if !b.flushSinglesUntil(cancel) {
		b.release()
		return posting.List{}, false
	}
	if cancel.Canceled() {
		b.release()
		return posting.List{}, false
	}

	out := b.ids
	b.ids = posting.List{}
	if !b.singleIDs.IsEmpty() {
		singleIDs := b.singleIDs
		b.singleIDs = posting.List{}
		if out.IsEmpty() {
			out = singleIDs
		} else {
			if cancel.Canceled() {
				out.Release()
				singleIDs.Release()
				b.release()
				return posting.List{}, false
			}
			out = out.BuildOr(singleIDs)
			singleIDs.Release()
		}
	}
	if cancel.Canceled() {
		out.Release()
		b.release()
		return posting.List{}, false
	}
	if out.IsBorrowed() {
		out = out.Clone()
	}
	b.inlineLen = 0
	b.lastSingle = 0
	b.maxSingle = 0
	b.singlesUnsorted = false
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}
	if cancel.Canceled() {
		out.Release()
		return posting.List{}, false
	}
	if optimize {
		out = out.BuildOptimized()
		if cancel.Canceled() {
			out.Release()
			return posting.List{}, false
		}
	}
	return out, true
}

func (b *fieldIndexPostingUnionBuilder) release() {
	b.ids.Release()
	b.singleIDs.Release()
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
	}
	*b = fieldIndexPostingUnionBuilder{}
}

func sortFieldIndexSinglesUntil(ids []uint64, maxID uint64, cancel *RangePostingsUnionCancel) bool {
	if len(ids) < 1024 {
		if cancel.Canceled() {
			return false
		}
		slices.Sort(ids)
		return !cancel.Canceled()
	}
	passes := (bits.Len64(maxID) + 7) >> 3
	if passes == 0 {
		return true
	}
	if cancel.Canceled() {
		return false
	}
	buf := pooled.GetUint64Slice(len(ids))[:len(ids)]
	src := ids
	dst := buf
	ok := true

	for pass := 0; pass < passes; pass++ {
		if cancel.Canceled() {
			ok = false
			break
		}
		var offsets [256]int
		shift := uint(pass << 3)
		for i := 0; i < len(src); i++ {
			offsets[byte(src[i]>>shift)]++
		}
		sum := 0
		for i := 0; i < len(offsets); i++ {
			count := offsets[i]
			offsets[i] = sum
			sum += count
		}
		if cancel.Canceled() {
			ok = false
			break
		}
		for i := 0; i < len(src); i++ {
			v := src[i]
			bucket := byte(v >> shift)
			dst[offsets[bucket]] = v
			offsets[bucket]++
		}
		src, dst = dst, src
	}
	if ok && passes&1 != 0 {
		if cancel.Canceled() {
			ok = false
		} else {
			copy(ids, src)
		}
	}
	pooled.ReleaseUint64Slice(buf)
	return ok && !cancel.Canceled()
}

func sortFieldIndexSingles(ids []uint64, maxID uint64) {
	if len(ids) < 1024 {
		slices.Sort(ids)
		return
	}
	passes := (bits.Len64(maxID) + 7) >> 3
	if passes == 0 {
		return
	}
	buf := pooled.GetUint64Slice(len(ids))[:len(ids)]
	src := ids
	dst := buf

	for pass := 0; pass < passes; pass++ {
		var offsets [256]int
		shift := uint(pass << 3)
		for i := 0; i < len(src); i++ {
			offsets[byte(src[i]>>shift)]++
		}
		sum := 0
		for i := 0; i < len(offsets); i++ {
			count := offsets[i]
			offsets[i] = sum
			sum += count
		}
		for i := 0; i < len(src); i++ {
			v := src[i]
			bucket := byte(v >> shift)
			dst[offsets[bucket]] = v
			offsets[bucket]++
		}
		src, dst = dst, src
	}
	if passes&1 != 0 {
		copy(ids, src)
	}
	pooled.ReleaseUint64Slice(buf)
}

func (o FieldIndexView) appendRangePostingsUnion(builder *fieldIndexPostingUnionBuilder, br FieldIndexRange) {
	if br.Empty() {
		return
	}
	cur := o.NewCursor(br, false)
	for {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			builder.addSingle(idx)
			continue
		}
		if ids.IsEmpty() {
			continue
		}
		builder.addPosting(ids)
	}
}

func (o FieldIndexView) appendRangePostingsUnionUntil(builder *fieldIndexPostingUnionBuilder, br FieldIndexRange, cancel *RangePostingsUnionCancel) bool {
	if br.Empty() {
		return true
	}
	cur := o.NewCursor(br, false)
	for {
		if cancel.Check() {
			return false
		}
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			return true
		}
		if single {
			builder.addSingle(idx)
			continue
		}
		if ids.IsEmpty() {
			continue
		}
		builder.addPosting(ids)
	}
}

func (o FieldIndexView) unionRangePostingsBatchCompactEnabled(first, second FieldIndexRange) bool {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return false
	}
	if totalSpan <= fieldIndexCompactAsSinglesAdaptiveMaxLen {
		return true
	}
	_, estFirst := o.RangeStats(first)
	_, estSecond := o.RangeStats(second)

	return fieldIndexPostingBatchCompactEnabled(mathutil.SatAddUint64(estFirst, estSecond))
}

func (o FieldIndexView) mergeRangePostingsBatchCompactEnabled(first, second FieldIndexRange, totalSpan int) bool {
	if totalSpan <= fieldIndexCompactAsSinglesAdaptiveMaxLen {
		return true
	}
	if first.filterPrefixSet || second.filterPrefixSet {
		return false
	}
	bucketsFirst, rowsFirst := o.RangeStats(first)
	bucketsSecond, rowsSecond := o.RangeStats(second)
	buckets := bucketsFirst + bucketsSecond
	if buckets == 0 {
		return false
	}
	rows := mathutil.SatAddUint64(rowsFirst, rowsSecond)
	if rows <= posting.SmallCap {
		return false
	}
	return rows <= uint64(buckets)*posting.MidCap
}

func (o FieldIndexView) UnionRangePostings(first, second FieldIndexRange) posting.List {
	if first.Empty() && second.Empty() {
		return posting.List{}
	}
	builder := newFieldIndexPostingUnionBuilder(o.unionRangePostingsBatchCompactEnabled(first, second))
	o.appendRangePostingsUnion(&builder, first)
	o.appendRangePostingsUnion(&builder, second)

	return builder.finish(true)
}

func (o FieldIndexView) UnionRangePostingsUntil(first, second FieldIndexRange, cancel *RangePostingsUnionCancel) (posting.List, bool) {
	if first.Empty() && second.Empty() {
		return posting.List{}, true
	}
	builder := newFieldIndexPostingUnionBuilder(o.unionRangePostingsBatchCompactEnabled(first, second))
	if !o.appendRangePostingsUnionUntil(&builder, first, cancel) ||
		!o.appendRangePostingsUnionUntil(&builder, second, cancel) {
		builder.release()
		return posting.List{}, false
	}
	return builder.finishUntil(true, cancel)
}

func (o FieldIndexView) MergeRangePostingsInto(dst posting.List, first, second FieldIndexRange) posting.List {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return dst
	}
	const mergeFieldIndexRangeDirectMaxBuckets = 32

	builder := newFieldIndexPostingUnionBuilder(o.mergeRangePostingsBatchCompactEnabled(first, second, totalSpan))
	if !dst.IsEmpty() && totalSpan > mergeFieldIndexRangeDirectMaxBuckets {
		o.appendRangePostingsUnion(&builder, first)
		o.appendRangePostingsUnion(&builder, second)
		return dst.BuildMergedOwned(builder.finish(false))
	}
	builder.ids = dst

	o.appendRangePostingsUnion(&builder, first)
	o.appendRangePostingsUnion(&builder, second)

	return builder.finish(false)
}
