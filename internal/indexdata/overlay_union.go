package indexdata

import (
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const (
	overlaySingleChunkCap       = 32768
	overlaySingleAdaptiveMaxLen = 200_000
	overlaySingleInlineCap      = 16
)

type overlayPostingUnionBuilder struct {
	ids           posting.List
	singles       []uint64
	inlineSingles [overlaySingleInlineCap]uint64
	inlineLen     int
	batchSingles  bool
}

func overlayPostingBatchSinglesEnabled(capHint uint64) bool {
	return capHint > posting.SmallCap && capHint <= overlaySingleAdaptiveMaxLen
}

func newOverlayPostingUnionBuilder(batchSingles bool) overlayPostingUnionBuilder {
	return overlayPostingUnionBuilder{batchSingles: batchSingles}
}

func (b *overlayPostingUnionBuilder) flushSingles() {
	if b.singles != nil {
		if len(b.singles) == 0 {
			return
		}
		b.ids = b.ids.BuildAddedMany(b.singles)
		b.singles = b.singles[:0]
		return
	}
	if b.inlineLen == 0 {
		return
	}
	b.ids = b.ids.BuildAddedMany(b.inlineSingles[:b.inlineLen])
	b.inlineLen = 0
}

func (b *overlayPostingUnionBuilder) addSingle(idx uint64) {
	if !b.batchSingles {
		b.ids = b.ids.BuildAdded(idx)
		return
	}
	if b.singles == nil {
		if b.inlineLen < len(b.inlineSingles) {
			b.inlineSingles[b.inlineLen] = idx
			b.inlineLen++
			return
		}
		b.singles = pooled.GetUint64Slice(overlaySingleChunkCap)
		b.singles = append(b.singles, b.inlineSingles[:]...)
		b.inlineLen = 0
	}
	b.singles = append(b.singles, idx)
	if len(b.singles) == cap(b.singles) {
		b.flushSingles()
	}
}

func (b *overlayPostingUnionBuilder) addPosting(ids posting.List) {
	if ids.IsEmpty() {
		return
	}
	if b.batchSingles {
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
	b.flushSingles()
	if b.ids.IsEmpty() {
		b.ids = ids.Borrow()
		return
	}
	b.ids = b.ids.BuildOr(ids)
}

func (b *overlayPostingUnionBuilder) finish(optimize bool) posting.List {
	b.flushSingles()
	out := b.ids
	if out.IsBorrowed() {
		out = out.Clone()
	}
	b.ids = posting.List{}
	b.inlineLen = 0
	if b.singles != nil {
		pooled.PutUint64Slice(b.singles)
		b.singles = nil
	}
	if optimize {
		return out.BuildOptimized()
	}
	return out
}

func (o FieldOverlay) appendRangePostingsUnion(builder *overlayPostingUnionBuilder, br OverlayRange) {
	if br.Empty() {
		return
	}
	cur := o.NewCursor(br, false)
	for {
		_, ids, ok := cur.Next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		builder.addPosting(ids)
	}
}

func (o FieldOverlay) unionRangePostingsBatchSinglesEnabled(first, second OverlayRange) bool {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return false
	}
	if totalSpan <= overlaySingleAdaptiveMaxLen {
		return true
	}
	_, estFirst := o.RangeStats(first)
	_, estSecond := o.RangeStats(second)

	return overlayPostingBatchSinglesEnabled(satAddUint64(estFirst, estSecond))
}

func satAddUint64(total, add uint64) uint64 {
	if ^uint64(0)-total < add {
		return ^uint64(0)
	}
	return total + add
}

func (o FieldOverlay) UnionRangePostings(first, second OverlayRange) posting.List {
	if first.Empty() && second.Empty() {
		return posting.List{}
	}
	builder := newOverlayPostingUnionBuilder(o.unionRangePostingsBatchSinglesEnabled(first, second))
	o.appendRangePostingsUnion(&builder, first)
	o.appendRangePostingsUnion(&builder, second)

	return builder.finish(true)
}

func (o FieldOverlay) MergeRangePostingsInto(dst posting.List, first, second OverlayRange) posting.List {
	totalSpan := first.Len() + second.Len()
	if totalSpan == 0 {
		return dst
	}
	const mergeOverlayRangeDirectMaxBuckets = 32

	builder := newOverlayPostingUnionBuilder(totalSpan <= overlaySingleAdaptiveMaxLen)
	if !dst.IsEmpty() && totalSpan > mergeOverlayRangeDirectMaxBuckets {
		o.appendRangePostingsUnion(&builder, first)
		o.appendRangePostingsUnion(&builder, second)
		return dst.BuildMergedOwned(builder.finish(false))
	}
	builder.ids = dst

	o.appendRangePostingsUnion(&builder, first)
	o.appendRangePostingsUnion(&builder, second)

	return builder.finish(false)
}
