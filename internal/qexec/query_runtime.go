package qexec

import (
	"math/bits"

	"github.com/vapstack/rbi/internal/indexdata"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/snapshot"
)

type postsAnyFilterState struct {
	postsBuf              []posting.List
	ids                   posting.List
	containsCalls         int
	containsMaterializeAt int
	applyObservedSavings  uint64
	neg                   bool
	reuse                 materializedPredReuse
}

var postsAnyFilterStatePool = pooled.Pointers[postsAnyFilterState]{
	Cleanup: func(state *postsAnyFilterState) {
		state.ids.Release()
	},
	Clear: true,
}

var lazyMaterializedPredicateStatePool = pooled.Pointers[lazyMaterializedPredicateState]{
	Cleanup: func(state *lazyMaterializedPredicateState) {
		state.ids.Release()
	},
	Clear: true,
}

type lazyMaterializedPredicateLoader interface {
	evalLazyMaterializedPredicateWithKey(raw qir.Expr, cacheKey qcache.MaterializedPredKey) posting.List
}

func (state *postsAnyFilterState) materialize() posting.List {
	if !state.ids.IsEmpty() {
		return state.ids
	}
	if cached, ok := state.reuse.load(); ok {
		state.ids = cached
		return state.ids
	}
	if isSingletonHeavyPostingBuf(state.postsBuf) {
		state.ids = materializePostingBufFast(state.postsBuf)
	} else {
		state.ids = materializePostingUnionBufOwned(state.postsBuf)
	}
	state.ids = state.reuse.share(state.ids)
	return state.ids
}

func postsAnyContainsMaterializeAfterBuf(posts []posting.List) int {
	termCount := len(posts)
	if termCount <= 1 {
		return 0
	}

	sampleN := termCount
	if sampleN > 8 {
		sampleN = 8
	}

	step := 1
	if termCount > sampleN {
		step = (termCount - 1) / (sampleN - 1)
		if step < 1 {
			step = 1
		}
	}

	var sampleTotal uint64
	for i, sampled := 0, 0; i < termCount && sampled < sampleN; i, sampled = i+step, sampled+1 {
		sampleTotal += posts[i].Cardinality()
	}

	if sampleTotal == 0 {
		return 0
	}

	estTotal := sampleTotal
	if sampleN < termCount {
		estTotal = (sampleTotal * uint64(termCount)) / uint64(sampleN)
	}

	avgPostingCard := estTotal / uint64(termCount)
	if avgPostingCard == 0 {
		avgPostingCard = 1
	}

	scanCost := uint64(termCount)
	scanCost *= uint64(max(bits.Len64(avgPostingCard+1), 1))
	if scanCost == 0 {
		scanCost = 1
	}

	threshold := estTotal / scanCost
	if estTotal%scanCost != 0 {
		threshold++
	}
	if threshold < 1 {
		threshold = 1
	}

	maxInt := int(^uint(0) >> 1)
	if threshold > uint64(maxInt) {
		return maxInt
	}
	return int(threshold)
}

func postsAnyContainsWork(posts []posting.List) (uint64, uint64, uint64, bool) {
	termCount := len(posts)
	if termCount <= 1 {
		return 0, 0, 0, false
	}

	unionCard, singletons := postsAnyEstimatedUnionCardinality(posts)
	if unionCard == 0 {
		return 0, 0, 0, false
	}

	buildWork := postingUnionLinearWork(termCount, unionCard)
	if shouldUseFastSinglesUnion(termCount, singletons, unionCard) {
		buildWork = postingUnionFastSinglesWork(termCount, singletons, unionCard)
	}

	directWork := uint64(0)
	for i := 0; i < len(posts); i++ {
		directWork = satAddUint64(directWork, postingContainsLookupWork(posts[i].Cardinality()))
	}

	return buildWork, directWork, postingContainsLookupWork(unionCard), true
}

func (state *postsAnyFilterState) setExpectedContainsCalls(expectedCalls int) {
	if !state.ids.IsEmpty() || expectedCalls <= 0 {
		return
	}

	buildWork, directPerCall, materializedPerCall, ok := postsAnyContainsWork(state.postsBuf)
	if !ok || directPerCall <= materializedPerCall {
		return
	}

	directTotal := satMulUint64(uint64(expectedCalls), directPerCall)
	materializedTotal := satAddUint64(
		satMulUint64(buildWork, materializedShareStructuralFactor(len(state.postsBuf))),
		satMulUint64(uint64(expectedCalls), materializedPerCall),
	)

	if materializedTotal < directTotal {
		state.containsMaterializeAt = 1
	}
}

func (state *postsAnyFilterState) matches(idx uint64) bool {
	if !state.neg {
		if !state.ids.IsEmpty() {
			return state.ids.Contains(idx)
		}

		if state.containsMaterializeAt > 0 {
			state.containsCalls++
			if state.containsCalls >= state.containsMaterializeAt {
				return state.materialize().Contains(idx)
			}
		}

		switch len(state.postsBuf) {
		case 0:
			return false
		case 1:
			return state.postsBuf[0].Contains(idx)
		case 2:
			return state.postsBuf[0].Contains(idx) || state.postsBuf[1].Contains(idx)
		case 3:
			return state.postsBuf[0].Contains(idx) ||
				state.postsBuf[1].Contains(idx) ||
				state.postsBuf[2].Contains(idx)
		case 4:
			return state.postsBuf[0].Contains(idx) ||
				state.postsBuf[1].Contains(idx) ||
				state.postsBuf[2].Contains(idx) ||
				state.postsBuf[3].Contains(idx)
		}
		for i := 0; i < len(state.postsBuf); i++ {
			if state.postsBuf[i].Contains(idx) {
				return true
			}
		}
		return false
	}

	if !state.ids.IsEmpty() {
		return !state.ids.Contains(idx)
	}

	if state.containsMaterializeAt > 0 {
		state.containsCalls++
		if state.containsCalls >= state.containsMaterializeAt {
			return !state.materialize().Contains(idx)
		}
	}

	switch len(state.postsBuf) {
	case 0:
		return true
	case 1:
		return !state.postsBuf[0].Contains(idx)
	case 2:
		return !state.postsBuf[0].Contains(idx) && !state.postsBuf[1].Contains(idx)
	case 3:
		return !state.postsBuf[0].Contains(idx) &&
			!state.postsBuf[1].Contains(idx) &&
			!state.postsBuf[2].Contains(idx)
	case 4:
		return !state.postsBuf[0].Contains(idx) &&
			!state.postsBuf[1].Contains(idx) &&
			!state.postsBuf[2].Contains(idx) &&
			!state.postsBuf[3].Contains(idx)
	}
	for i := 0; i < len(state.postsBuf); i++ {
		if state.postsBuf[i].Contains(idx) {
			return false
		}
	}
	return true
}

func (state *postsAnyFilterState) countBucket(bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if !state.neg {
		if !state.ids.IsEmpty() || state.containsMaterializeAt == 1 {
			return state.materialize().AndCardinality(bucket), true
		}
		return countBucketPostsAnyBuf(state.postsBuf, bucket)
	}
	if !state.ids.IsEmpty() || state.containsMaterializeAt == 1 {
		bc := bucket.Cardinality()
		hit := state.materialize().AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	return countBucketPostsAnyNotBuf(state.postsBuf, bucket)
}

func postsAnyDirectBucketFilterMaxCard(termCount int) uint64 {
	if termCount <= 1 {
		return 64
	}
	limit := uint64(termCount) * 64
	if limit < 64 {
		return 64
	}
	if limit > 256 {
		return 256
	}
	return limit
}

func postsAnyEstimatedUnionCardinality(posts []posting.List) (uint64, int) {
	if posts == nil {
		return 0, 0
	}
	est := uint64(0)
	singletons := 0
	for i := 0; i < len(posts); i++ {
		card := posts[i].Cardinality()
		est = satAddUint64(est, card)
		if card == 1 {
			singletons++
		}
	}
	return est, singletons
}

func postsAnyDirectIntersectWork(posts []posting.List, dstCard uint64) uint64 {
	if posts == nil || dstCard == 0 {
		return 0
	}
	work := uint64(0)
	for i := 0; i < len(posts); i++ {
		card := posts[i].Cardinality()
		iterRows := card
		otherCard := dstCard
		if dstCard < card {
			iterRows = dstCard
			otherCard = card
		}
		work = satAddUint64(work, satMulUint64(iterRows, postingContainsLookupWork(otherCard)))
	}
	return work
}

func (state *postsAnyFilterState) applyWork(dstCard uint64) (uint64, uint64, uint64, uint64, bool) {
	if state.postsBuf == nil || dstCard == 0 {
		return 0, 0, 0, 0, false
	}
	termCount := len(state.postsBuf)
	if termCount <= 1 || termCount > 4 {
		return 0, 0, 0, 0, false
	}

	unionCard := uint64(0)
	singletons := 0
	if !state.ids.IsEmpty() {
		unionCard = state.ids.Cardinality()
	} else {
		unionCard, singletons = postsAnyEstimatedUnionCardinality(state.postsBuf)
	}
	if unionCard == 0 {
		return 0, 0, 0, 0, false
	}

	buildWork := uint64(0)
	if state.ids.IsEmpty() {
		buildWork = postingUnionLinearWork(termCount, unionCard)
		if shouldUseFastSinglesUnion(termCount, singletons, unionCard) {
			buildWork = postingUnionFastSinglesWork(termCount, singletons, unionCard)
		}
	}
	directWork := postsAnyDirectIntersectWork(state.postsBuf, dstCard)
	materializedCheckWork := satMulUint64(dstCard, postingContainsLookupWork(unionCard))
	return unionCard, buildWork, directWork, materializedCheckWork, true
}

func (state *postsAnyFilterState) applyOwnedLargeDirect(dst posting.List, card uint64) (posting.List, bool) {
	if state.postsBuf == nil || card == 0 || !dst.IsOwnedLarge() {
		return posting.List{}, false
	}

	it := dst.Iter()
	var inline [singleInlineCap]uint64
	inlineLen := 0
	var singles []uint64
	matched := uint64(0)

	for it.HasNext() {
		idx := it.Next()
		hit := false
		for i := 0; i < len(state.postsBuf); i++ {
			if state.postsBuf[i].Contains(idx) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		matched++
		if singles == nil {
			if inlineLen < len(inline) {
				inline[inlineLen] = idx
				inlineLen++
				continue
			}
			singles = pooled.GetUint64Slice(singleChunkCap)
			singles = append(singles, inline[:]...)
			inlineLen = 0
		}
		singles = append(singles, idx)
	}
	it.Release()

	if matched == 0 {
		if singles != nil {
			pooled.ReleaseUint64Slice(singles)
		}
		dst.Release()
		return posting.List{}, true
	}

	if matched == card {
		if singles != nil {
			pooled.ReleaseUint64Slice(singles)
		}
		return dst, true
	}

	if matched <= posting.MidCap {
		var out posting.List
		if singles != nil {
			out = posting.BuildFromSorted(singles)
			pooled.ReleaseUint64Slice(singles)
		} else {
			out = posting.BuildFromSorted(inline[:inlineLen])
		}
		dst.Release()
		return out, true
	}

	if singles != nil {
		out, ok := dst.TryResetOwnedLargeFromSorted(singles)
		pooled.ReleaseUint64Slice(singles)
		return out, ok
	}
	return dst.TryResetOwnedLargeFromSorted(inline[:inlineLen])
}

func (state *postsAnyFilterState) apply(dst posting.List) (posting.List, bool) {
	if dst.IsEmpty() {
		return dst, true
	}

	if state.neg {
		union := state.materialize()
		if union.IsEmpty() {
			return dst, true
		}
		return dst.BuildAndNot(union), true
	}

	postCount := len(state.postsBuf)

	if postCount <= 4 {

		card := dst.Cardinality()

		if card > 0 {
			unionCard := uint64(0)
			canDirect := false

			if estUnion, buildWork, directWork, materializedCheckWork, ok := state.applyWork(card); ok {
				unionCard = estUnion
				if state.ids.IsEmpty() {
					if directWork > materializedCheckWork {
						savings := directWork - materializedCheckWork
						if satAddUint64(state.applyObservedSavings, savings) >= buildWork {
							union := state.materialize()
							if union.IsEmpty() {
								dst.Release()
								return posting.List{}, true
							}
							return dst.BuildAnd(union), true
						}
						state.applyObservedSavings = satAddUint64(state.applyObservedSavings, savings)
					}
					canDirect = directWork < satAddUint64(buildWork, materializedCheckWork)

				} else {
					if directWork >= materializedCheckWork {
						return dst.BuildAnd(state.ids), true
					}
					canDirect = true
				}
			}

			if card <= postsAnyDirectBucketFilterMaxCard(postCount) {
				if next, ok := state.applyOwnedLargeDirect(dst, card); ok {
					return next, true
				}
				if next, ok := dst.TryBuildAndAnyBuf(state.postsBuf); ok {
					return next, true
				}

				builder := newPostingUnionBuilder(postingBatchSinglesEnabled(card))
				it := dst.Iter()
				matched := uint64(0)
				for it.HasNext() {
					idx := it.Next()
					for i := 0; i < len(state.postsBuf); i++ {
						if state.postsBuf[i].Contains(idx) {
							builder.addSingle(idx)
							matched++
							break
						}
					}
				}
				it.Release()

				if matched == 0 {
					builder.release()
					dst.Release()
					return posting.List{}, true
				}

				if matched == card {
					builder.release()
					return dst, true
				}

				out := builder.finish(false)
				dst.Release()
				return out, true
			}

			if canDirect {
				if next, ok := state.applyOwnedLargeDirect(dst, card); ok {
					return next, true
				}

				capHint := card
				if unionCard < capHint {
					capHint = unionCard
				}

				builder := newPostingUnionBuilder(postingBatchSinglesEnabled(capHint))
				for i := 0; i < len(state.postsBuf); i++ {
					builder = postingListAppendIntersecting(dst, state.postsBuf[i], builder)
				}
				dst.Release()
				out := builder.finish(false)
				builder.release()
				return out, true
			}

			hit := false
			for i := 0; i < len(state.postsBuf); i++ {
				if state.postsBuf[i].Intersects(dst) {
					hit = true
					break
				}
			}
			if !hit {
				dst.Release()
				return posting.List{}, true
			}
		}
	}

	union := state.materialize()
	if union.IsEmpty() {
		dst.Release()
		return posting.List{}, true
	}

	return dst.BuildAnd(union), true
}

type lazyMaterializedPredicateState struct {
	loader   lazyMaterializedPredicateLoader
	raw      qir.Expr
	cacheKey qcache.MaterializedPredKey
	ids      posting.List
	loaded   bool
}

func (state *lazyMaterializedPredicateState) materialize() posting.List {
	if !state.loaded {
		if state.loader != nil {
			state.ids = state.loader.evalLazyMaterializedPredicateWithKey(state.raw, state.cacheKey)
			state.loader = nil
		}
		state.loaded = true
	}
	return state.ids
}

var fieldIndexRangeIterPool = pooled.Pointers[fieldIndexRangeIter]{
	Cleanup: func(it *fieldIndexRangeIter) {
		if it.curIt != nil {
			it.curIt.Release()
			it.curIt = nil
		}
		it.cur = indexdata.FieldIndexCursor{}
		it.single = struct {
			set bool
			v   uint64
		}{}
	},
	Clear: true,
}

type fieldIndexRangePredicateState struct {
	ov                    indexdata.FieldIndexView
	br                    indexdata.FieldIndexRange
	probe                 fieldIndexRangeProbe
	reuse                 materializedPredReuse
	neg                   bool
	keepProbeHits         bool
	bucketCount           int
	linearContainsMax     int
	materializeAfter      int
	expectedContainsCalls int
	containsCalls         int
	containsMode          rangeContainsMode
	probePostingFilter    bool
	postingFilterCheap    bool
	probeMaterializeAt    int
	rangeMaterializeAt    int
	postingFilterCalls    int
	rangeMaterialized     bool
	probeMaterialized     bool
	linearPostsBuf        []posting.List
	rangeIDs              posting.List
	probeIDs              posting.List
}

var fieldIndexRangePredicateStatePool = pooled.Pointers[fieldIndexRangePredicateState]{
	Cleanup: func(state *fieldIndexRangePredicateState) {
		state.releaseLinearPosts()
		state.probeIDs.Release()
		state.rangeIDs.Release()
	},
	Clear: true,
}

func (state *fieldIndexRangePredicateState) setExpectedContainsCalls(expectedCalls int) {
	if expectedCalls <= 0 {
		expectedCalls = state.materializeAfter
	}
	if expectedCalls <= 0 {
		expectedCalls = 1
	}
	state.expectedContainsCalls = expectedCalls
	state.containsMode = chooseRangeContainsMode(
		state.probe.probeLen,
		state.probe.probeEst,
		state.expectedContainsCalls,
		0,
		state.materializeAfter,
	)
}

func (state *fieldIndexRangePredicateState) growContainsMode(expectedCalls int) {
	if expectedCalls <= state.expectedContainsCalls {
		return
	}
	state.setExpectedContainsCalls(expectedCalls)
}

func (state *fieldIndexRangePredicateState) releaseLinearPosts() {
	if state.linearPostsBuf == nil {
		return
	}
	posting.ReleaseSlice(state.linearPostsBuf)
	state.linearPostsBuf = nil
}

func (state *fieldIndexRangePredicateState) materializeRange() posting.List {
	if state.rangeMaterialized {
		return state.rangeIDs
	}
	if !state.probe.useComplement {
		if cached, ok := state.reuse.load(); ok {
			state.rangeIDs = cached
			state.rangeMaterialized = true
			state.releaseLinearPosts()
			return state.rangeIDs
		}
	}
	state.rangeIDs = state.ov.UnionRangePostings(state.br, indexdata.FieldIndexRange{})
	state.rangeMaterialized = true
	state.releaseLinearPosts()
	if !state.probe.useComplement {
		state.rangeIDs = state.reuse.share(state.rangeIDs)
	}
	return state.rangeIDs
}

func (state *fieldIndexRangePredicateState) ensureLinearPosts() []posting.List {
	if state.linearPostsBuf != nil {
		return state.linearPostsBuf
	}
	state.linearPostsBuf = posting.GetSlice(state.bucketCount)
	for i := 0; i < state.probe.spanCnt; i++ {
		cur := state.ov.NewCursor(state.probe.spans[i], false)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			state.linearPostsBuf = append(state.linearPostsBuf, ids)
		}
	}
	return state.linearPostsBuf
}

func (state *fieldIndexRangePredicateState) linearContains(idx uint64) bool {
	if state.expectedContainsCalls <= 1 || state.containsMode != rangeContainsLinear {
		return state.probe.linearContains(idx)
	}
	posts := state.ensureLinearPosts()
	for i := 0; i < len(posts); i++ {
		if posts[i].Contains(idx) {
			return true
		}
	}
	return false
}

func (state *fieldIndexRangePredicateState) materializeProbe() posting.List {
	if !state.probe.useComplement {
		return state.materializeRange()
	}
	if state.probeMaterialized {
		return state.probeIDs
	}
	if state.probe.spanCnt == 0 {
		state.probeMaterialized = true
		state.releaseLinearPosts()
		return posting.List{}
	}
	var second indexdata.FieldIndexRange
	if state.probe.spanCnt > 1 {
		second = state.probe.spans[1]
	}
	state.probeIDs = state.ov.UnionRangePostings(state.probe.spans[0], second)
	state.probeMaterialized = true
	state.releaseLinearPosts()
	return state.probeIDs
}

func (state *fieldIndexRangePredicateState) rawHit(idx uint64) bool {
	if state.rangeMaterialized {
		return state.rangeIDs.Contains(idx)
	}
	if state.probeMaterialized {
		return state.probeIDs.Contains(idx)
	}
	if state.probe.probeLen <= state.linearContainsMax {
		return state.linearContains(idx)
	}
	state.containsCalls++
	if state.containsMode == rangeContainsLinear && state.materializeAfter > 0 &&
		state.containsCalls >= state.materializeAfter {
		state.growContainsMode(state.containsCalls)
	}
	if state.containsMode == rangeContainsPosting &&
		state.materializeAfter > 0 &&
		state.containsCalls >= state.materializeAfter {
		return state.materializeProbe().Contains(idx)
	}
	return state.linearContains(idx)
}

func (state *fieldIndexRangePredicateState) matches(idx uint64) bool {
	if state.rangeMaterialized {
		hit := state.rangeIDs.Contains(idx)
		if state.neg {
			return !hit
		}
		return hit
	}
	hit := state.rawHit(idx)
	if state.keepProbeHits {
		return hit
	}
	return !hit
}

func (state *fieldIndexRangePredicateState) countBucket(bucket posting.List) (uint64, bool) {
	if state.rangeMaterialized {
		in := bucket.AndCardinality(state.rangeIDs)
		if !state.neg {
			return in, true
		}
		bc := bucket.Cardinality()
		if in >= bc {
			return 0, true
		}
		return bc - in, true
	}

	if state.probeMaterialized {
		in := bucket.AndCardinality(state.probeIDs)
		if state.keepProbeHits {
			return in, true
		}
		bc := bucket.Cardinality()
		if in >= bc {
			return 0, true
		}
		return bc - in, true
	}

	in, ok := state.probe.countBucket(bucket)
	if !ok {
		return 0, false
	}

	if !state.neg {
		return in, true
	}

	bc := bucket.Cardinality()
	if in >= bc {
		return 0, true
	}
	return bc - in, true
}

func (state *fieldIndexRangePredicateState) applyToPosting(dst posting.List) (posting.List, bool) {
	if dst.IsEmpty() {
		return dst, true
	}
	state.postingFilterCalls++

	if state.probePostingFilter {
		forceMaterialize := state.probe.probeLen > rangePostingFilterKeepProbeMaxBuckets &&
			state.postingFilterCalls >= state.probeMaterializeAt

		if !forceMaterialize {
			return applyRangeProbePostingFilter(dst, state.keepProbeHits, state.probe.probeLen, state.probe)
		}

		ids := state.materializeProbe()
		if ids.IsEmpty() {
			if state.keepProbeHits {
				dst.Release()
				dst = posting.List{}
			}
			return dst, true
		}

		if state.keepProbeHits {
			dst = dst.BuildAnd(ids)
		} else {
			dst = dst.BuildAndNot(ids)
		}

		return dst, true
	}

	if state.postingFilterCalls < state.rangeMaterializeAt {
		return posting.List{}, false
	}

	ids := state.materializeRange()
	if ids.IsEmpty() {
		if state.neg {
			return dst, true
		}
		dst.Release()
		return posting.List{}, true
	}

	if state.neg {
		dst = dst.BuildAndNot(ids)
	} else {
		dst = dst.BuildAnd(ids)
	}
	return dst, true
}

func (state *fieldIndexRangePredicateState) newIter() posting.Iterator {
	if state.neg {
		return nil
	}
	it := fieldIndexRangeIterPool.Get()
	it.cur = state.ov.NewCursor(state.br, false)
	return it
}

func tryShareMaterializedPredOnSnapshot(snap *snapshot.View, cacheKey qcache.MaterializedPredKey, ids posting.List) posting.List {
	if snap == nil || cacheKey.IsZero() {
		return ids
	}
	if snap.MaterializedPredCacheLimit() <= 0 {
		return ids
	}
	if ids.IsEmpty() {
		snap.StoreMaterializedPredKey(cacheKey, posting.List{})
		return ids
	}
	if next, ok := snap.LoadOrStoreMaterializedPredKey(cacheKey, ids); ok {
		return next
	}
	if next, ok := snap.TryLoadOrStoreMaterializedPredOversizedKey(cacheKey, ids); ok {
		return next
	}
	return ids
}

/**/

type materializedPredReuseMode uint8

const (
	materializedPredReuseNone materializedPredReuseMode = iota
	materializedPredReuseReadOnly
	materializedPredReuseShared
	materializedPredReuseSecondHitShared
)

type materializedPredReuse struct {
	snap     *snapshot.View
	cacheKey qcache.MaterializedPredKey
	mode     materializedPredReuseMode
}

func newMaterializedPredReadOnlyReuse(snap *snapshot.View, cacheKey qcache.MaterializedPredKey) materializedPredReuse {
	if snap == nil || cacheKey.IsZero() {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseReadOnly,
	}
}

func newMaterializedPredSharedReuse(snap *snapshot.View, cacheKey qcache.MaterializedPredKey) materializedPredReuse {
	if snap == nil || cacheKey.IsZero() {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseShared,
	}
}

func newMaterializedPredSecondHitSharedReuse(snap *snapshot.View, cacheKey qcache.MaterializedPredKey) materializedPredReuse {
	if snap == nil || cacheKey.IsZero() {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseSecondHitShared,
	}
}

func (reuse materializedPredReuse) load() (posting.List, bool) {
	if reuse.mode == materializedPredReuseNone || reuse.snap == nil || reuse.cacheKey.IsZero() {
		return posting.List{}, false
	}
	return reuse.snap.LoadMaterializedPredKey(reuse.cacheKey)
}

func (reuse materializedPredReuse) share(ids posting.List) posting.List {
	if (reuse.mode != materializedPredReuseShared && reuse.mode != materializedPredReuseSecondHitShared) ||
		reuse.snap == nil || reuse.cacheKey.IsZero() {
		return ids
	}
	if reuse.mode == materializedPredReuseSecondHitShared &&
		!reuse.snap.ShouldPromoteRuntimeMaterializedPredKey(reuse.cacheKey) {
		return ids
	}
	return tryShareMaterializedPredOnSnapshot(reuse.snap, reuse.cacheKey, ids)
}

func (reuse materializedPredReuse) canSecondHitShareModeratelyOversizedEstimate(est uint64) bool {
	if reuse.mode != materializedPredReuseSecondHitShared || reuse.snap == nil || reuse.cacheKey.IsZero() || est == 0 {
		return false
	}
	return reuse.snap.CanShareModeratelyOversizedMaterializedPred(est)
}

func (qv *View) materializedPredKeyForExactScalarRange(field string, bounds indexdata.Bounds) qcache.MaterializedPredKey {
	if qv.snap.MaterializedPredCacheLimit() <= 0 {
		return qcache.MaterializedPredKey{}
	}
	return qcache.MaterializedPredKeyForExactScalarRange(field, bounds)
}

func (qv *View) materializedPredComplementKeyForExactScalarRange(field string, bounds indexdata.Bounds) qcache.MaterializedPredKey {
	if qv.snap.MaterializedPredCacheLimit() <= 0 {
		return qcache.MaterializedPredKey{}
	}
	return qcache.MaterializedPredComplementKeyForExactScalarRange(field, bounds)
}

/**/

const (
	singleChunkCap       = 32768
	singleAdaptiveMaxLen = 200_000
	singleInlineCap      = 16
)

type postingUnionBuilder struct {
	ids           posting.List
	singles       []uint64
	inlineSingles [singleInlineCap]uint64
	inlineLen     int
	batchSingles  bool
}

func postingBatchSinglesEnabled(capHint uint64) bool {
	return capHint > posting.SmallCap && capHint <= singleAdaptiveMaxLen
}

func newPostingUnionBuilder(batchSingles bool) postingUnionBuilder {
	return postingUnionBuilder{batchSingles: batchSingles}
}

func (b *postingUnionBuilder) flushSingles() {
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
	singles := b.inlineSingles[:b.inlineLen]
	b.ids = b.ids.BuildAddedMany(singles)
	b.inlineLen = 0
}

func (b *postingUnionBuilder) addSingle(idx uint64) {
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
		b.singles = pooled.GetUint64Slice(singleChunkCap)
		b.singles = append(b.singles, b.inlineSingles[:]...)
		b.inlineLen = 0
	}
	b.singles = append(b.singles, idx)
	if len(b.singles) == cap(b.singles) {
		b.flushSingles()
	}
}

func (b *postingUnionBuilder) addPosting(ids posting.List) {
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

func (b *postingUnionBuilder) finish(optimize bool) posting.List {
	b.flushSingles()

	out := b.ids
	if out.IsBorrowed() {
		out = out.Clone()
	}

	b.ids = posting.List{}
	b.inlineLen = 0

	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}

	if optimize {
		return out.BuildOptimized()
	}
	return out
}

func (b *postingUnionBuilder) release() {
	b.ids.Release()
	b.ids = posting.List{}
	b.inlineLen = 0
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}
}

type postingSetBuilder struct {
	ids           posting.List
	seen          u64set
	singles       []uint64
	inlineSingles [singleInlineCap]uint64
	inlineLen     int
	count         uint64
	batchSingles  bool
}

type postingLazySetBuilder struct {
	builder      postingSetBuilder
	capHint      uint64
	batchSingles bool
	active       bool
}

func newPostingLazySetBuilder(capHint uint64) postingLazySetBuilder {
	return postingLazySetBuilder{
		capHint:      capHint,
		batchSingles: postingBatchSinglesEnabled(capHint),
	}
}

func (b *postingLazySetBuilder) addChecked(idx uint64) bool {
	if !b.active {
		b.builder = newPostingSetBuilder(b.capHint, b.batchSingles)
		b.active = true
	}
	return b.builder.addChecked(idx)
}

func (b *postingLazySetBuilder) finish(optimize bool) posting.List {
	if !b.active {
		return posting.List{}
	}
	out := b.builder.finish(optimize)
	b.active = false
	return out
}

func (b *postingLazySetBuilder) release() {
	if !b.active {
		return
	}
	b.builder.release()
	b.active = false
}

func newPostingSetBuilder(capHint uint64, batchSingles bool) postingSetBuilder {
	b := postingSetBuilder{
		seen:         getU64Set(normalizePostingSetCapHint(capHint)),
		batchSingles: batchSingles,
	}
	return b
}

func normalizePostingSetCapHint(capHint uint64) int {
	if capHint < 64 {
		capHint = 64
	}
	return clampUint64ToInt(capHint)
}

func (b *postingSetBuilder) flushSingles() {
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

	singles := b.inlineSingles[:b.inlineLen]
	b.ids = b.ids.BuildAddedMany(singles)
	b.inlineLen = 0
}

func (b *postingSetBuilder) addChecked(idx uint64) bool {
	if !b.seen.Add(idx) {
		return false
	}

	b.count++

	if !b.batchSingles {
		b.ids = b.ids.BuildAdded(idx)
		return true
	}

	if b.singles == nil {
		if b.inlineLen < len(b.inlineSingles) {
			b.inlineSingles[b.inlineLen] = idx
			b.inlineLen++
			return true
		}
		b.singles = pooled.GetUint64Slice(singleChunkCap)
		b.singles = append(b.singles, b.inlineSingles[:]...)
		b.inlineLen = 0
	}

	b.singles = append(b.singles, idx)

	if len(b.singles) == cap(b.singles) {
		b.flushSingles()
	}

	return true
}

func (b *postingSetBuilder) cardinality() uint64 {
	return b.count
}

func (b *postingSetBuilder) finish(optimize bool) posting.List {
	b.flushSingles()

	out := b.ids
	b.ids = posting.List{}

	releaseU64Set(&b.seen)

	b.inlineLen = 0
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}

	b.count = 0

	if optimize {
		return out.BuildOptimized()
	}
	return out
}

func (b *postingSetBuilder) release() {
	b.ids.Release()
	b.ids = posting.List{}

	releaseU64Set(&b.seen)

	b.inlineLen = 0
	if b.singles != nil {
		pooled.ReleaseUint64Slice(b.singles)
		b.singles = nil
	}
	b.count = 0
}
