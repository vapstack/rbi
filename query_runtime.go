package rbi

import (
	"math/bits"
	"slices"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type postsAnyFilterState struct {
	postsBuf              *pooled.SliceBuf[posting.List]
	ids                   posting.List
	containsCalls         int
	containsMaterializeAt int
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
	evalLazyMaterializedPredicateWithKey(raw qx.Expr, cacheKey materializedPredKey) posting.List
}

func (state *postsAnyFilterState) materialize() posting.List {
	if state == nil {
		return posting.List{}
	}
	if !state.ids.IsEmpty() {
		return state.ids
	}
	if isSingletonHeavyPostingBuf(state.postsBuf) {
		state.ids = materializePostingBufFast(state.postsBuf)
	} else {
		state.ids = materializePostingUnionBufOwned(state.postsBuf)
	}
	return state.ids
}

func postsAnyContainsMaterializeAfterBuf(posts *pooled.SliceBuf[posting.List]) int {
	termCount := postingBufLen(posts)
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
		sampleTotal += posts.Get(i).Cardinality()
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

func (state *postsAnyFilterState) matches(idx uint64) bool {
	if state == nil {
		return false
	}
	if !state.ids.IsEmpty() {
		return state.ids.Contains(idx)
	}
	if state.containsMaterializeAt > 0 {
		state.containsCalls++
		if state.containsCalls >= state.containsMaterializeAt {
			return state.materialize().Contains(idx)
		}
	}
	switch postingBufLen(state.postsBuf) {
	case 0:
		return false
	case 1:
		return state.postsBuf.Get(0).Contains(idx)
	case 2:
		return state.postsBuf.Get(0).Contains(idx) || state.postsBuf.Get(1).Contains(idx)
	case 3:
		return state.postsBuf.Get(0).Contains(idx) ||
			state.postsBuf.Get(1).Contains(idx) ||
			state.postsBuf.Get(2).Contains(idx)
	case 4:
		return state.postsBuf.Get(0).Contains(idx) ||
			state.postsBuf.Get(1).Contains(idx) ||
			state.postsBuf.Get(2).Contains(idx) ||
			state.postsBuf.Get(3).Contains(idx)
	}
	for i := 0; i < state.postsBuf.Len(); i++ {
		if state.postsBuf.Get(i).Contains(idx) {
			return true
		}
	}
	return false
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

func postsAnyEstimatedUnionCardinality(posts *pooled.SliceBuf[posting.List]) (uint64, int) {
	if posts == nil {
		return 0, 0
	}
	est := uint64(0)
	singletons := 0
	for i := 0; i < posts.Len(); i++ {
		card := posts.Get(i).Cardinality()
		est = satAddUint64(est, card)
		if card == 1 {
			singletons++
		}
	}
	return est, singletons
}

func postsAnyDirectIntersectWork(posts *pooled.SliceBuf[posting.List], dstCard uint64) uint64 {
	if posts == nil || dstCard == 0 {
		return 0
	}
	work := uint64(0)
	for i := 0; i < posts.Len(); i++ {
		card := posts.Get(i).Cardinality()
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

func (state *postsAnyFilterState) shouldUseDirectIntersect(dstCard uint64) (uint64, bool) {
	if state == nil || state.postsBuf == nil || dstCard == 0 {
		return 0, false
	}
	termCount := state.postsBuf.Len()
	if termCount <= 1 || termCount > 4 {
		return 0, false
	}

	unionCard := uint64(0)
	singletons := 0
	if !state.ids.IsEmpty() {
		unionCard = state.ids.Cardinality()
	} else {
		unionCard, singletons = postsAnyEstimatedUnionCardinality(state.postsBuf)
	}
	if unionCard == 0 {
		return 0, false
	}

	directWork := postsAnyDirectIntersectWork(state.postsBuf, dstCard)
	materializedWork := satMulUint64(dstCard, postingContainsLookupWork(unionCard))
	if state.ids.IsEmpty() {
		buildWork := postingUnionLinearWork(termCount, unionCard)
		if shouldUseFastSinglesUnion(termCount, singletons, unionCard) {
			buildWork = postingUnionFastSinglesWork(termCount, singletons, unionCard)
		}
		materializedWork = satAddUint64(materializedWork, buildWork)
	}
	if directWork >= materializedWork {
		return 0, false
	}
	return unionCard, true
}

func (state *postsAnyFilterState) apply(dst posting.List) (posting.List, bool) {
	if dst.IsEmpty() {
		return dst, true
	}
	postCount := postingBufLen(state.postsBuf)
	if postCount <= 4 {
		card := dst.Cardinality()
		if card > 0 && card <= postsAnyDirectBucketFilterMaxCard(postCount) {
			if idx, ok := dst.TrySingle(); ok {
				for i := 0; i < state.postsBuf.Len(); i++ {
					if state.postsBuf.Get(i).Contains(idx) {
						return dst, true
					}
				}
				dst.Release()
				return posting.List{}, true
			}

			builder := newPostingUnionBuilder(postingBatchSinglesEnabled(card))
			it := dst.Iter()
			matched := uint64(0)
			for it.HasNext() {
				idx := it.Next()
				for i := 0; i < state.postsBuf.Len(); i++ {
					if state.postsBuf.Get(i).Contains(idx) {
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

		if unionCard, ok := state.shouldUseDirectIntersect(card); ok {
			capHint := card
			if unionCard < capHint {
				capHint = unionCard
			}
			builder := newPostingUnionBuilder(postingBatchSinglesEnabled(capHint))
			for i := 0; i < state.postsBuf.Len(); i++ {
				builder = postingListAppendIntersecting(dst, state.postsBuf.Get(i), builder)
			}
			dst.Release()
			out := builder.finish(false)
			builder.release()
			return out, true
		}

		hit := false
		for i := 0; i < state.postsBuf.Len(); i++ {
			if state.postsBuf.Get(i).Intersects(dst) {
				hit = true
				break
			}
		}
		if !hit {
			dst.Release()
			return posting.List{}, true
		}
	}
	union := state.materialize()
	if union.IsEmpty() {
		dst.Release()
		return posting.List{}, true
	}
	return dst.BuildAnd(union), true
}

func postingBufLen(buf *pooled.SliceBuf[posting.List]) int {
	if buf == nil {
		return 0
	}
	return buf.Len()
}

func (state *postsAnyFilterState) borrowMaterialized() posting.List {
	union := state.materialize()
	if union.IsEmpty() {
		return posting.List{}
	}
	return union.Borrow()
}

type lazyMaterializedPredicateState struct {
	loader   lazyMaterializedPredicateLoader
	raw      qx.Expr
	cacheKey materializedPredKey
	ids      posting.List
	loaded   bool
}

func (state *lazyMaterializedPredicateState) materialize() posting.List {
	if state == nil {
		return posting.List{}
	}
	if !state.loaded {
		if state.loader != nil {
			state.ids = state.loader.evalLazyMaterializedPredicateWithKey(state.raw, state.cacheKey)
			state.loader = nil
		}
		state.loaded = true
	}
	return state.ids
}

var overlayRangeIterPool = pooled.Pointers[overlayRangeIter]{
	Cleanup: func(it *overlayRangeIter) {
		if it.curIt != nil {
			it.curIt.Release()
			it.curIt = nil
		}
		it.cur = overlayKeyCursor{}
		it.single = struct {
			set bool
			v   uint64
		}{}
	},
	Clear: true,
}

type overlayRangePredicateState struct {
	ov                    fieldOverlay
	br                    overlayRange
	probe                 overlayRangeProbe
	reuse                 materializedPredReuse
	neg                   bool
	keepProbeHits         bool
	bucketCount           int
	linearContainsMax     int
	materializeAfter      int
	expectedContainsCalls int
	containsCalls         int
	containsMode          baseRangeContainsMode
	probePostingFilter    bool
	postingFilterCheap    bool
	probeMaterializeAt    int
	rangeMaterializeAt    int
	postingFilterCalls    int
	rangeMaterialized     bool
	probeMaterialized     bool
	linearPostsBuf        *pooled.SliceBuf[posting.List]
	rangeIDs              posting.List
	probeIDs              posting.List
}

var overlayRangePredicateStatePool = pooled.Pointers[overlayRangePredicateState]{
	Cleanup: func(state *overlayRangePredicateState) {
		state.releaseLinearPosts()
		state.probeIDs.Release()
		state.rangeIDs.Release()
	},
	Clear: true,
}

func (state *overlayRangePredicateState) setExpectedContainsCalls(expectedCalls int) {
	if state == nil {
		return
	}
	if expectedCalls <= 0 {
		expectedCalls = state.materializeAfter
	}
	if expectedCalls <= 0 {
		expectedCalls = 1
	}
	state.expectedContainsCalls = expectedCalls
	state.containsMode = chooseBaseRangeContainsMode(
		state.probe.probeLen,
		state.probe.probeEst,
		state.expectedContainsCalls,
		0,
		state.materializeAfter,
	)
}

func (state *overlayRangePredicateState) growContainsMode(expectedCalls int) {
	if state == nil || expectedCalls <= state.expectedContainsCalls {
		return
	}
	state.setExpectedContainsCalls(expectedCalls)
}

func (state *overlayRangePredicateState) releaseLinearPosts() {
	if state == nil || state.linearPostsBuf == nil {
		return
	}
	postingSlicePool.Put(state.linearPostsBuf)
	state.linearPostsBuf = nil
}

func (state *overlayRangePredicateState) materializeRange() posting.List {
	if state == nil {
		return posting.List{}
	}
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
	state.rangeIDs = overlayUnionRange(state.ov, state.br)
	state.rangeMaterialized = true
	state.releaseLinearPosts()
	if !state.probe.useComplement {
		state.rangeIDs = state.reuse.share(state.rangeIDs)
	}
	return state.rangeIDs
}

func (state *overlayRangePredicateState) ensureLinearPosts() *pooled.SliceBuf[posting.List] {
	if state == nil {
		return nil
	}
	if state.linearPostsBuf != nil {
		return state.linearPostsBuf
	}
	state.linearPostsBuf = postingSlicePool.Get()
	state.linearPostsBuf.Grow(state.bucketCount)
	for i := 0; i < state.probe.spanCnt; i++ {
		cur := state.ov.newCursor(state.probe.spans[i], false)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			state.linearPostsBuf.Append(ids)
		}
	}
	return state.linearPostsBuf
}

func (state *overlayRangePredicateState) linearContains(idx uint64) bool {
	if state == nil {
		return false
	}
	if state.expectedContainsCalls <= 1 || state.containsMode != baseRangeContainsLinear {
		return state.probe.linearContains(idx)
	}
	posts := state.ensureLinearPosts()
	for i := 0; i < posts.Len(); i++ {
		if posts.Get(i).Contains(idx) {
			return true
		}
	}
	return false
}

func (state *overlayRangePredicateState) materializeProbe() posting.List {
	if state == nil {
		return posting.List{}
	}
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
	var second overlayRange
	if state.probe.spanCnt > 1 {
		second = state.probe.spans[1]
	}
	state.probeIDs = overlayUnionRanges(state.ov, state.probe.spans[0], second)
	state.probeMaterialized = true
	state.releaseLinearPosts()
	return state.probeIDs
}

func (state *overlayRangePredicateState) rawHit(idx uint64) bool {
	if state == nil {
		return false
	}
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
	if state.containsMode == baseRangeContainsLinear && state.materializeAfter > 0 &&
		state.containsCalls >= state.materializeAfter {
		state.growContainsMode(state.containsCalls)
	}
	if state.containsMode == baseRangeContainsPosting &&
		state.materializeAfter > 0 &&
		state.containsCalls >= state.materializeAfter {
		return state.materializeProbe().Contains(idx)
	}
	return state.linearContains(idx)
}

func (state *overlayRangePredicateState) matches(idx uint64) bool {
	if state == nil {
		return false
	}
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

func (state *overlayRangePredicateState) countBucket(bucket posting.List) (uint64, bool) {
	if state == nil {
		return 0, false
	}
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

func (state *overlayRangePredicateState) applyToPosting(dst posting.List) (posting.List, bool) {
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

func (state *overlayRangePredicateState) newIter() posting.Iterator {
	if state == nil || state.neg {
		return nil
	}
	it := overlayRangeIterPool.Get()
	it.cur = state.ov.newCursor(state.br, false)
	return it
}

type baseRangePredicateState struct {
	probe                      baseRangeProbe
	reuse                      materializedPredReuse
	keepProbeHits              bool
	neg                        bool
	linearContainsMax          int
	hashSetAfter               int
	materializeAfter           int
	expectedContainsCalls      int
	postingFilterMaterializeAt int
	containsMode               baseRangeContainsMode

	containsCalls      int
	postingFilterCalls int
	hasSet             bool
	set                u64set
	linearPostsBuf     *pooled.SliceBuf[posting.List]
	ids                posting.List
}

var baseRangePredicateStatePool = pooled.Pointers[baseRangePredicateState]{
	Cleanup: func(state *baseRangePredicateState) {
		if state.hasSet {
			releaseU64Set(&state.set)
			state.hasSet = false
		}
		state.releaseLinearPosts()
		state.ids.Release()
	},
	Clear: true,
}

func (state *baseRangePredicateState) releaseLinearPosts() {
	if state == nil || state.linearPostsBuf == nil {
		return
	}
	postingSlicePool.Put(state.linearPostsBuf)
	state.linearPostsBuf = nil
}

func tryShareMaterializedPredOnSnapshot(snap *indexSnapshot, cacheKey materializedPredKey, ids posting.List) posting.List {
	if snap == nil || cacheKey.isZero() {
		return ids
	}
	if snap.matPredCacheMaxEntries <= 0 {
		return ids
	}
	if ids.IsEmpty() {
		snap.storeMaterializedPredKey(cacheKey, posting.List{})
		return ids
	}
	if next, ok := snap.loadOrStoreMaterializedPredKey(cacheKey, ids); ok {
		return next
	}
	if next, ok := snap.tryLoadOrStoreMaterializedPredOversizedKey(cacheKey, ids); ok {
		return next
	}
	return ids
}

func (state *baseRangePredicateState) materialize() posting.List {
	if state == nil {
		return posting.List{}
	}
	if !state.ids.IsEmpty() {
		return state.ids
	}
	if cached, ok := state.reuse.load(); ok {
		state.ids = cached
		return state.ids
	}
	if state.probe.singletonHeavy() {
		state.ids = materializeBaseRangeProbeFast(state.probe)
	} else {
		state.ids = materializePostingUnionBaseRange(state.probe)
	}
	state.ids = state.reuse.share(state.ids)
	return state.ids
}

func (state *baseRangePredicateState) setExpectedContainsCalls(expectedCalls int) {
	if state == nil {
		return
	}
	if expectedCalls <= 0 {
		expectedCalls = state.materializeAfter
	}
	if expectedCalls <= 0 {
		expectedCalls = 1
	}
	state.expectedContainsCalls = expectedCalls
	state.containsMode = chooseBaseRangeContainsMode(
		state.probe.probeLen,
		state.probe.probeEst,
		state.expectedContainsCalls,
		state.hashSetAfter,
		state.materializeAfter,
	)
}

func (state *baseRangePredicateState) growContainsMode(expectedCalls int) {
	if state == nil || expectedCalls <= state.expectedContainsCalls {
		return
	}
	state.setExpectedContainsCalls(expectedCalls)
}

func materializePostingUnionBaseRange(probe baseRangeProbe) posting.List {
	if probe.probeLen == 0 {
		return posting.List{}
	}
	postsBuf := postingSlicePool.Get()
	postsBuf.Grow(probe.probeLen)
	probe.appendPostings(postsBuf)
	out := materializePostingUnionBufOwned(postsBuf)
	postingSlicePool.Put(postsBuf)
	return out
}

func (state *baseRangePredicateState) ensureLinearPosts() *pooled.SliceBuf[posting.List] {
	if state == nil {
		return nil
	}
	if state.linearPostsBuf != nil {
		return state.linearPostsBuf
	}
	state.linearPostsBuf = postingSlicePool.Get()
	state.linearPostsBuf.Grow(state.probe.probeLen)
	state.probe.appendPostings(state.linearPostsBuf)
	return state.linearPostsBuf
}

func (state *baseRangePredicateState) linearContains(idx uint64) bool {
	if state == nil {
		return false
	}
	if state.expectedContainsCalls <= 1 || state.containsMode != baseRangeContainsLinear {
		return state.probe.linearContains(idx)
	}
	posts := state.ensureLinearPosts()
	for i := 0; i < posts.Len(); i++ {
		if posts.Get(i).Contains(idx) {
			return true
		}
	}
	return false
}

func (state *baseRangePredicateState) rawHit(idx uint64) bool {
	if state == nil {
		return false
	}
	if state.probe.probeLen <= state.linearContainsMax {
		return state.linearContains(idx)
	}
	if state.hasSet {
		return state.set.Has(idx)
	}
	if !state.ids.IsEmpty() {
		return state.ids.Contains(idx)
	}

	state.containsCalls++
	if state.containsMode == baseRangeContainsLinear {
		nextBuildAfter := state.materializeAfter
		if state.hashSetAfter > 0 && (nextBuildAfter == 0 || state.hashSetAfter < nextBuildAfter) {
			nextBuildAfter = state.hashSetAfter
		}
		if nextBuildAfter > 0 && state.containsCalls >= nextBuildAfter {
			state.growContainsMode(state.containsCalls)
		}
	}
	switch state.containsMode {
	case baseRangeContainsHashSet:
		if state.containsCalls >= state.hashSetAfter {
			capHint := int(state.probe.probeEst)
			if capHint < 64 {
				capHint = 64
			}
			state.set = newU64Set(capHint)
			state.hasSet = true
			state.probe.addToSet(&state.set)
			return state.set.Has(idx)
		}
	case baseRangeContainsPosting:
		if state.containsCalls >= state.materializeAfter && state.probe.probeLen > state.linearContainsMax {
			return state.materialize().Contains(idx)
		}
	}

	return state.linearContains(idx)
}

func (state *baseRangePredicateState) matches(idx uint64) bool {
	hit := state.rawHit(idx)
	if state.keepProbeHits {
		return hit
	}
	return !hit
}

func (state *baseRangePredicateState) countBucket(bucket posting.List) (uint64, bool) {
	if state == nil {
		return 0, false
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

func (state *baseRangePredicateState) applyToPosting(dst posting.List) (posting.List, bool) {
	if dst.IsEmpty() {
		return dst, true
	}

	state.postingFilterCalls++
	forceMaterialize := state.postingFilterCalls >= state.postingFilterMaterializeAt
	if !forceMaterialize {
		return applyRangeProbePostingFilter(dst, state.keepProbeHits, state.probe.probeLen, state.probe)
	}

	ids := state.materialize()
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

func (state *baseRangePredicateState) newIter() posting.Iterator {
	if state == nil || state.neg {
		return nil
	}
	return newRangeIter(state.probe.s, state.probe.start, state.probe.end)
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
	snap     *indexSnapshot
	cacheKey materializedPredKey
	mode     materializedPredReuseMode
}

func newMaterializedPredReadOnlyReuse(snap *indexSnapshot, cacheKey materializedPredKey) materializedPredReuse {
	if snap == nil || cacheKey.isZero() {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseReadOnly,
	}
}

func newMaterializedPredSharedReuse(snap *indexSnapshot, cacheKey materializedPredKey) materializedPredReuse {
	if snap == nil || cacheKey.isZero() {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseShared,
	}
}

func newMaterializedPredSecondHitSharedReuse(snap *indexSnapshot, cacheKey materializedPredKey) materializedPredReuse {
	if snap == nil || cacheKey.isZero() {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseSecondHitShared,
	}
}

func (reuse materializedPredReuse) load() (posting.List, bool) {
	if reuse.mode == materializedPredReuseNone || reuse.snap == nil || reuse.cacheKey.isZero() {
		return posting.List{}, false
	}
	return reuse.snap.loadMaterializedPredKey(reuse.cacheKey)
}

func (reuse materializedPredReuse) share(ids posting.List) posting.List {
	if (reuse.mode != materializedPredReuseShared && reuse.mode != materializedPredReuseSecondHitShared) ||
		reuse.snap == nil || reuse.cacheKey.isZero() {
		return ids
	}
	if reuse.mode == materializedPredReuseSecondHitShared &&
		!reuse.snap.shouldPromoteRuntimeMaterializedPredKey(reuse.cacheKey) {
		return ids
	}
	return tryShareMaterializedPredOnSnapshot(reuse.snap, reuse.cacheKey, ids)
}

func (reuse materializedPredReuse) canSecondHitShareModeratelyOversizedEstimate(est uint64) bool {
	if reuse.mode != materializedPredReuseSecondHitShared || reuse.snap == nil || reuse.cacheKey.isZero() || est == 0 {
		return false
	}
	if reuse.snap.matPredCacheMaxEntries <= 0 {
		return false
	}
	if reuse.snap.matPredCacheMaxCard == 0 {
		return false
	}
	if est <= reuse.snap.matPredCacheMaxCard {
		return true
	}
	oversizedLimit := materializedPredCacheOversizedLimit(reuse.snap.matPredCacheMaxEntries)
	if oversizedLimit <= 0 {
		return false
	}
	limit := reuse.snap.matPredCacheMaxCard
	mul := uint64(oversizedLimit)
	if ^uint64(0)/limit < mul {
		return false
	}
	return est <= limit*mul
}

func (qv *queryView[K, V]) materializedPredKeyForExactScalarRange(field string, bounds rangeBounds) materializedPredKey {
	if qv.snap.matPredCacheMaxEntries <= 0 {
		return materializedPredKey{}
	}
	return materializedPredKeyForExactScalarRange(field, bounds)
}

func (qv *queryView[K, V]) materializedPredCacheKeyForExactScalarRange(field string, bounds rangeBounds) string {
	return qv.materializedPredKeyForExactScalarRange(field, bounds).String()
}

/**/

const (
	singleChunkCap       = 32768
	singleAdaptiveMaxLen = 200_000
	singleInlineCap      = 16
)

var singleIDsPool = pooled.Pointers[singleIDsBuffer]{
	New: func() *singleIDsBuffer {
		return &singleIDsBuffer{
			values: make([]uint64, 0, singleChunkCap),
		}
	},
	Cleanup: func(buf *singleIDsBuffer) {
		buf.values = buf.values[:0]
	},
}

type singleIDsBuffer struct {
	values []uint64
}

func getSingleIDsBuf() *singleIDsBuffer {
	return singleIDsPool.Get()
}

func releaseSingleIDs(buf *singleIDsBuffer) {
	if buf == nil || cap(buf.values) != singleChunkCap {
		return
	}
	singleIDsPool.Put(buf)
}

type postingUnionBuilder struct {
	ids           posting.List
	singles       *singleIDsBuffer
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
		if len(b.singles.values) == 0 {
			return
		}
		slices.Sort(b.singles.values)
		b.ids = b.ids.BuildAddedMany(b.singles.values)
		b.singles.values = b.singles.values[:0]
		return
	}
	if b.inlineLen == 0 {
		return
	}
	singles := b.inlineSingles[:b.inlineLen]
	slices.Sort(singles)
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
		b.singles = getSingleIDsBuf()
		b.singles.values = append(b.singles.values, b.inlineSingles[:]...)
		b.inlineLen = 0
	}
	b.singles.values = append(b.singles.values, idx)
	if len(b.singles.values) == cap(b.singles.values) {
		b.flushSingles()
	}
}

func (b *postingUnionBuilder) addPosting(ids posting.List) {
	if ids.IsEmpty() {
		return
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
	b.ids = posting.List{}
	b.inlineLen = 0
	if b.singles != nil {
		releaseSingleIDs(b.singles)
		b.singles = nil
	}
	if optimize {
		if out.IsBorrowed() {
			return out
		}
		return out.BuildOptimized()
	}
	return out
}

func (b *postingUnionBuilder) release() {
	b.ids.Release()
	b.ids = posting.List{}
	b.inlineLen = 0
	if b.singles != nil {
		releaseSingleIDs(b.singles)
		b.singles = nil
	}
}

type postingSetBuilder struct {
	ids           posting.List
	seen          u64set
	singles       *singleIDsBuffer
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
		seen:         newU64Set(normalizePostingSetCapHint(capHint)),
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
		if len(b.singles.values) == 0 {
			return
		}
		slices.Sort(b.singles.values)
		b.ids = b.ids.BuildAddedMany(b.singles.values)
		b.singles.values = b.singles.values[:0]
		return
	}
	if b.inlineLen == 0 {
		return
	}
	singles := b.inlineSingles[:b.inlineLen]
	slices.Sort(singles)
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
		b.singles = getSingleIDsBuf()
		b.singles.values = append(b.singles.values, b.inlineSingles[:]...)
		b.inlineLen = 0
	}
	b.singles.values = append(b.singles.values, idx)
	if len(b.singles.values) == cap(b.singles.values) {
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
		releaseSingleIDs(b.singles)
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
		releaseSingleIDs(b.singles)
		b.singles = nil
	}
	b.count = 0
}
