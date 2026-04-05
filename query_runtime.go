package rbi

import (
	"math/bits"
	"slices"
	"sync"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

type postsAnyFilterState struct {
	posts                 []posting.List
	ids                   posting.List
	containsCalls         int
	containsMaterializeAt int
}

var postsAnyFilterStatePool sync.Pool
var lazyMaterializedPredicateStatePool sync.Pool

type lazyMaterializedPredicateLoader interface {
	evalLazyMaterializedPredicate(raw qx.Expr, cacheKey string) posting.List
}

func acquirePostsAnyFilterState(posts []posting.List) *postsAnyFilterState {
	if v := postsAnyFilterStatePool.Get(); v != nil {
		state := v.(*postsAnyFilterState)
		state.posts = posts
		state.containsMaterializeAt = postsAnyContainsMaterializeAfter(posts)
		return state
	}
	return &postsAnyFilterState{
		posts:                 posts,
		containsMaterializeAt: postsAnyContainsMaterializeAfter(posts),
	}
}

func releasePostsAnyFilterState(state *postsAnyFilterState) {
	state.ids.Release()
	*state = postsAnyFilterState{}
	postsAnyFilterStatePool.Put(state)
}

func (state *postsAnyFilterState) materialize() posting.List {
	if state == nil {
		return posting.List{}
	}
	if !state.ids.IsEmpty() {
		return state.ids
	}
	if isSingletonHeavyProbe(state.posts) {
		state.ids = materializeProbeFast(state.posts)
	} else {
		state.ids = materializePostingUnionOwned(state.posts)
	}
	return state.ids
}

func postsAnyContainsMaterializeAfter(posts []posting.List) int {
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
	switch len(state.posts) {
	case 0:
		return false
	case 1:
		return state.posts[0].Contains(idx)
	case 2:
		return state.posts[0].Contains(idx) || state.posts[1].Contains(idx)
	case 3:
		return state.posts[0].Contains(idx) ||
			state.posts[1].Contains(idx) ||
			state.posts[2].Contains(idx)
	case 4:
		return state.posts[0].Contains(idx) ||
			state.posts[1].Contains(idx) ||
			state.posts[2].Contains(idx) ||
			state.posts[3].Contains(idx)
	}
	for _, ids := range state.posts {
		if ids.Contains(idx) {
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

func (state *postsAnyFilterState) apply(dst posting.List) (posting.List, bool) {
	if dst.IsEmpty() {
		return dst, true
	}
	if len(state.posts) <= 4 {
		card := dst.Cardinality()
		if card > 0 && card <= postsAnyDirectBucketFilterMaxCard(len(state.posts)) {
			if idx, ok := dst.TrySingle(); ok {
				for _, ids := range state.posts {
					if ids.Contains(idx) {
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
				for _, ids := range state.posts {
					if ids.Contains(idx) {
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

		hit := false
		for _, ids := range state.posts {
			if ids.Intersects(dst) {
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
	cacheKey string
	ids      posting.List
	loaded   bool
}

func acquireLazyMaterializedPredicateState(loader lazyMaterializedPredicateLoader, raw qx.Expr, cacheKey string) *lazyMaterializedPredicateState {
	if v := lazyMaterializedPredicateStatePool.Get(); v != nil {
		state := v.(*lazyMaterializedPredicateState)
		state.loader = loader
		state.raw = raw
		state.cacheKey = cacheKey
		return state
	}
	return &lazyMaterializedPredicateState{
		loader:   loader,
		raw:      raw,
		cacheKey: cacheKey,
	}
}

func releaseLazyMaterializedPredicateState(state *lazyMaterializedPredicateState) {
	state.ids.Release()
	*state = lazyMaterializedPredicateState{}
	lazyMaterializedPredicateStatePool.Put(state)
}

func (state *lazyMaterializedPredicateState) materialize() posting.List {
	if state == nil {
		return posting.List{}
	}
	if !state.loaded {
		if state.loader != nil {
			state.ids = state.loader.evalLazyMaterializedPredicate(state.raw, state.cacheKey)
			state.loader = nil
		}
		state.loaded = true
	}
	return state.ids
}

var overlayRangeIterPool sync.Pool

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
	linearPostsBuf        *postingSliceBuf
	linearPosts           []posting.List
	rangeIDs              posting.List
	probeIDs              posting.List
}

var overlayRangePredicateStatePool sync.Pool

func acquireOverlayRangePredicateState(
	ov fieldOverlay,
	br overlayRange,
	probe overlayRangeProbe,
	bucketCount int,
	est uint64,
	neg bool,
	reuse materializedPredReuse,
	usePostingFilter bool,
) *overlayRangePredicateState {
	linearContainsMax := rangeLinearContainsLimit(probe.probeLen, probe.probeEst)
	materializeAfter := rangeMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	rangeMaterializeAt := rangePostingFilterMaterializeAfterForProbe(bucketCount, est)
	var state *overlayRangePredicateState
	if v := overlayRangePredicateStatePool.Get(); v != nil {
		state = v.(*overlayRangePredicateState)
	} else {
		state = new(overlayRangePredicateState)
	}
	state.ov = ov
	state.br = br
	state.probe = probe
	state.reuse = reuse
	state.neg = neg
	state.bucketCount = bucketCount
	state.linearContainsMax = linearContainsMax
	state.materializeAfter = materializeAfter
	state.rangeMaterializeAt = rangeMaterializeAt
	state.keepProbeHits = probe.useComplement == neg
	state.probePostingFilter = false
	state.postingFilterCheap = false
	state.probeMaterializeAt = 0
	if usePostingFilter {
		totalBuckets := probe.ov.keyCount()
		inBuckets := 0
		for i := 0; i < probe.spanCnt; i++ {
			inBuckets += probe.spans[i].baseEnd - probe.spans[i].baseStart
		}
		if probe.useComplement {
			inBuckets = totalBuckets - inBuckets
		}
		if totalBuckets > 0 && inBuckets > 0 && inBuckets < totalBuckets && probe.probeLen > 0 {
			state.probePostingFilter = true
			state.postingFilterCheap = !state.keepProbeHits
			state.probeMaterializeAt = rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
		}
	}
	state.setExpectedContainsCalls(materializeAfter)
	return state
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
	state.linearPostsBuf.values = state.linearPosts
	releasePostingSliceBuf(state.linearPostsBuf)
	state.linearPostsBuf = nil
	state.linearPosts = nil
}

func releaseOverlayRangePredicateState(state *overlayRangePredicateState) {
	state.releaseLinearPosts()
	state.probeIDs.Release()
	state.rangeIDs.Release()
	*state = overlayRangePredicateState{}
	overlayRangePredicateStatePool.Put(state)
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

func (state *overlayRangePredicateState) ensureLinearPosts() []posting.List {
	if state == nil {
		return nil
	}
	if state.linearPostsBuf != nil {
		return state.linearPosts
	}
	state.linearPostsBuf = getPostingSliceBuf(state.bucketCount)
	state.linearPostsBuf.values = state.linearPostsBuf.values[:0]
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
			state.linearPostsBuf.values = append(state.linearPostsBuf.values, ids)
		}
	}
	state.linearPosts = state.linearPostsBuf.values
	return state.linearPosts
}

func (state *overlayRangePredicateState) linearContains(idx uint64) bool {
	for _, ids := range state.ensureLinearPosts() {
		if ids.Contains(idx) {
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
	for i := 0; i < state.probe.spanCnt; i++ {
		part := overlayUnionRange(state.ov, state.probe.spans[i])
		state.probeIDs = state.probeIDs.BuildMergedOwned(part)
	}
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
			return applyRangeProbePostingFilter(dst, state.keepProbeHits, state.probe.probeLen, state.probe.linearContains, state.probe.forEachPosting)
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
	return acquireOverlayRangeIter(state.ov, state.br)
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
	linearPostsBuf     *postingSliceBuf
	linearPosts        []posting.List
	ids                posting.List
}

var baseRangePredicateStatePool sync.Pool

func acquireBaseRangePredicateState(
	probe baseRangeProbe,
	neg bool,
	reuse materializedPredReuse,
) *baseRangePredicateState {
	keepProbeHits := probe.useComplement == neg
	linearContainsMax := rangeLinearContainsLimit(probe.probeLen, probe.probeEst)
	materializeAfter := rangeMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	hashSetAfter := rangeHashSetAfterForProbe(probe.probeLen, probe.probeEst)
	postingFilterMaterializeAt := rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	var state *baseRangePredicateState
	if v := baseRangePredicateStatePool.Get(); v != nil {
		state = v.(*baseRangePredicateState)
	} else {
		state = &baseRangePredicateState{}
	}
	state.probe = probe
	state.reuse = reuse
	state.keepProbeHits = keepProbeHits
	state.neg = neg
	state.linearContainsMax = linearContainsMax
	state.hashSetAfter = hashSetAfter
	state.materializeAfter = materializeAfter
	state.postingFilterMaterializeAt = postingFilterMaterializeAt
	state.setExpectedContainsCalls(materializeAfter)
	return state
}

func releaseBaseRangePredicateState(state *baseRangePredicateState) {
	if state.hasSet {
		releaseU64Set(&state.set)
		state.hasSet = false
	}
	if state.linearPostsBuf != nil {
		state.linearPostsBuf.values = state.linearPosts
		releasePostingSliceBuf(state.linearPostsBuf)
		state.linearPostsBuf = nil
		state.linearPosts = nil
	}
	state.ids.Release()
	*state = baseRangePredicateState{}
	baseRangePredicateStatePool.Put(state)
}

func tryShareMaterializedPredOnSnapshot(snap *indexSnapshot, cacheKey string, ids posting.List) posting.List {
	if snap == nil || cacheKey == "" {
		return ids
	}
	if snap.matPredCacheMaxEntries <= 0 {
		return ids
	}
	if ids.IsEmpty() {
		snap.storeMaterializedPred(cacheKey, posting.List{})
		return ids
	}
	if next, ok := snap.loadOrStoreMaterializedPred(cacheKey, ids); ok {
		return next
	}
	if next, ok := snap.tryLoadOrStoreMaterializedPredOversized(cacheKey, ids); ok {
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
	postsBuf := getPostingSliceBuf(probe.probeLen)
	postsBuf.values = postsBuf.values[:0]
	probe.forEachPosting(func(ids posting.List) bool {
		postsBuf.values = append(postsBuf.values, ids)
		return true
	})
	out := materializePostingUnionOwned(postsBuf.values)
	releasePostingSliceBuf(postsBuf)
	return out
}

func (state *baseRangePredicateState) ensureLinearPosts() []posting.List {
	if state == nil {
		return nil
	}
	if state.linearPostsBuf != nil {
		return state.linearPosts
	}
	state.linearPostsBuf = getPostingSliceBuf(state.probe.probeLen)
	state.linearPostsBuf.values = state.linearPostsBuf.values[:0]
	state.probe.forEachPosting(func(ids posting.List) bool {
		state.linearPostsBuf.values = append(state.linearPostsBuf.values, ids)
		return true
	})
	state.linearPosts = state.linearPostsBuf.values
	return state.linearPosts
}

func (state *baseRangePredicateState) linearContains(idx uint64) bool {
	if state == nil {
		return false
	}
	if state.expectedContainsCalls <= 1 {
		return state.probe.linearContains(idx)
	}
	for _, ids := range state.ensureLinearPosts() {
		if ids.Contains(idx) {
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
			state.probe.forEachPosting(func(b posting.List) bool {
				b.ForEach(func(v uint64) bool {
					state.set.Add(v)
					return true
				})
				return true
			})
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
		return applyRangeProbePostingFilter(dst, state.keepProbeHits, state.probe.probeLen, state.probe.linearContains, state.probe.forEachPosting)
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
	cacheKey string
	mode     materializedPredReuseMode
}

func newMaterializedPredReadOnlyReuse(snap *indexSnapshot, cacheKey string) materializedPredReuse {
	if snap == nil || cacheKey == "" {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseReadOnly,
	}
}

func newMaterializedPredSharedReuse(snap *indexSnapshot, cacheKey string) materializedPredReuse {
	if snap == nil || cacheKey == "" {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseShared,
	}
}

func newMaterializedPredSecondHitSharedReuse(snap *indexSnapshot, cacheKey string) materializedPredReuse {
	if snap == nil || cacheKey == "" {
		return materializedPredReuse{}
	}
	return materializedPredReuse{
		snap:     snap,
		cacheKey: cacheKey,
		mode:     materializedPredReuseSecondHitShared,
	}
}

func (reuse materializedPredReuse) load() (posting.List, bool) {
	if reuse.mode == materializedPredReuseNone || reuse.snap == nil || reuse.cacheKey == "" {
		return posting.List{}, false
	}
	return reuse.snap.loadMaterializedPred(reuse.cacheKey)
}

func (reuse materializedPredReuse) share(ids posting.List) posting.List {
	if (reuse.mode != materializedPredReuseShared && reuse.mode != materializedPredReuseSecondHitShared) ||
		reuse.snap == nil || reuse.cacheKey == "" {
		return ids
	}
	if reuse.mode == materializedPredReuseSecondHitShared &&
		!reuse.snap.shouldPromoteRuntimeMaterializedPred(reuse.cacheKey) {
		return ids
	}
	return tryShareMaterializedPredOnSnapshot(reuse.snap, reuse.cacheKey, ids)
}

func (reuse materializedPredReuse) canSecondHitShareModeratelyOversizedEstimate(est uint64) bool {
	if reuse.mode != materializedPredReuseSecondHitShared || reuse.snap == nil || reuse.cacheKey == "" || est == 0 {
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

func (qv *queryView[K, V]) materializedPredReadOnlyReuseForScalar(
	field string,
	bound normalizedScalarBound,
	useComplement bool,
) materializedPredReuse {
	if bound.full {
		return materializedPredReuse{}
	}
	cacheKey := qv.materializedPredCacheKeyForScalar(field, bound.op, bound.key)
	if useComplement {
		cacheKey = qv.materializedPredComplementCacheKeyForScalar(field, bound.op, bound.key)
		return newMaterializedPredReadOnlyReuse(qv.snap, cacheKey)
	}
	return newMaterializedPredReadOnlyReuse(qv.snap, cacheKey)
}

func (qv *queryView[K, V]) materializedPredReadOnlyReuseForExactScalarRange(
	field string,
	bounds rangeBounds,
) materializedPredReuse {
	return newMaterializedPredReadOnlyReuse(qv.snap, materializedPredCacheKeyForExactScalarRange(field, bounds))
}

func (qv *queryView[K, V]) materializedPredSharedReuseForScalar(
	field string,
	bound normalizedScalarBound,
) materializedPredReuse {
	if bound.full {
		return materializedPredReuse{}
	}
	cacheKey := qv.materializedPredCacheKeyForScalar(field, bound.op, bound.key)
	return newMaterializedPredSharedReuse(qv.snap, cacheKey)
}

func (qv *queryView[K, V]) materializedPredSharedReuseForExactScalarRange(
	field string,
	bounds rangeBounds,
) materializedPredReuse {
	return newMaterializedPredSharedReuse(qv.snap, materializedPredCacheKeyForExactScalarRange(field, bounds))
}

func (qv *queryView[K, V]) materializedPredSecondHitSharedReuseForScalar(
	field string,
	bound normalizedScalarBound,
) materializedPredReuse {
	if bound.full {
		return materializedPredReuse{}
	}
	cacheKey := qv.materializedPredCacheKeyForScalar(field, bound.op, bound.key)
	return newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)
}

func (qv *queryView[K, V]) materializedPredSecondHitSharedReuseForExactScalarRange(
	field string,
	bounds rangeBounds,
) materializedPredReuse {
	return newMaterializedPredSecondHitSharedReuse(qv.snap, materializedPredCacheKeyForExactScalarRange(field, bounds))
}

/**/

const (
	singleChunkCap       = 32768
	singleAdaptiveMaxLen = 200_000
)

var singleIDsPool = sync.Pool{
	New: func() any {
		return &singleIDsBuffer{
			values: make([]uint64, 0, singleChunkCap),
		}
	},
}

type singleIDsBuffer struct {
	values []uint64
}

func getSingleIDsBuf() *singleIDsBuffer {
	return singleIDsPool.Get().(*singleIDsBuffer)
}

func releaseSingleIDs(buf *singleIDsBuffer) {
	if buf == nil || cap(buf.values) != singleChunkCap {
		return
	}
	buf.values = buf.values[:0]
	singleIDsPool.Put(buf)
}

type postingUnionBuilder struct {
	ids     posting.List
	singles *singleIDsBuffer
}

func postingBatchSinglesEnabled(capHint uint64) bool {
	return capHint > posting.SmallCap && capHint <= singleAdaptiveMaxLen
}

func newPostingUnionBuilder(batchSingles bool) postingUnionBuilder {
	if !batchSingles {
		return postingUnionBuilder{}
	}
	return postingUnionBuilder{singles: getSingleIDsBuf()}
}

func (b *postingUnionBuilder) flushSingles() {
	if b.singles == nil || len(b.singles.values) == 0 {
		return
	}
	slices.Sort(b.singles.values)
	b.ids = b.ids.BuildAddedMany(b.singles.values)
	b.singles.values = b.singles.values[:0]
}

func (b *postingUnionBuilder) addSingle(idx uint64) {
	if b.singles == nil {
		b.ids = b.ids.BuildAdded(idx)
		return
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
	b.ids = b.ids.BuildOr(ids)
}

func (b *postingUnionBuilder) finish(optimize bool) posting.List {
	b.flushSingles()
	out := b.ids
	b.ids = posting.List{}
	if b.singles != nil {
		releaseSingleIDs(b.singles)
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
	if b.singles != nil {
		releaseSingleIDs(b.singles)
		b.singles = nil
	}
}

type postingSetBuilder struct {
	ids     posting.List
	seen    u64set
	singles *singleIDsBuffer
	count   uint64
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
		seen: newU64Set(normalizePostingSetCapHint(capHint)),
	}
	if batchSingles {
		b.singles = getSingleIDsBuf()
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
	if b.singles == nil || len(b.singles.values) == 0 {
		return
	}
	slices.Sort(b.singles.values)
	b.ids = b.ids.BuildAddedMany(b.singles.values)
	b.singles.values = b.singles.values[:0]
}

func (b *postingSetBuilder) addChecked(idx uint64) bool {
	if !b.seen.Add(idx) {
		return false
	}
	b.count++
	if b.singles == nil {
		b.ids = b.ids.BuildAdded(idx)
		return true
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
	if b.singles != nil {
		releaseSingleIDs(b.singles)
		b.singles = nil
	}
	b.count = 0
}
