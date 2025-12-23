package rbi

import (
	"strings"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

type leafPred struct {
	iter     func() roaringIter
	contains func(uint64) bool
	estCard  uint64
}

type roaringIter interface {
	HasNext() bool
	Next() uint64
}

// should it be the main path now?

func (db *DB[K, V]) tryLimitQuery(q *qx.QX) ([]K, bool, error) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false, nil
	}
	if q.Expr.Not {
		return nil, false, nil
	}

	leaves, ok := extractAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		return nil, false, nil
	}

	if len(q.Order) == 1 && q.Order[0].Type == qx.OrderBasic {
		return db.tryLimitQueryOrderBasic(q, leaves)
	}

	if f, bounds, ok := db.tryExtractFieldBoundsNoOrder(leaves); ok {
		return db.tryLimitQueryByFieldBounds(q, leaves, f, bounds)
	}

	return db.tryLimitQueryNoOrder(q, leaves)
}

func (db *DB[K, V]) tryLimitQueryNoOrder(q *qx.QX, leaves []qx.Expr) ([]K, bool, error) {

	preds := make([]leafPred, 0, len(leaves))

	for _, e := range leaves {
		if isBoundOp(e.Op) {
			return nil, false, nil
		}
		lp, ok, err := db.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		preds = append(preds, lp)
	}

	lead := pickLead(preds)
	if lead == nil {
		return nil, false, nil
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	limit := int(q.Limit)
	out := make([]K, 0, limit)

	iter := lead.iter()
	for iter.HasNext() {
		idx := iter.Next()

		pass := true
		for _, p := range preds {
			if !p.contains(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		out = append(out, db.idFromIdxNoLock(idx))
		if len(out) == limit {
			return out, true, nil
		}
	}

	return out, true, nil
}

func (db *DB[K, V]) tryLimitQueryOrderBasic(q *qx.QX, leaves []qx.Expr) ([]K, bool, error) {
	order := q.Order[0]

	f := order.Field
	if f == "" {
		return nil, false, nil
	}

	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	slice := db.index[f]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	bounds, rest, ok, err := db.extractBoundsForField(f, leaves)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}

	preds := make([]leafPred, 0, len(rest))
	for _, e := range rest {
		if isBoundOp(e.Op) {
			return nil, false, nil
		}
		lp, ok, err := db.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		preds = append(preds, lp)
	}

	start, end := applyBoundsToIndexRange(s, bounds)

	if start >= end {
		return nil, true, nil
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	limit := int(q.Limit)
	out := make([]K, 0, limit)

	emitBucket := func(bm *roaring64.Bitmap) bool {
		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()

			pass := true
			for _, p := range preds {
				if !p.contains(idx) {
					pass = false
					break
				}
			}
			if !pass {
				continue
			}

			out = append(out, db.idFromIdxNoLock(idx))
			if len(out) == limit {
				return true
			}
		}
		return false
	}

	if !order.Desc {
		for i := start; i < end; i++ {
			if s[i].IDs == nil || s[i].IDs.IsEmpty() {
				continue
			}
			if emitBucket(s[i].IDs) {
				return out, true, nil
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if s[i].IDs == nil || s[i].IDs.IsEmpty() {
				if i == start {
					break
				}
				continue
			}
			if emitBucket(s[i].IDs) {
				return out, true, nil
			}
			if i == start {
				break
			}
		}
	}

	return out, true, nil
}

func (db *DB[K, V]) tryExtractFieldBoundsNoOrder(leaves []qx.Expr) (string, rangeBounds, bool) {
	var f string
	var out rangeBounds

	found := false
	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		if e.Not || e.Field == "" {
			return "", out, false
		}
		if !found {
			found = true
			f = e.Field
		} else if e.Field != f {
			return "", out, false
		}
		out.has = true
	}
	return f, out, found
}

func (db *DB[K, V]) tryLimitQueryByFieldBounds(q *qx.QX, leaves []qx.Expr, field string, _ rangeBounds) ([]K, bool, error) {

	fm := db.fields[field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	slice := db.index[field]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	bounds, rest, ok, err := db.extractBoundsForField(field, leaves)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}

	preds := make([]leafPred, 0, len(rest))
	for _, e := range rest {
		if isBoundOp(e.Op) {
			return nil, false, nil
		}
		p, ok, err := db.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		preds = append(preds, p)
	}

	start, end := applyBoundsToIndexRange(s, bounds)
	if start >= end {
		return nil, true, nil
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	limit := int(q.Limit)
	out := make([]K, 0, limit)

	for i := start; i < end; i++ {
		bm := s[i].IDs
		if bm == nil || bm.IsEmpty() {
			continue
		}
		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()

			pass := true
			for _, p := range preds {
				if !p.contains(idx) {
					pass = false
					break
				}
			}
			if !pass {
				continue
			}

			out = append(out, db.idFromIdxNoLock(idx))
			if len(out) == limit {
				return out, true, nil
			}
		}
	}

	return out, true, nil
}

func isBoundOp(op qx.Op) bool {
	switch op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return true
	default:
		return false
	}
}

func (db *DB[K, V]) extractBoundsForField(field string, leaves []qx.Expr) (rangeBounds, []qx.Expr, bool, error) {
	var b rangeBounds
	rest := make([]qx.Expr, 0, len(leaves))

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			rest = append(rest, e)
			continue
		}
		if e.Not || e.Field == "" {
			return b, nil, false, nil
		}
		if e.Field != field {
			return b, nil, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return b, nil, true, err
		}
		if isSlice || len(keys) != 1 {
			return b, nil, false, nil
		}
		k := keys[0]

		b.has = true

		switch e.Op {
		case qx.OpGT:
			b.applyLo(k, false)
		case qx.OpGTE:
			b.applyLo(k, true)
		case qx.OpLT:
			b.applyHi(k, false)
		case qx.OpLTE:
			b.applyHi(k, true)
		case qx.OpPREFIX:
			b.hasPrefix = true
			b.prefix = k
		}
	}

	return b, rest, true, nil
}

func applyBoundsToIndexRange(s []index, b rangeBounds) (start, end int) {
	start = 0
	end = len(s)

	if b.hasPrefix {
		p := b.prefix
		start = lowerBoundIndex(s, p)
		end = start
		for end < len(s) {
			if !strings.HasPrefix(s[end].Key, p) {
				break
			}
			end++
		}
		return start, end
	}

	if b.hasLo {
		start = lowerBoundIndex(s, b.loKey)
		if !b.loInc {
			if start < len(s) && s[start].Key == b.loKey {
				start++
			}
		}
	}
	if b.hasHi {
		if b.hiInc {
			end = upperBoundIndex(s, b.hiKey)
		} else {
			end = lowerBoundIndex(s, b.hiKey)
		}
	}

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return 0, 0
	}
	return start, end
}

func (db *DB[K, V]) buildLeafPred(e qx.Expr) (leafPred, bool, error) {
	if e.Not || e.Field == "" {
		return leafPred{}, false, nil
	}

	slice := db.index[e.Field]
	if slice == nil {
		return leafPred{}, false, nil
	}

	fm := db.fields[e.Field]
	if fm == nil {
		return leafPred{}, false, nil
	}

	switch e.Op {

	case qx.OpEQ:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if isSlice || len(keys) != 1 {
			return leafPred{}, false, nil
		}

		bm := findIndex(slice, keys[0])
		if bm == nil {
			return emptyLeaf(), true, nil
		}

		return leafPred{
			iter: func() roaringIter { return bm.Iterator() },
			contains: func(idx uint64) bool {
				return bm.Contains(idx)
			},
			estCard: bm.GetCardinality(),
		}, true, nil

	case qx.OpIN:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		bms, est := lookupMany(slice, keys)
		if len(bms) == 0 {
			return emptyLeaf(), true, nil
		}

		return leafPred{
			iter: func() roaringIter { return newUnionIter(bms) },
			contains: func(idx uint64) bool {
				for _, bm := range bms {
					if bm.Contains(idx) {
						return true
					}
				}
				return false
			},
			estCard: est,
		}, true, nil

	case qx.OpHAS:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		bms := make([]*roaring64.Bitmap, 0, len(keys))
		var est uint64
		for _, k := range keys {
			bm := findIndex(slice, k)
			if bm == nil {
				return emptyLeaf(), true, nil
			}
			bms = append(bms, bm)
			c := bm.GetCardinality()
			if est == 0 || c < est {
				est = c
			}
		}

		leadBm := minCardBM(bms)

		return leafPred{
			iter: func() roaringIter { return leadBm.Iterator() },
			contains: func(idx uint64) bool {
				for _, bm := range bms {
					if !bm.Contains(idx) {
						return false
					}
				}
				return true
			},
			estCard: est,
		}, true, nil

	case qx.OpHASANY:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		bms, est := lookupMany(slice, keys)
		if len(bms) == 0 {
			return emptyLeaf(), true, nil
		}

		return leafPred{
			iter: func() roaringIter { return newUnionIter(bms) },
			contains: func(idx uint64) bool {
				for _, bm := range bms {
					if bm.Contains(idx) {
						return true
					}
				}
				return false
			},
			estCard: est,
		}, true, nil
	}

	return leafPred{}, false, nil
}

func pickLead(ps []leafPred) *leafPred {
	if len(ps) == 0 {
		return nil
	}
	best := &ps[0]
	for i := 1; i < len(ps); i++ {
		if ps[i].estCard < best.estCard {
			best = &ps[i]
		}
	}
	return best
}

func extractAndLeaves(e qx.Expr) ([]qx.Expr, bool) {
	switch e.Op {
	case qx.OpNOOP:
		return nil, false
	case qx.OpAND:
		if len(e.Operands) == 0 {
			return nil, false
		}
		var out []qx.Expr
		for _, ch := range e.Operands {
			sub, ok := extractAndLeaves(ch)
			if !ok {
				return nil, false
			}
			out = append(out, sub...)
		}
		return out, true
	default:
		if e.Not {
			return nil, false
		}
		return []qx.Expr{e}, true
	}
}

func lookupMany(slice *[]index, keys []string) ([]*roaring64.Bitmap, uint64) {
	bms := make([]*roaring64.Bitmap, 0, len(keys))
	var est uint64
	for _, k := range keys {
		bm := findIndex(slice, k)
		if bm != nil && !bm.IsEmpty() {
			bms = append(bms, bm)
			est += bm.GetCardinality()
		}
	}
	return bms, est
}

func minCardBM(bms []*roaring64.Bitmap) *roaring64.Bitmap {
	if len(bms) == 0 {
		return nil
	}
	best := bms[0]
	bestC := best.GetCardinality()
	for i := 1; i < len(bms); i++ {
		if c := bms[i].GetCardinality(); c < bestC {
			best = bms[i]
			bestC = c
		}
	}
	return best
}

func emptyLeaf() leafPred {
	return leafPred{
		iter:     func() roaringIter { return emptyIter{} },
		contains: func(uint64) bool { return false },
		estCard:  0,
	}
}

/**/

type emptyIter struct{}

func (emptyIter) HasNext() bool { return false }
func (emptyIter) Next() uint64  { return 0 }

/**/

type unionIter struct {
	iters []roaringIter
	i     int
	seen  u64set
	next  uint64
	has   bool
}

func newUnionIter(bms []*roaring64.Bitmap) roaringIter {
	iters := make([]roaringIter, 0, len(bms))
	for _, bm := range bms {
		iters = append(iters, bm.Iterator())
	}
	return &unionIter{
		iters: iters,
		seen:  newU64Set(256),
	}
}

func (u *unionIter) HasNext() bool {
	if u.has {
		return true
	}
	for u.i < len(u.iters) {
		it := u.iters[u.i]
		for it.HasNext() {
			v := it.Next()
			if u.seen.Add(v) {
				u.next = v
				u.has = true
				return true
			}
		}
		u.i++
	}
	return false
}

func (u *unionIter) Next() uint64 {
	if !u.has && !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

/**/

type u64set struct {
	keys []uint64
	used []byte
	mask uint64
	n    int
}

func newU64Set(capHint int) u64set {
	n := 1
	for n < capHint*2 {
		n <<= 1
	}
	return u64set{
		keys: make([]uint64, n),
		used: make([]byte, n),
		mask: uint64(n - 1),
	}
}

func (s *u64set) Add(x uint64) bool {
	if s.n*2 >= len(s.keys) {
		s.grow()
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			s.used[i] = 1
			s.keys[i] = x
			s.n++
			return true
		}
		if s.keys[i] == x {
			return false
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) grow() {
	oldK := s.keys
	oldU := s.used

	n := len(oldK) << 1
	s.keys = make([]uint64, n)
	s.used = make([]byte, n)
	s.mask = uint64(n - 1)
	s.n = 0

	for i := 0; i < len(oldK); i++ {
		if oldU[i] != 0 {
			_ = s.Add(oldK[i])
		}
	}
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
