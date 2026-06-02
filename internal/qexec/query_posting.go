package qexec

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type postingResult struct {
	ids posting.List
	neg bool
}

func releasePostingResults(buf []postingResult) {
	for i := 0; i < len(buf); i++ {
		buf[i].ids.Release()
	}
}

type emptyIter struct{}

func (emptyIter) HasNext() bool { return false }
func (emptyIter) Next() uint64  { return 0 }
func (emptyIter) Release()      {}

type postingConcatIter struct {
	posts []posting.List
	i     int
	curIt posting.Iterator
}

var postingConcatIterPool = pooled.Pointers[postingConcatIter]{
	Cleanup: func(it *postingConcatIter) {
		if it.curIt != nil {
			it.curIt.Release()
		}
	},
	Clear: true,
}

func newPostingConcatBufIter(posts []posting.List) posting.Iterator {
	it := postingConcatIterPool.Get()
	it.posts = posts
	return it
}

func (it *postingConcatIter) postCount() int {
	return len(it.posts)
}

func (it *postingConcatIter) postAt(i int) posting.List {
	return it.posts[i]
}

func (it *postingConcatIter) HasNext() bool {
	for {
		if it.curIt != nil && it.curIt.HasNext() {
			return true
		}
		if it.curIt != nil {
			it.curIt.Release()
			it.curIt = nil
		}
		if it.i >= it.postCount() {
			return false
		}
		p := it.postAt(it.i)
		it.i++
		if p.IsEmpty() {
			continue
		}
		it.curIt = p.Iter()
	}
}

func (it *postingConcatIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	return it.curIt.Next()
}

func (it *postingConcatIter) Release() {
	postingConcatIterPool.Put(it)
}

type postingUnionIter struct {
	posts []posting.List
	i     int
	curIt posting.Iterator
	seen  u64set
	next  uint64
	has   bool
}

type postingSmallUnionIter struct {
	posts []posting.List
	i     int
	curIt posting.Iterator
	next  uint64
	has   bool
}

var postingSmallUnionIterPool = pooled.Pointers[postingSmallUnionIter]{
	Cleanup: func(it *postingSmallUnionIter) {
		if it.curIt != nil {
			it.curIt.Release()
		}
	},
	Clear: true,
}

var postingUnionIterPool = pooled.Pointers[postingUnionIter]{
	Clear: true,
}

func newPostingUnionIter(posts []posting.List) posting.Iterator {
	if len(posts) > 1 && len(posts) <= 3 {
		it := postingSmallUnionIterPool.Get()
		it.posts = posts
		return it
	}
	capHint := min(max(len(posts)*16, 64), 1024)
	it := postingUnionIterPool.Get()
	it.posts = posts
	it.seen = getU64Set(capHint)
	return it
}

func (u *postingSmallUnionIter) postCount() int {
	return len(u.posts)
}

func (u *postingSmallUnionIter) postAt(i int) posting.List {
	return u.posts[i]
}

func (u *postingUnionIter) postCount() int {
	return len(u.posts)
}

func (u *postingUnionIter) postAt(i int) posting.List {
	return u.posts[i]
}

func (u *postingSmallUnionIter) HasNext() bool {
	if u.has {
		return true
	}
	for {
		if u.curIt != nil {
			for u.curIt.HasNext() {
				v := u.curIt.Next()
				dup := false
				for j := 0; j < u.i-1; j++ {
					if u.postAt(j).Contains(v) {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				u.next = v
				u.has = true
				return true
			}
			u.curIt.Release()
			u.curIt = nil
		}
		if u.i >= u.postCount() {
			return false
		}
		p := u.postAt(u.i)
		u.i++
		if p.IsEmpty() {
			continue
		}
		u.curIt = p.Iter()
	}
}

func (u *postingSmallUnionIter) Next() uint64 {
	if !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

func (u *postingSmallUnionIter) Release() {
	postingSmallUnionIterPool.Put(u)
}

func (u *postingUnionIter) HasNext() bool {
	if u.has {
		return true
	}
	for {
		if u.curIt != nil {
			for u.curIt.HasNext() {
				v := u.curIt.Next()
				if u.seen.Add(v) {
					u.next = v
					u.has = true
					return true
				}
			}
			u.curIt.Release()
			u.curIt = nil
		}
		if u.i >= u.postCount() {
			return false
		}
		p := u.postAt(u.i)
		u.i++
		if p.IsEmpty() {
			continue
		}
		u.curIt = p.Iter()
	}
}

func (u *postingUnionIter) Next() uint64 {
	if !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

func (u *postingUnionIter) Release() {
	if u.curIt != nil {
		u.curIt.Release()
		u.curIt = nil
	}
	u.posts = nil
	releaseU64Set(&u.seen)
	postingUnionIterPool.Put(u)
}
