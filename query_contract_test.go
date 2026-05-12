package rbi

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/vapstack/qx"
)

type queryContract[K ~uint64 | ~string] struct {
	t         testing.TB
	db        *DB[K, Rec]
	reference func(testing.TB, *DB[K, Rec], *qx.QX) ([]K, error)
	equal     func(*qx.QX, []K, []K) bool
}

type queryContractReference[K ~uint64 | ~string] struct {
	pageKeys []K
	fullKeys []K
	pageSet  bool
	fullSet  bool
}

func (r *queryContractReference[K]) page(c queryContract[K], q *qx.QX) []K {
	if !r.pageSet {
		keys, err := c.reference(c.t, c.db, q)
		if err != nil {
			c.t.Fatalf("reference keys(%+v): %v", q, err)
		}
		r.pageKeys = keys
		r.pageSet = true
	}
	return r.pageKeys
}

func (r *queryContractReference[K]) full(c queryContract[K], q *qx.QX) []K {
	if !r.fullSet {
		if q.Window.Offset == 0 && q.Window.Limit == 0 && len(q.Order) == 0 {
			r.fullKeys = r.page(c, q)
		} else {
			fullQ := cloneQuery(q)
			clearQueryOrderWindowForTest(fullQ)
			keys, err := c.reference(c.t, c.db, fullQ)
			if err != nil {
				c.t.Fatalf("reference keys(%+v): %v", fullQ, err)
			}
			r.fullKeys = keys
		}
		r.fullSet = true
	}
	return r.fullKeys
}

func (r *queryContractReference[K]) count(c queryContract[K], q *qx.QX) uint64 {
	if q.Window.Offset == 0 && q.Window.Limit == 0 && len(q.Order) == 0 {
		return uint64(len(r.page(c, q)))
	}
	return uint64(len(r.full(c, q)))
}

func newUint64QueryContract(t testing.TB, db *DB[uint64, Rec]) queryContract[uint64] {
	t.Helper()
	return queryContract[uint64]{
		t:         t,
		db:        db,
		reference: expectedKeysUint64,
		equal:     queryIDsEqual,
	}
}

func newStringQueryContract(t testing.TB, db *DB[string, Rec]) queryContract[string] {
	t.Helper()
	return queryContract[string]{
		t:         t,
		db:        db,
		reference: expectedKeysString,
		equal:     queryStringIDsEqual,
	}
}

func (c queryContract[K]) ReferenceKeys(q *qx.QX) []K {
	c.t.Helper()
	var ref queryContractReference[K]
	return ref.page(c, q)
}

func (c queryContract[K]) ReferenceFullKeys(q *qx.QX) []K {
	c.t.Helper()
	var ref queryContractReference[K]
	return ref.full(c, q)
}

func (c queryContract[K]) AssertQueryKeysMatchReference(q *qx.QX) []K {
	c.t.Helper()
	got, err := c.db.QueryKeys(q)
	if err != nil {
		c.t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	var ref queryContractReference[K]
	c.assertKeysMatchReference("QueryKeys", q, got, &ref)
	return got
}

func (c queryContract[K]) AssertPreparedKeysMatchReference(q *qx.QX) []K {
	c.t.Helper()

	nq := normalizeQueryForTest(q)
	if err := c.db.engine.checkUsedQuery(nq); err != nil {
		c.t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	got, err := execPreparedQueryForTest(c.db, nq)
	if err != nil {
		c.t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	var ref queryContractReference[K]
	c.assertKeysMatchReference("execPreparedQuery", q, got, &ref)
	return got
}

func (c queryContract[K]) AssertKeysMatchReference(label string, q *qx.QX, got []K) {
	c.t.Helper()
	var ref queryContractReference[K]
	c.assertKeysMatchReference(label, q, got, &ref)
}

func (c queryContract[K]) assertKeysMatchReference(label string, q *qx.QX, got []K, ref *queryContractReference[K]) {
	c.t.Helper()
	if err := queryContractValidateNoDuplicateKeys(label, got); err != nil {
		c.t.Fatal(err)
	}

	if queryContractNoOrderWindow(q) {
		full := ref.full(c, q)
		if err := queryContractValidateNoOrderWindow(q, got, full); err != nil {
			c.t.Fatalf("%s no-order window mismatch: %v\ngot=%v\nfull=%v", label, err, got, full)
		}
		return
	}

	want := ref.page(c, q)
	if !c.equal(q, got, want) {
		c.t.Fatalf("%s mismatch:\nq=%+v\ngot=%v\nwant=%v", label, q, got, want)
	}
}

func (c queryContract[K]) AssertQueryRecordsMatchReference(q *qx.QX) []*Rec {
	c.t.Helper()

	got, err := c.db.Query(q)
	if err != nil {
		c.t.Fatalf("Query(%+v): %v", q, err)
	}

	if len(q.Order) > 0 {
		wantIDs := c.ReferenceKeys(q)
		want := c.batchGet("BatchGet(reference keys)", wantIDs)
		queryContractAssertRecordSlicesEqual(c.t, "Query", q, got, want)
		return got
	}

	fullIDs := c.ReferenceFullKeys(q)
	full := c.batchGet("BatchGet(full reference keys)", fullIDs)
	if queryContractNoOrderWindow(q) {
		if err := queryContractValidateNoOrderRecordsWindow(q, got, full); err != nil {
			c.t.Fatalf("Query no-order window mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
		}
		return got
	}

	if err := queryContractValidateNoOrderRecordsFull(got, full); err != nil {
		c.t.Fatalf("Query no-order full-set mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
	}
	return got
}

func (c queryContract[K]) AssertQueryRecordsMatchKeys(q *qx.QX, keys []K) []*Rec {
	c.t.Helper()

	got, err := c.db.Query(q)
	if err != nil {
		c.t.Fatalf("Query(%+v): %v", q, err)
	}
	want := c.batchGet("BatchGet(QueryKeys result)", keys)
	queryContractAssertRecordSlicesEqual(c.t, "Query vs QueryKeys", q, got, want)
	return got
}

func (c queryContract[K]) AssertCountMatchesReference(q *qx.QX) uint64 {
	c.t.Helper()

	var ref queryContractReference[K]
	return c.assertCountMatchesReference(q, &ref)
}

func (c queryContract[K]) assertCountMatchesReference(q *qx.QX, ref *queryContractReference[K]) uint64 {
	c.t.Helper()

	want := ref.count(c, q)
	got, err := c.db.Count(q.Filter)
	if err != nil {
		c.t.Fatalf("Count(%+v): %v", q, err)
	}
	if got != want {
		c.t.Fatalf("Count mismatch:\nq=%+v\ngot=%d\nwant=%d", q, got, want)
	}
	return got
}

func (c queryContract[K]) AssertPreparedCountMatchesReference(q *qx.QX) uint64 {
	c.t.Helper()

	var ref queryContractReference[K]
	return c.assertPreparedCountMatchesReference(q, &ref)
}

func (c queryContract[K]) assertPreparedCountMatchesReference(q *qx.QX, ref *queryContractReference[K]) uint64 {
	c.t.Helper()

	nq := normalizeQueryForTest(q)
	want := ref.count(c, q)
	got, err := c.db.engine.aggregateCountPreparedExpr(nq.Filter)
	if err != nil {
		c.t.Fatalf("aggregateCountPreparedExpr(%+v): %v", nq.Filter, err)
	}
	if got != want {
		c.t.Fatalf("aggregateCountPreparedExpr mismatch:\nq=%+v\ngot=%d\nwant=%d", nq, got, want)
	}
	return got
}

func (c queryContract[K]) assertAllReadPathsMatchReference(q *qx.QX) queryContractReference[K] {
	c.t.Helper()
	var ref queryContractReference[K]

	keys, err := c.db.QueryKeys(q)
	if err != nil {
		c.t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	c.assertKeysMatchReference("QueryKeys", q, keys, &ref)

	got, err := c.db.Query(q)
	if err != nil {
		c.t.Fatalf("Query(%+v): %v", q, err)
	}

	if len(q.Order) > 0 {
		wantIDs := ref.page(c, q)
		want := c.batchGet("BatchGet(reference keys)", wantIDs)
		queryContractAssertRecordSlicesEqual(c.t, "Query", q, got, want)
	} else {
		fullIDs := ref.full(c, q)
		full := c.batchGet("BatchGet(full reference keys)", fullIDs)
		if queryContractNoOrderWindow(q) {
			if err := queryContractValidateNoOrderRecordsWindow(q, got, full); err != nil {
				c.t.Fatalf("Query no-order window mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
			}
		} else if err := queryContractValidateNoOrderRecordsFull(got, full); err != nil {
			c.t.Fatalf("Query no-order full-set mismatch: %v\nq=%+v\nitems=%v", err, q, queryContractRecordSignatures(got))
		}
	}

	wantByKeys := c.batchGet("BatchGet(QueryKeys result)", keys)
	queryContractAssertRecordSlicesEqual(c.t, "Query vs QueryKeys", q, got, wantByKeys)

	c.assertCountMatchesReference(q, &ref)

	nq := normalizeQueryForTest(q)
	if err := c.db.engine.checkUsedQuery(nq); err != nil {
		c.t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	preparedKeys, err := execPreparedQueryForTest(c.db, nq)
	if err != nil {
		c.t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	c.assertKeysMatchReference("execPreparedQuery", q, preparedKeys, &ref)
	c.assertPreparedCountMatchesReference(q, &ref)

	return ref
}

func (c queryContract[K]) batchGet(label string, ids []K) []*Rec {
	c.t.Helper()
	items, err := c.db.BatchGet(ids...)
	if err != nil {
		c.t.Fatalf("%s: %v", label, err)
	}
	return items
}

func clearQueryOrderWindowForTest(q *qx.QX) {
	if q == nil {
		return
	}
	q.Order = nil
	q.Window.Offset = 0
	q.Window.Limit = 0
}

func execPreparedQueryForTest[K ~uint64 | ~string](db *DB[K, Rec], q *qx.QX) ([]K, error) {
	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()

	snap, seq, ref, pinned := db.engine.pinCurrentSnapshot()
	defer db.engine.unpinCurrentSnapshot(seq, ref, pinned)

	view := db.engine.makeQueryView(snap)
	defer db.engine.releaseQueryView(view)

	ids, err := view.execPreparedQuery(&viewQ)
	if err != nil {
		return nil, err
	}
	return db.queryKeysFromIDs(snap, ids), nil
}

func queryContractNoOrderWindow(q *qx.QX) bool {
	return q != nil && len(q.Order) == 0 && (q.Window.Offset > 0 || q.Window.Limit > 0)
}

func queryContractWindowMaxLen(q *qx.QX, fullLen int) int {
	maxLen := fullLen
	if q.Window.Offset >= uint64(fullLen) {
		maxLen = 0
	} else if q.Window.Offset > 0 {
		maxLen = fullLen - int(q.Window.Offset)
	}
	if q.Window.Limit > 0 && int(q.Window.Limit) < maxLen {
		maxLen = int(q.Window.Limit)
	}
	return maxLen
}

func queryContractValidateNoDuplicateKeys[K comparable](label string, keys []K) error {
	seen := make(map[K]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s duplicate key=%v result=%v", label, key, keys)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func queryContractValidateNoOrderWindow[K comparable](q *qx.QX, got, full []K) error {
	if err := queryContractValidateNoDuplicateKeys("no-order window", got); err != nil {
		return err
	}

	allow := make(map[K]struct{}, len(full))
	for _, key := range full {
		allow[key] = struct{}{}
	}
	for _, key := range got {
		if _, ok := allow[key]; !ok {
			return fmt.Errorf("key=%v outside full result set", key)
		}
	}

	maxLen := queryContractWindowMaxLen(q, len(full))
	if len(got) > maxLen {
		return fmt.Errorf("window overflow got=%d max=%d", len(got), maxLen)
	}
	return nil
}

func queryContractRecSignature(rec *Rec) string {
	if rec == nil {
		return "<nil>"
	}
	opt := "<nil>"
	if rec.Opt != nil {
		opt = *rec.Opt
	}
	return fmt.Sprintf(
		"%s|%s|%d|%g|%t|%s|%s|%s|%s",
		rec.Name,
		rec.Email,
		rec.Age,
		rec.Score,
		rec.Active,
		rec.Country,
		rec.FullName,
		opt,
		strings.Join(rec.Tags, "\x1f"),
	)
}

func queryContractBuildRecSignatureCounts(items []*Rec) map[string]int {
	out := make(map[string]int, len(items))
	for i := range items {
		if items[i] == nil {
			continue
		}
		out[queryContractRecSignature(items[i])]++
	}
	return out
}

func queryContractRecordSignatures(items []*Rec) []string {
	out := make([]string, len(items))
	for i := range items {
		out[i] = queryContractRecSignature(items[i])
	}
	return out
}

func queryContractValidateNoOrderRecordsWindow(q *qx.QX, items, full []*Rec) error {
	fullSigCounts := queryContractBuildRecSignatureCounts(full)
	seen := make(map[string]int, len(items))
	for i := range items {
		if items[i] == nil {
			return fmt.Errorf("nil item at i=%d", i)
		}
		sig := queryContractRecSignature(items[i])
		limit, ok := fullSigCounts[sig]
		if !ok {
			return fmt.Errorf("item %q outside full result set", sig)
		}
		seen[sig]++
		if seen[sig] > limit {
			return fmt.Errorf("duplicate item %q exceeds full-set multiplicity", sig)
		}
	}

	maxLen := queryContractWindowMaxLen(q, len(full))
	if len(items) > maxLen {
		return fmt.Errorf("items window overflow got=%d max=%d", len(items), maxLen)
	}
	return nil
}

func queryContractValidateNoOrderRecordsFull(items, full []*Rec) error {
	if len(items) != len(full) {
		return fmt.Errorf("full-set len mismatch got=%d want=%d", len(items), len(full))
	}
	gotCounts := queryContractBuildRecSignatureCounts(items)
	wantCounts := queryContractBuildRecSignatureCounts(full)
	if !reflect.DeepEqual(gotCounts, wantCounts) {
		return fmt.Errorf("full-set signature mismatch got=%v want=%v", gotCounts, wantCounts)
	}
	return nil
}

func queryContractAssertRecordSlicesEqual(t testing.TB, label string, q *qx.QX, got, want []*Rec) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s items len mismatch:\nq=%+v\ngot=%d\nwant=%d", label, q, len(got), len(want))
	}
	for i := range want {
		if got[i] == nil || want[i] == nil {
			t.Fatalf("%s nil item mismatch at i=%d:\nq=%+v\ngot=%#v\nwant=%#v", label, i, q, got[i], want[i])
		}
		if !reflect.DeepEqual(*got[i], *want[i]) {
			t.Fatalf("%s item mismatch at i=%d:\nq=%+v\ngot=%#v\nwant=%#v", label, i, q, got[i], want[i])
		}
	}
}
