package rbi

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

func TestAutoBatchMissingBucketRollsBackWriteTx(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(c.dataBucket)
	}); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	err := writeSet(c, 1, &Rec{Name: "missing"})
	if err == nil || !strings.Contains(err.Error(), "bucket does not exist") {
		t.Fatalf("expected missing bucket error, got %v", err)
	}

	done := make(chan error, 1)
	go func() {
		tx, err := c.root.bolt.Begin(true)
		if err == nil {
			err = tx.Rollback()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("begin write tx after missing bucket: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("write tx blocked after missing bucket error")
	}
}

func TestBatch_OnChange_CallbacksRunForSetPatchDelete(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t)

	var (
		mu     sync.Mutex
		events []string
	)
	cb := func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
		oldName := "<nil>"
		newName := "<nil>"
		if oldValue != nil {
			oldName = oldValue.Name
		}
		if newValue != nil {
			newName = newValue.Name
		}
		mu.Lock()
		events = append(events, fmt.Sprintf("%d:%s->%s", key, oldName, newName))
		mu.Unlock()
		return nil
	}

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}, OnChange(cb)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writePatch(c, 1, []Field{{Name: "name", Value: "bob"}}, OnChange(cb)); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if err := writeDelete(c, 1, OnChange(cb)); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	want := []string{
		"1:<nil>->alice",
		"1:alice->bob",
		"1:bob-><nil>",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("unexpected callback events: got=%v want=%v", events, want)
	}

	bs := c.StoreStats()
	if bs.CallbackOps < 3 {
		t.Fatalf("expected at least 3 callback ops in root stats, got %d", bs.CallbackOps)
	}
}

func TestAutoBatchMixedQueuedWritesMatchCommitOrderModel(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{
		AnalyzeInterval: -1,
		BatchSoftLimit:  16,
	})

	cloneRec := func(src *Rec) Rec {
		out := *src
		out.Tags = slices.Clone(src.Tags)
		if src.Opt != nil {
			opt := *src.Opt
			out.Opt = &opt
		}
		return out
	}
	makeRec := func(i int) Rec {
		countries := [...]string{"US", "DE", "NL", "PL"}
		names := [...]string{"alice", "bob", "carol", "dave"}
		tags := []string{"base", fmt.Sprintf("g-%d", i%5)}
		if i%3 == 0 {
			tags = append(tags, "hot")
		}
		return Rec{
			Meta:     Meta{Country: countries[i%len(countries)]},
			Name:     names[i%len(names)],
			Email:    fmt.Sprintf("mixed-%03d@example.test", i),
			Age:      20 + i%50,
			Score:    float64(i) + 0.25,
			Active:   i%2 == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("Mixed %03d", i),
		}
	}
	patchFor := func(rec Rec) []Field {
		return []Field{
			{Name: "country", Value: rec.Country},
			{Name: "name", Value: rec.Name},
			{Name: "email", Value: rec.Email},
			{Name: "age", Value: rec.Age},
			{Name: "score", Value: rec.Score},
			{Name: "active", Value: rec.Active},
			{Name: "tags", Value: slices.Clone(rec.Tags)},
			{Name: "full_name", Value: rec.FullName},
			{Name: "opt", Value: nil},
		}
	}
	poisonRec := func(rec *Rec) {
		rec.Country = "poison-country"
		rec.Name = "poison-name"
		rec.Email = "poison-email"
		rec.Age = -1
		rec.Score = -1
		rec.Active = false
		for i := range rec.Tags {
			rec.Tags[i] = "poison-tag"
		}
		rec.FullName = "poison-full-name"
	}
	poisonPatch := func(patch []Field) {
		for i := range patch {
			if tags, ok := patch[i].Value.([]string); ok {
				for j := range tags {
					tags[j] = "poison-patch-tag"
				}
			}
		}
	}

	model := make(map[uint64]Rec, 48)
	for i := 1; i <= 24; i++ {
		rec := makeRec(i)
		id := uint64(i)
		if err := writeSet(c, id, &rec); err != nil {
			t.Fatalf("seed Set(%d): %v", id, err)
		}
		model[id] = cloneRec(&rec)
	}

	type event struct {
		id     uint64
		delete bool
		rec    Rec
	}
	var (
		mu     sync.Mutex
		events []event
	)
	recordChange := func(_ *Tx, id uint64, _, newValue *Rec) error {
		mu.Lock()
		if newValue == nil {
			events = append(events, event{id: id, delete: true})
		} else {
			events = append(events, event{id: id, rec: cloneRec(newValue)})
		}
		mu.Unlock()
		return nil
	}

	type op struct {
		kind  int
		id    uint64
		rec   Rec
		patch []Field
	}
	ops := make([]op, 96)
	for i := range ops {
		id := uint64(1 + (i*7)%40)
		rec := makeRec(1000 + i)
		switch i % 6 {
		case 0, 3:
			ops[i] = op{kind: 0, id: id, rec: rec}
		case 1, 4:
			ops[i] = op{kind: 1, id: id, patch: patchFor(rec)}
		default:
			ops[i] = op{kind: 2, id: id}
		}
	}

	before := c.StoreStats()
	start := make(chan struct{})
	errCh := make(chan error, len(ops))
	var wg sync.WaitGroup
	for i := range ops {
		op := ops[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var err error
			switch op.kind {
			case 0:
				rec := op.rec
				err = writeSet(c, op.id, &rec, OnChange(recordChange))
				if err == nil {
					poisonRec(&rec)
				}
			case 1:
				err = writePatch(c, op.id, op.patch, OnChange(recordChange))
				if err == nil {
					poisonPatch(op.patch)
				}
			default:
				err = writeDelete(c, op.id, OnChange(recordChange))
			}
			if err != nil {
				errCh <- fmt.Errorf("kind=%d id=%d: %w", op.kind, op.id, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for i := range events {
		ev := events[i]
		if ev.delete {
			delete(model, ev.id)
		} else {
			model[ev.id] = ev.rec
		}
	}

	if got, err := readCount(c); err != nil {
		t.Fatalf("Count: %v", err)
	} else if got != uint64(len(model)) {
		t.Fatalf("Count = %d, want %d", got, len(model))
	}
	for id := uint64(1); id <= 40; id++ {
		got, err := readGet(c, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		want, ok := model[id]
		if !ok {
			if got != nil {
				t.Fatalf("Get(%d) = %#v, want nil", id, got)
			}
			continue
		}
		if got == nil || !reflect.DeepEqual(*got, want) {
			t.Fatalf("Get(%d) = %#v, want %#v", id, got, want)
		}
	}

	assertQuery := func(label string, q *qx.QX, match func(Rec) bool) {
		t.Helper()
		got, err := readQueryKeys(c, q)
		if err != nil {
			t.Fatalf("%s QueryKeys: %v", label, err)
		}
		want := make([]uint64, 0, len(model))
		for id, rec := range model {
			if match(rec) {
				want = append(want, id)
			}
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s QueryKeys = %v, want %v", label, got, want)
		}
	}
	assertQuery("active", qx.Query(qx.EQ("active", true)), func(rec Rec) bool {
		return rec.Active
	})
	assertQuery("country", qx.Query(qx.EQ("country", "DE")), func(rec Rec) bool {
		return rec.Country == "DE"
	})
	assertQuery("tags", qx.Query(qx.HASANY("tags", []string{"hot"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "hot")
	})

	after := c.StoreStats()
	if after.CallbackOps <= before.CallbackOps {
		t.Fatalf("expected queued writes to run callbacks, before=%+v after=%+v", before, after)
	}
}

func TestBatch_SequentialSet_DoesNotProduceMultiUnitBatches(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 16,
	})

	before := c.StoreStats()
	for i := 1; i <= 64; i++ {
		if err := writeSet(c, uint64(i), &Rec{
			Name: fmt.Sprintf("seq-%03d", i),
			Age:  18 + (i % 50),
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	after := waitAutoBatchExtraStats(t, c.root, "sequential batch accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+64 &&
			st.Enqueued == before.LogicalUnitsEnqueued+64 &&
			st.Dequeued == before.LogicalUnitsDequeued+64 &&
			st.ExecutedBatches == before.ExecutedBatches+64 &&
			st.BatchSize1 == before.BatchSize1+64
	})

	if after.Submitted != before.LogicalUnitsSubmitted+64 || after.Enqueued != before.LogicalUnitsEnqueued+64 || after.Dequeued != before.LogicalUnitsDequeued+64 {
		t.Fatalf("expected all sequential Set writes to be enqueued, before=%+v after=%+v", before, after)
	}
	if after.ExecutedBatches != before.ExecutedBatches+64 || after.BatchSize1 != before.BatchSize1+64 || after.MultiUnitBatches != before.MultiUnitBatches {
		t.Fatalf("expected no multi-request batches for sequential Set calls, before=%+v after=%+v", before, after)
	}
	if after.MaxBatchSeen > 1 {
		t.Fatalf("expected max seen batch size to stay 1 for sequential Set calls, stats=%+v", after)
	}
	if after.BatchSize1 == 0 {
		t.Fatalf("expected single-request batch distribution bucket to be tracked, stats=%+v", after)
	}
}

func TestBatch_RepeatedPatchIDMaintainsIndexConsistency(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{AnalyzeInterval: -1})

	if err := writeSet(c, 1, &Rec{
		Meta:     Meta{Country: "NL"},
		Name:     "alice",
		Email:    "alice@example.test",
		Age:      30,
		Score:    10.5,
		Active:   false,
		Tags:     []string{"go"},
		FullName: "ID-001",
	}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	if err := writePatches(c,
		[]uint64{1, 1},
		[]Field{
			{Name: "age", Value: 31},
			{Name: "tags", Value: []string{"rust", "db"}},
		},
	); err != nil {
		t.Fatalf("MultiPatch repeated id: %v", err)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil")
	}
	if got.Age != 31 {
		t.Fatalf("unexpected age: got=%d want=31", got.Age)
	}
	if !slices.Equal(got.Tags, []string{"rust", "db"}) {
		t.Fatalf("unexpected tags: got=%v want=%v", got.Tags, []string{"rust", "db"})
	}

	assertContains := func(q *qx.QX, desc string) {
		t.Helper()
		ids, qerr := readQueryKeys(c, q)
		if qerr != nil {
			t.Fatalf("QueryKeys(%s): %v", desc, qerr)
		}
		if !slices.Contains(ids, uint64(1)) {
			t.Fatalf("%s missing id=1, got=%v", desc, ids)
		}
	}
	assertOmits := func(q *qx.QX, desc string) {
		t.Helper()
		ids, qerr := readQueryKeys(c, q)
		if qerr != nil {
			t.Fatalf("QueryKeys(%s): %v", desc, qerr)
		}
		if slices.Contains(ids, uint64(1)) {
			t.Fatalf("stale %s still contains id=1, got=%v", desc, ids)
		}
	}

	assertContains(qx.Query(qx.EQ("age", 31)), "age=31")
	assertContains(qx.Query(qx.HASALL("tags", []string{"rust"})), `tag="rust"`)
	assertContains(qx.Query(qx.HASALL("tags", []string{"db"})), `tag="db"`)
	assertOmits(qx.Query(qx.EQ("age", 30)), "age=30")
	assertOmits(qx.Query(qx.HASALL("tags", []string{"go"})), `tag="go"`)
}

func TestBatch_MaxOne_StillUsesRootScheduler(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 1,
	})

	before := c.StoreStats()

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := waitAutoBatchExtraStats(t, c.root, "BatchSoftLimit=1 write settled", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+1 &&
			st.Enqueued == before.LogicalUnitsEnqueued+1 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.ExecutedBatches == before.ExecutedBatches+1 &&
			st.BatchSize1 == before.BatchSize1+1
	})
	if after.Submitted != before.LogicalUnitsSubmitted+1 || after.Enqueued != before.LogicalUnitsEnqueued+1 || after.Dequeued != before.LogicalUnitsDequeued+1 {
		t.Fatalf("expected BatchSoftLimit=1 write to still use queue path, before=%+v after=%+v", before, after)
	}
	if after.ExecutedBatches != before.ExecutedBatches+1 || after.BatchSize1 != before.BatchSize1+1 || after.MultiUnitBatches != before.MultiUnitBatches {
		t.Fatalf("expected BatchSoftLimit=1 to execute as a single-unit physical batch, before=%+v after=%+v", before, after)
	}
}

func TestBatch_CollectionBatchSoftLimitLimitsRootSharedBatch(t *testing.T) {
	enableStoreStatsForTest(t)

	raw, _ := openRawBolt(t)
	slowDB, err := Open[uint64, Rec](raw, testOptions(Options{BatchSoftLimit: 1}))
	if err != nil {
		t.Fatalf("New slow: %v", err)
	}
	fastDB, err := Open[string, Product](raw, testOptions(Options{BatchSoftLimit: 16}))
	if err != nil {
		t.Fatalf("New fast: %v", err)
	}
	t.Cleanup(func() {
		_ = slowDB.Close()
		_ = fastDB.Close()
		_ = raw.Close()
	})

	before := slowDB.StoreStats()
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(fastDB, "blocker", &Product{SKU: "blocker"}, OnChange(func(*Tx, string, *Product, *Product) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() {
		done1 <- writeSet(slowDB, 1, &Rec{Name: "one"})
	}()
	go func() {
		done2 <- writeSet(slowDB, 2, &Rec{Name: "two"})
	}()

	waitAutoBatchExtraStats(t, slowDB.root, "slow writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)
	for name, ch := range map[string]<-chan error{
		"blocker": blockerDone,
		"one":     done1,
		"two":     done2,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s write: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %s write", name)
		}
	}

	after := waitAutoBatchExtraStats(t, slowDB.root, "BatchSoftLimit=1 collection batches settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+3 &&
			st.ExecutedBatches == before.ExecutedBatches+3 &&
			st.BatchSize1 == before.BatchSize1+3
	})
	if after.MultiUnitBatches != before.MultiUnitBatches {
		t.Fatalf("BatchSoftLimit=1 collection formed shared batch, before=%+v after=%+v", before, after)
	}
	if after.ExecutedBatches != before.ExecutedBatches+3 || after.BatchSize1 != before.BatchSize1+3 {
		t.Fatalf("expected blocker and slow writes to execute as single-unit batches, before=%+v after=%+v", before, after)
	}
}

func TestAutoBatchRequestLocalApplyFailureDoesNotRejectNeighbor(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempStringCollection(t, Options{BatchSoftLimit: 16})
	before := c.StoreStats()

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(c, "blocker", &Rec{Name: "blocker"}, OnChange(func(*Tx, string, *Rec, *Rec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	badDone := make(chan error, 1)
	goodDone := make(chan error, 1)
	go func() {
		badDone <- writeSet(c, strings.Repeat("x", bbolt.MaxKeySize+1), &Rec{Name: "bad"})
	}()
	go func() {
		goodDone <- writeSet(c, "good", &Rec{Name: "good"})
	}()

	waitAutoBatchExtraStats(t, c.root, "bad+good writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)

	if err := <-blockerDone; err != nil {
		t.Fatalf("blocker write: %v", err)
	}
	if err := <-badDone; !errors.Is(err, berrors.ErrKeyTooLarge) {
		t.Fatalf("bad write err=%v, want ErrKeyTooLarge", err)
	}
	if err := <-goodDone; err != nil {
		t.Fatalf("good write: %v", err)
	}

	got, err := readGet(c, "good")
	if err != nil {
		t.Fatalf("Get(good): %v", err)
	}
	defer releaseUniqueRecords(c, got)
	if got == nil || got.Name != "good" {
		t.Fatalf("Get(good)=%#v want good record", got)
	}

	after := waitAutoBatchExtraStats(t, c.root, "bad+good coalesced batch settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+3 &&
			st.ExecutedBatches == before.ExecutedBatches+2 &&
			st.MultiUnitBatches == before.MultiUnitBatches+1 &&
			st.BatchSize1 == before.BatchSize1+1 &&
			st.BatchSize2To4 == before.BatchSize2To4+1
	})
	if after.ExecutedBatches != before.ExecutedBatches+2 || after.MultiUnitBatches != before.MultiUnitBatches+1 || after.BatchSize1 != before.BatchSize1+1 || after.BatchSize2To4 != before.BatchSize2To4+1 {
		t.Fatalf("request-local failure did not run in one multi-unit batch, before=%+v after=%+v", before, after)
	}
}

func TestBatch_CrossDBWritesSharePhysicalRootBatch(t *testing.T) {
	enableStoreStatsForTest(t)

	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{BatchSoftLimit: 16}))
	if err != nil {
		t.Fatalf("New rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{BatchSoftLimit: 16}))
	if err != nil {
		t.Fatalf("New product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	before := recDB.StoreStats()
	epochBefore := recDB.root.registry.current.Load().epoch

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(recDB, 1, &Rec{Name: "blocker"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	recDone := make(chan error, 1)
	productDone := make(chan error, 1)
	go func() {
		recDone <- writeSet(recDB, 2, &Rec{Name: "queued-rec", Age: 22})
	}()
	go func() {
		productDone <- writeSet(productDB, "queued-product", &Product{SKU: "queued-product", Price: 42})
	}()

	waitAutoBatchExtraStats(t, recDB.root, "cross-db writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)
	for name, ch := range map[string]<-chan error{
		"blocker": blockerDone,
		"rec":     recDone,
		"product": productDone,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s write: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %s write", name)
		}
	}

	after := waitAutoBatchExtraStats(t, recDB.root, "cross-DB batch accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+3 &&
			st.ExecutedBatches == before.ExecutedBatches+2 &&
			st.MultiUnitBatches == before.MultiUnitBatches+1 &&
			st.BatchSize1 == before.BatchSize1+1 &&
			st.BatchSize2To4 == before.BatchSize2To4+1 &&
			st.MaxBatchSeen >= 2
	})
	if after.ExecutedBatches != before.ExecutedBatches+2 || after.MultiUnitBatches != before.MultiUnitBatches+1 || after.BatchSize1 != before.BatchSize1+1 || after.BatchSize2To4 != before.BatchSize2To4+1 {
		t.Fatalf("cross-DB writes did not form one root physical batch, before=%+v after=%+v", before, after)
	}
	if after.MaxBatchSeen < 2 {
		t.Fatalf("root physical batch size was not observed, stats=%+v", after)
	}
	if got := recDB.root.registry.current.Load().epoch; got != epochBefore+2 {
		t.Fatalf("root epoch after blocker+crossDB batch=%d want %d", got, epochBefore+2)
	}

	rec, err := readGet(recDB, 2)
	if err != nil {
		t.Fatalf("Get rec: %v", err)
	}
	product, err := readGet(productDB, "queued-product")
	if err != nil {
		t.Fatalf("Get product: %v", err)
	}
	if rec == nil || rec.Name != "queued-rec" || rec.Age != 22 {
		t.Fatalf("unexpected rec after crossDB batch: %#v", rec)
	}
	if product == nil || product.SKU != "queued-product" || product.Price != 42 {
		t.Fatalf("unexpected product after crossDB batch: %#v", product)
	}
}

func TestBatch_OnChangeFailureDoesNotReplayAcceptedNeighbor(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 16})
	before := c.StoreStats()
	epochBefore := c.root.registry.current.Load().epoch

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(c, 1, &Rec{Name: "blocker"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	failErr := errors.New("fail current unit")
	var acceptedCalls atomic.Int32
	acceptedDone := make(chan error, 1)
	failedDone := make(chan error, 1)
	go func() {
		acceptedDone <- writeSet(c, 2, &Rec{Name: "accepted"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			acceptedCalls.Add(1)
			return nil
		}))
	}()
	waitAutoBatchExtraStats(t, c.root, "accepted hook write queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+2 &&
			st.Enqueued == before.LogicalUnitsEnqueued+2 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 1
	})
	go func() {
		failedDone <- writeSet(c, 3, &Rec{Name: "failed"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			return failErr
		}))
	}()

	waitAutoBatchExtraStats(t, c.root, "hook writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)
	for name, ch := range map[string]<-chan error{
		"blocker":  blockerDone,
		"accepted": acceptedDone,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s write: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %s write", name)
		}
	}
	select {
	case err := <-failedDone:
		if !errors.Is(err, failErr) {
			t.Fatalf("failed write error=%v want %v", err, failErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for failed write")
	}

	if got := acceptedCalls.Load(); got != 1 {
		t.Fatalf("accepted OnChange calls=%d want 1", got)
	}
	if got := c.root.registry.current.Load().epoch; got != epochBefore+2 {
		t.Fatalf("root epoch after blocker+accepted batch=%d want %d", got, epochBefore+2)
	}
	after := waitAutoBatchExtraStats(t, c.root, "hook batch accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+3 &&
			st.ExecutedBatches == before.ExecutedBatches+2 &&
			st.MultiUnitBatches == before.MultiUnitBatches+1 &&
			st.BatchSize1 == before.BatchSize1+1 &&
			st.BatchSize2To4 == before.BatchSize2To4+1
	})
	if after.ExecutedBatches != before.ExecutedBatches+2 || after.MultiUnitBatches != before.MultiUnitBatches+1 || after.BatchSize1 != before.BatchSize1+1 || after.BatchSize2To4 != before.BatchSize2To4+1 {
		t.Fatalf("hook writes did not execute as one root batch, before=%+v after=%+v", before, after)
	}

	accepted, err := readGet(c, 2)
	if err != nil {
		t.Fatalf("Get accepted: %v", err)
	}
	failed, err := readGet(c, 3)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if accepted == nil || accepted.Name != "accepted" {
		t.Fatalf("accepted unit was not committed: %#v", accepted)
	}
	if failed != nil {
		t.Fatalf("failed unit persisted: %#v", failed)
	}
}

func TestBatch_AcceptedUnitsSeeSteppedOldValuesSameCollection(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 16})
	before := c.StoreStats()

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(c, 1, &Rec{Name: "blocker"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	var firstOld, secondOld int
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- writeSet(c, 2, &Rec{Name: "first", Age: 10}, OnChange(func(_ *Tx, _ uint64, oldValue, _ *Rec) error {
			if oldValue != nil {
				firstOld = oldValue.Age
			}
			return nil
		}))
	}()
	waitAutoBatchExtraStats(t, c.root, "first same-key write queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+2 &&
			st.Enqueued == before.LogicalUnitsEnqueued+2 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 1
	})
	go func() {
		secondDone <- writeSet(c, 2, &Rec{Name: "second", Age: 20}, OnChange(func(_ *Tx, _ uint64, oldValue, _ *Rec) error {
			if oldValue != nil {
				secondOld = oldValue.Age
			}
			return nil
		}))
	}()

	waitAutoBatchExtraStats(t, c.root, "same-key writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)
	for name, ch := range map[string]<-chan error{
		"blocker": blockerDone,
		"first":   firstDone,
		"second":  secondDone,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s write: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %s write", name)
		}
	}

	if firstOld != 0 {
		t.Fatalf("first old age=%d want 0", firstOld)
	}
	if secondOld != 10 {
		t.Fatalf("second old age=%d want 10", secondOld)
	}
	got, err := readGet(c, 2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got == nil || got.Name != "second" || got.Age != 20 {
		t.Fatalf("final value=%#v want second age 20", got)
	}
}

func TestBatch_UniqueConflictBetweenUnitsKeepsEarlierUnit(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64CollectionUnique(t, Options{BatchSoftLimit: 16})
	before := c.StoreStats()

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(c, 1, &UniqueTestRec{Email: "blocker@x", Code: 1}, OnChange(func(*Tx, uint64, *UniqueTestRec, *UniqueTestRec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- writeSet(c, 2, &UniqueTestRec{Email: "shared@x", Code: 2})
	}()
	waitAutoBatchExtraStats(t, c.root, "first unique write queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+2 &&
			st.Enqueued == before.LogicalUnitsEnqueued+2 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 1
	})
	go func() {
		secondDone <- writeSet(c, 3, &UniqueTestRec{Email: "shared@x", Code: 3})
	}()

	waitAutoBatchExtraStats(t, c.root, "unique writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)
	for name, ch := range map[string]<-chan error{
		"blocker": blockerDone,
		"first":   firstDone,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s write: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %s write", name)
		}
	}
	select {
	case err := <-secondDone:
		if !errors.Is(err, rbierrors.ErrUniqueViolation) {
			t.Fatalf("second write error=%v want %v", err, rbierrors.ErrUniqueViolation)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second write")
	}

	first, err := readGet(c, 2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	second, err := readGet(c, 3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if first == nil || first.Email != "shared@x" {
		t.Fatalf("first unique unit was not committed: %#v", first)
	}
	if second != nil {
		t.Fatalf("conflicting unique unit persisted: %#v", second)
	}
	after := waitAutoBatchExtraStats(t, c.root, "unique conflict batch accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+3 &&
			st.ExecutedBatches == before.ExecutedBatches+2 &&
			st.MultiUnitBatches == before.MultiUnitBatches+1 &&
			st.BatchSize1 == before.BatchSize1+1 &&
			st.BatchSize2To4 == before.BatchSize2To4+1
	})
	if after.ExecutedBatches != before.ExecutedBatches+2 || after.MultiUnitBatches != before.MultiUnitBatches+1 || after.BatchSize1 != before.BatchSize1+1 || after.BatchSize2To4 != before.BatchSize2To4+1 {
		t.Fatalf("unique writes did not execute as one root batch, before=%+v after=%+v", before, after)
	}
}

func TestBatch_UniqueAcceptedDepartureAllowsLaterUnit(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64CollectionUnique(t, Options{BatchSoftLimit: 16})
	if err := writeSet(c, 1, &UniqueTestRec{Email: "reuse@x", Code: 1}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	if err := writeSet(c, 2, &UniqueTestRec{Email: "holder@x", Code: 2}); err != nil {
		t.Fatalf("seed Set(2): %v", err)
	}
	waitAutoBatchExtraStats(t, c.root, "unique transfer seed accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == 2 &&
			st.Enqueued == 2 &&
			st.Dequeued == 2 &&
			st.ExecutedBatches == 2 &&
			st.BatchSize1 == 2
	})
	before := c.StoreStats()

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(c, 10, &UniqueTestRec{Email: "blocker@x", Code: 10}, OnChange(func(*Tx, uint64, *UniqueTestRec, *UniqueTestRec) error {
			close(entered)
			<-release
			return nil
		}))
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	departDone := make(chan error, 1)
	reuseDone := make(chan error, 1)
	go func() {
		departDone <- writePatch(c, 1, []Field{{Name: "email", Value: "left@x"}}, PatchStrict)
	}()
	waitAutoBatchExtraStats(t, c.root, "unique departure queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+2 &&
			st.Enqueued == before.LogicalUnitsEnqueued+2 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 1
	})
	go func() {
		reuseDone <- writePatch(c, 2, []Field{{Name: "email", Value: "reuse@x"}}, PatchStrict)
	}()
	waitAutoBatchExtraStats(t, c.root, "unique reuse queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+3 &&
			st.Enqueued == before.LogicalUnitsEnqueued+3 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 2
	})

	close(release)
	for name, ch := range map[string]<-chan error{
		"blocker":   blockerDone,
		"departure": departDone,
		"reuse":     reuseDone,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s write: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %s write", name)
		}
	}

	one, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	two, err := readGet(c, 2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if one == nil || one.Email != "left@x" {
		t.Fatalf("departure unit did not commit: %#v", one)
	}
	if two == nil || two.Email != "reuse@x" {
		t.Fatalf("reuse unit did not see accepted departure: %#v", two)
	}
	after := waitAutoBatchExtraStats(t, c.root, "unique transfer batch accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+3 &&
			st.ExecutedBatches == before.ExecutedBatches+2 &&
			st.MultiUnitBatches == before.MultiUnitBatches+1 &&
			st.BatchSize1 == before.BatchSize1+1 &&
			st.BatchSize2To4 == before.BatchSize2To4+1
	})
	if after.ExecutedBatches != before.ExecutedBatches+2 || after.MultiUnitBatches != before.MultiUnitBatches+1 || after.BatchSize1 != before.BatchSize1+1 || after.BatchSize2To4 != before.BatchSize2To4+1 {
		t.Fatalf("unique transfer did not execute as one root batch, before=%+v after=%+v", before, after)
	}
}

func TestBatch_PatchUnique_QueuedIntoBatch(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64CollectionUnique(t, Options{
		BatchSoftLimit: 16,
	})

	if err := writeSet(c, 1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	before := c.StoreStats()
	if err := writePatch(c, 1, []Field{{Name: "email", Value: "c@x"}}); err != nil {
		t.Fatalf("Patch unique field should use root scheduler path: %v", err)
	}
	mid := c.StoreStats()
	if mid.LogicalUnitsEnqueued <= before.LogicalUnitsEnqueued {
		t.Fatalf("expected patch to be enqueued into root scheduler, before=%+v after=%+v", before, mid)
	}

	err := writePatch(c, 1, []Field{{Name: "email", Value: "b@x"}})
	if err == nil || !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("expected unique violation for conflicting email patch, got: %v", err)
	}

	v1, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "c@x" {
		t.Fatalf("id=1 must keep last successful value, got: %#v", v1)
	}
}

func TestBatch_DuplicatePatchSameID_DecodeFailurePropagatesToLaterRequests(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 16,
	})

	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(uint64(1), c.strKey, &keyBuf), []byte{0xc1})
	}); err != nil {
		t.Fatalf("seed invalid payload: %v", err)
	}

	err := writePatches(c, []uint64{1, 1}, []Field{{Name: "age", Value: 31}})
	if err == nil || !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("MultiPatch error = %v, want decode existing value", err)
	}
}

func TestBatch_OnChangeError_RollsBack(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	wantErr := errors.New("on change failed")
	err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}, OnChange(func(_ *Tx, _ uint64, _, _ *Rec) error {
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected OnChange error %v, got %v", wantErr, err)
	}

	got, gerr := readGet(c, 1)
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if got != nil {
		t.Fatalf("expected rollback on OnChange error, got %#v", got)
	}
}

func TestBatch_StringKeyOnChangeError_DoesNotPersistStringKey(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t)

	if err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	hookErr := errors.New("on change fail")
	err := writeSet(c,
		"ghost-on-change",
		&Product{SKU: "ghost-on-change", Price: 11},
		OnChange(func(_ *Tx, _ string, _ *Product, _ *Product) error { return hookErr }),
	)
	if !errors.Is(err, hookErr) {
		t.Fatalf("Set must fail with OnChange error, got: %v", err)
	}
	if got, err := readGet(c, "ghost-on-change"); err != nil {
		t.Fatalf("Get(ghost-on-change): %v", err)
	} else if got != nil {
		t.Fatalf("ghost-on-change must not persist after OnChange failure, got %#v", got)
	}
}

func keysOfMap[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestAutoBatchExt_BatchAtomic_DuplicatePatchSameID_OnChangeSeesSteppedState(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	if err := writeSet(c, 1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	call := 0
	var seen []string
	err := writePatches(c,
		[]uint64{1, 1},
		[]Field{{Name: "age", Value: 99}},
		OnChange(func(_ *Tx, _ uint64, oldValue, newValue *Rec) error {
			call++
			newValue.Name = fmt.Sprintf("step-%d", call)
			seen = append(seen, fmt.Sprintf("%s->%s", oldValue.Name, newValue.Name))
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiPatch: %v", err)
	}

	want := []string{"seed->step-1", "step-1->step-2"}
	if !slices.Equal(seen, want) {
		t.Fatalf("OnChange sequence = %v, want %v", seen, want)
	}
	if got, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "step-2" || got.Age != 99 {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
}

func TestAutoBatchExt_BatchAtomic_DuplicateDeleteSameID_OnChangeRunsOnce(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	if err := writeSet(c, 1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	calls := 0
	err := writeDeletes(c,
		[]uint64{1, 1},
		OnChange(func(_ *Tx, _ uint64, _ *Rec, _ *Rec) error {
			calls++
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiDelete: %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnChange calls = %d, want 1", calls)
	}
	if got, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must be deleted, got %#v", got)
	}
}

func TestAutoBatchExt_BatchAtomic_PatchStrictDuplicateSameID_RollsBackBothSteps(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	if err := writeSet(c, 1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	err := writePatches(c, []uint64{1, 1}, []Field{{Name: "missing", Value: 1}}, PatchStrict)
	if err == nil || !strings.Contains(err.Error(), "cannot patch field") {
		t.Fatalf("MultiPatch error = %v, want strict patch error", err)
	}

	if got, gerr := readGet(c, 1); gerr != nil {
		t.Fatalf("Get(1): %v", gerr)
	} else if got == nil || got.Name != "seed" || got.Age != 10 {
		t.Fatalf("id=1 changed after failed MultiPatch: %#v", got)
	}
}

func TestAutoBatchExt_Race_HotSameID_AutoBatchQueryConsistency(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{
		AnalyzeInterval: -1,
		BatchSoftLimit:  16,
	})

	for i := 1; i <= 4; i++ {
		if err := writeSet(c, uint64(i), &Rec{
			Name:   fmt.Sprintf("seed-%d", i),
			Age:    20 + i,
			Active: i%2 == 0,
			Tags:   []string{"base"},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(4))
				switch r.IntN(3) {
				case 0:
					if err := writeSet(c, id, &Rec{
						Name:   []string{"alice", "bob", "carol"}[r.IntN(3)],
						Age:    18 + r.IntN(50),
						Active: r.IntN(2) == 0,
						Tags:   []string{"go", "db", "ops"}[:1+r.IntN(3)],
					}); err != nil {
						reportErr(fmt.Errorf("Set(%d): %w", id, err))
						return
					}
				case 1:
					patch := []Field{{Name: "age", Value: float64(20 + r.IntN(40))}}
					if err := writePatch(c, id, patch); err != nil {
						reportErr(fmt.Errorf("Patch(%d): %w", id, err))
						return
					}
				default:
					if err := writeDelete(c, id); err != nil {
						reportErr(fmt.Errorf("Delete(%d): %w", id, err))
						return
					}
				}
			}
		}(int64(1000 + w))
	}

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 30)),
		qx.Query(qx.PREFIX("name", "a")),
		qx.Query(qx.HASANY("tags", []string{"go", "db"})),
		qx.Query(qx.EQ("active", true)),
	}
	for rr := 0; rr < 4; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				q := queries[r.IntN(len(queries))]
				items, err := readQuery(c, q)
				if err != nil {
					reportErr(fmt.Errorf("Query: %w", err))
					return
				}
				for _, item := range items {
					if item == nil {
						continue
					}
					ok, evalErr := evalExprBool(item, q.Filter)
					if evalErr != nil {
						reportErr(fmt.Errorf("eval: %w", evalErr))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("query returned inconsistent item %#v for %v", item, q.Filter))
						return
					}
				}
			}
		}(int64(2000 + rr))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}
}

func TestAutoBatchExt_Race_HotUniqueContention_NoInvariantBreak(t *testing.T) {
	c, _ := openTempUint64CollectionUnique(t, Options{
		BatchSoftLimit: 16,
	})

	for i := 1; i <= 8; i++ {
		if err := writeSet(c, uint64(i), &UniqueTestRec{
			Email: fmt.Sprintf("seed-%d@x", i),
			Code:  i,
			Tags:  []string{"seed"},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(8))
				switch r.IntN(3) {
				case 0:
					err := writeSet(c, id, &UniqueTestRec{
						Email: fmt.Sprintf("u%d@x", r.IntN(6)),
						Code:  1 + r.IntN(6),
						Tags:  []string{fmt.Sprintf("w%d", r.IntN(3))},
					})
					if err != nil && !errors.Is(err, rbierrors.ErrUniqueViolation) {
						reportErr(fmt.Errorf("Set(%d): %w", id, err))
						return
					}
				case 1:
					var patch []Field
					if r.IntN(2) == 0 {
						patch = []Field{{Name: "email", Value: fmt.Sprintf("u%d@x", r.IntN(6))}}
					} else {
						patch = []Field{{Name: "tags", Value: []string{fmt.Sprintf("p%d", r.IntN(4))}}}
					}
					err := writePatch(c, id, patch)
					if err != nil && !errors.Is(err, rbierrors.ErrUniqueViolation) {
						reportErr(fmt.Errorf("Patch(%d): %w", id, err))
						return
					}
				default:
					if err := writeDelete(c, id); err != nil && !errors.Is(err, rbierrors.ErrUniqueViolation) {
						reportErr(fmt.Errorf("Delete(%d): %w", id, err))
						return
					}
				}
			}
		}(int64(3000 + w))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}

	seenEmail := make(map[string]uint64)
	seenCode := make(map[int]uint64)
	for id := uint64(1); id <= 8; id++ {
		got, err := readGet(c, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got == nil {
			continue
		}
		if prev, exists := seenEmail[got.Email]; exists {
			t.Fatalf("duplicate email %q for ids %d and %d", got.Email, prev, id)
		}
		seenEmail[got.Email] = id
		if prev, exists := seenCode[got.Code]; exists {
			t.Fatalf("duplicate code %d for ids %d and %d", got.Code, prev, id)
		}
		seenCode[got.Code] = id
	}

	for email, id := range seenEmail {
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("email", email)))
		if err != nil {
			t.Fatalf("QueryKeys(email=%s): %v", email, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("email=%s ids = %v, want [%d]", email, ids, id)
		}
	}
	for code, id := range seenCode {
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("code", code)))
		if err != nil {
			t.Fatalf("QueryKeys(code=%d): %v", code, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("code=%d ids = %v, want [%d]", code, ids, id)
		}
	}
}

func TestAutoBatchExt_New_Race_HotPatchHooks_QueryConsistency(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{
		AnalyzeInterval: -1,
		BatchSoftLimit:  16,
	})

	for i := 1; i <= 2; i++ {
		if err := writeSet(c, uint64(i), &Rec{
			Name:     fmt.Sprintf("seed-%d", i),
			Age:      20 + i,
			Tags:     []string{"seed"},
			FullName: fmt.Sprintf("seed-full-%d", i),
			Meta:     Meta{Country: "NL"},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	countries := []string{"NL", "DE", "PL", "ES"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(2))
				name := fmt.Sprintf("writer-%d", r.IntN(4))
				fullName := fmt.Sprintf("full-%d", r.IntN(4))
				country := countries[r.IntN(len(countries))]

				err := writePatch(c,
					id,
					[]Field{{Name: "age", Value: float64(20 + r.IntN(50))}},
					OnChange(func(_ *Tx, _ uint64, _ *Rec, v *Rec) error {
						v.Name = name
						v.FullName = fullName
						v.Country = country
						return nil
					}),
				)
				if err != nil {
					reportErr(fmt.Errorf("Patch(%d): %w", id, err))
					return
				}
			}
		}(int64(4100 + w))
	}

	queries := []*qx.QX{
		qx.Query(qx.PREFIX("name", "writer-")),
		qx.Query(qx.PREFIX("full_name", "full-")),
		qx.Query(qx.GTE("age", 20)),
		qx.Query(qx.EQ("country", "NL")),
		qx.Query(qx.EQ("country", "DE")),
		qx.Query(qx.EQ("country", "PL")),
		qx.Query(qx.EQ("country", "ES")),
	}

	for rr := 0; rr < 4; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				q := queries[r.IntN(len(queries))]
				items, err := readQuery(c, q)
				if err != nil {
					reportErr(fmt.Errorf("Query: %w", err))
					return
				}
				for _, item := range items {
					if item == nil {
						continue
					}
					ok, evalErr := evalExprBool(item, q.Filter)
					if evalErr != nil {
						reportErr(fmt.Errorf("eval: %w", evalErr))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("query returned inconsistent item %#v for %v", item, q.Filter))
						return
					}
				}
			}
		}(int64(5100 + rr))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}
}

/**/

func waitAutoBatchExtraStats(
	tb testing.TB,
	r *rootStore,
	desc string,
	ok func(rootSchedulerSnapshot) bool,
) rootSchedulerSnapshot {
	tb.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats := r.scheduler.snapshot()
		if ok(stats) {
			return stats
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timeout waiting for autobatch stats: %s last=%+v", desc, stats)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func readAutoBatchExtraRawValue[K ~string | ~uint64, V any](tb testing.TB, c *Collection[K, V], id K) *V {
	tb.Helper()

	var got *V
	err := c.root.bolt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		raw := bucket.Get(keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf))
		if raw == nil {
			return nil
		}
		payload, err := rawPayloadForTest(c, raw)
		if err != nil {
			return err
		}
		value, err := c.decode(payload)
		if err != nil {
			return err
		}
		got = value
		return nil
	})
	if err != nil {
		tb.Fatalf("raw read %v: %v", id, err)
	}
	return got
}

func TestAutoBatchExtra_ClosedSetRejectsBeforeRootScheduler(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 16,
	})

	before := c.root.scheduler.snapshot()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := writeSet(c, 1, &Rec{Name: "closed", Age: 10})
	if !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want rbierrors.ErrClosed", err)
	}

	after := c.root.scheduler.snapshot()
	if after.Submitted != before.Submitted || after.RejectedClosed != before.RejectedClosed {
		t.Fatalf("closed write must be rejected before root scheduler, before=%+v after=%+v", before, after)
	}
	if after.Enqueued != before.Enqueued || after.Dequeued != before.Dequeued {
		t.Fatalf("closed write must not enqueue/dequeue, before=%+v after=%+v", before, after)
	}
}

func TestAutoBatchExtra_CloseWaitsForQueuedGroupedJobAfterInFlightCommit(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 1,
	})

	before := c.root.scheduler.snapshot()

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := OnChange(func(*Tx, uint64, *Rec, *Rec) error {
		close(entered)
		<-release
		return nil
	})

	inFlightDone := make(chan error, 1)
	go func() {
		inFlightDone <- writeSet(c, 1, &Rec{Name: "persisted", Age: 11}, blockCommit)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for in-flight commit hook")
	}

	queuedDone := make(chan error, 1)
	go func() {
		queuedDone <- writeSets(c,
			[]uint64{2, 3},
			[]*Rec{
				{Name: "queued-2", Age: 22},
				{Name: "queued-3", Age: 33},
			},
		)
	}()

	waitAutoBatchExtraStats(t, c.root, "in-flight+queued state", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.Submitted+2 &&
			st.Enqueued == before.Enqueued+2 &&
			st.Dequeued == before.Dequeued+1 &&
			st.QueueLen == 1
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()

	releaseCommit := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}

	releaseCommit()

	select {
	case err := <-inFlightDone:
		if err != nil {
			t.Fatalf("in-flight writer error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for in-flight writer")
	}

	select {
	case err := <-queuedDone:
		if err != nil {
			t.Fatalf("queued grouped job error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for queued grouped job")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Close")
	}

	after := waitAutoBatchExtraStats(t, c.root, "mixed close outcomes settled", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.Submitted+2 &&
			st.Enqueued == before.Enqueued+2 &&
			st.Dequeued == before.Dequeued+2 &&
			st.ExecutedBatches == before.ExecutedBatches+2 &&
			st.RejectedClosed == before.RejectedClosed &&
			st.QueueLen == 0 &&
			!st.WorkerRunning
	})
	if after.MultiUnitBatches != before.MultiUnitBatches {
		t.Fatalf("queued grouped job is isolated and must not count as executed multi-request batch, before=%+v after=%+v", before, after)
	}

	if got := readAutoBatchExtraRawValue(t, c, uint64(1)); got == nil || got.Name != "persisted" || got.Age != 11 {
		t.Fatalf("id=1 must persist from in-flight writer, got=%#v", got)
	}
	for _, id := range []uint64{2, 3} {
		if got := readAutoBatchExtraRawValue(t, c, id); got == nil || got.Name != fmt.Sprintf("queued-%d", id) || got.Age != int(id*11) {
			t.Fatalf("id=%d must persist from retained queued writer, got=%#v", id, got)
		}
	}
}

func TestAutoBatchExtra_Race_UniqueHookMutations_NoInvariantBreak(t *testing.T) {
	c, _ := openTempUint64CollectionUnique(t, Options{
		AnalyzeInterval: -1,
		BatchSoftLimit:  16,
	})

	emails := []string{
		"seed-1@x",
		"seed-2@x",
		"seed-3@x",
		"seed-4@x",
		"u0@x",
		"u1@x",
		"u2@x",
		"u3@x",
		"h0@x",
		"h1@x",
	}
	tags := []string{"tg-a", "tg-b", "tg-c", "tg-d", "bs-a", "bs-b", "bs-c"}
	const codeMax = 8

	for i := 1; i <= 4; i++ {
		if err := writeSet(c, uint64(i), &UniqueTestRec{
			Email: fmt.Sprintf("seed-%d@x", i),
			Code:  i,
			Tags:  []string{tags[i-1]},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(4))
				var err error

				switch r.IntN(3) {
				case 0:
					rec := &UniqueTestRec{
						Email: emails[r.IntN(len(emails))],
						Code:  1 + r.IntN(codeMax),
						Tags:  []string{tags[r.IntN(4)]},
					}
					if r.IntN(2) == 0 {
						email := emails[r.IntN(len(emails))]
						code := 1 + r.IntN(codeMax)
						tag := tags[r.IntN(len(tags))]
						err = writeSet(c, id, rec, OnChange(func(_ *Tx, _ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
							v.Email = email
							v.Code = code
							v.Tags = append(v.Tags, tag)
							return nil
						}))
					} else {
						err = writeSet(c, id, rec)
					}
				case 1:
					var patch []Field
					if r.IntN(2) == 0 {
						patch = []Field{{Name: "tags", Value: []string{tags[r.IntN(len(tags))]}}}
					} else {
						patch = []Field{{Name: "code", Value: float64(1 + r.IntN(codeMax))}}
					}

					var opts []ExecOption[uint64, UniqueTestRec]
					if r.IntN(2) == 0 {
						email := emails[r.IntN(len(emails))]
						code := 1 + r.IntN(codeMax)
						opts = append(opts, OnChange(func(_ *Tx, _ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
							v.Email = email
							v.Code = code
							return nil
						}))
					}
					if r.IntN(2) == 0 {
						tag := tags[r.IntN(len(tags))]
						opts = append(opts, OnChange(func(_ *Tx, _ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
							v.Tags = append(v.Tags, tag)
							return nil
						}))
					}
					err = writePatch(c, id, patch, opts...)
				default:
					err = writeDelete(c, id)
				}

				if err != nil && !errors.Is(err, rbierrors.ErrUniqueViolation) {
					reportErr(fmt.Errorf("writer id=%d: %w", id, err))
					return
				}
			}
		}(int64(6200 + w))
	}

	for rr := 0; rr < 4; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				queryKind := r.IntN(3)
				queryEmail := ""
				queryCode := 0
				queryTag := ""

				var q *qx.QX
				switch queryKind {
				case 0:
					queryEmail = emails[r.IntN(len(emails))]
					q = qx.Query(qx.EQ("email", queryEmail))
				case 1:
					queryCode = 1 + r.IntN(codeMax)
					q = qx.Query(qx.EQ("code", queryCode))
				default:
					queryTag = tags[r.IntN(len(tags))]
					q = qx.Query(qx.HASANY("tags", []string{queryTag}))
				}

				items, err := readQuery(c, q)
				if err != nil {
					reportErr(fmt.Errorf("Query: %w", err))
					return
				}
				for _, item := range items {
					if item == nil {
						continue
					}
					switch queryKind {
					case 0:
						if item.Email != queryEmail {
							reportErr(fmt.Errorf("query returned inconsistent item %#v for email=%q", item, queryEmail))
							return
						}
					case 1:
						if item.Code != queryCode {
							reportErr(fmt.Errorf("query returned inconsistent item %#v for code=%d", item, queryCode))
							return
						}
					default:
						if !slices.Contains(item.Tags, queryTag) {
							reportErr(fmt.Errorf("query returned inconsistent item %#v for tag=%q", item, queryTag))
							return
						}
					}
				}
			}
		}(int64(7200 + rr))
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}

	seenEmail := make(map[string]uint64)
	seenCode := make(map[int]uint64)
	for id := uint64(1); id <= 4; id++ {
		got, err := readGet(c, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got == nil {
			continue
		}
		if prev, exists := seenEmail[got.Email]; exists {
			t.Fatalf("duplicate email %q for ids %d and %d", got.Email, prev, id)
		}
		seenEmail[got.Email] = id
		if prev, exists := seenCode[got.Code]; exists {
			t.Fatalf("duplicate code %d for ids %d and %d", got.Code, prev, id)
		}
		seenCode[got.Code] = id
	}

	for email, id := range seenEmail {
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("email", email)))
		if err != nil {
			t.Fatalf("QueryKeys(email=%q): %v", email, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("email=%q ids=%v want=[%d]", email, ids, id)
		}
	}
	for code, id := range seenCode {
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("code", code)))
		if err != nil {
			t.Fatalf("QueryKeys(code=%d): %v", code, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("code=%d ids=%v want=[%d]", code, ids, id)
		}
	}
}

func TestClose_UnblocksQueuedBatchWriters(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 16,
	})

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	closeDone := make(chan error, 1)

	go func() {
		firstDone <- writeSet(c, 1, &Rec{Name: "first", Age: 10}, OnChange(func(_ *Tx, _ uint64, _, _ *Rec) error {
			close(firstStarted)
			<-releaseFirst
			return nil
		}))
	}()

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch callback did not start in time")
	}

	go func() {
		secondDone <- writeSet(c, 2, &Rec{Name: "second", Age: 20})
	}()

	go func() {
		closeDone <- c.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for c.state.Load()&collectionClosed == 0 && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if c.state.Load()&collectionClosed == 0 {
		close(releaseFirst)
		<-firstDone
		<-secondDone
		<-closeDone
		t.Fatal("db.closed was not set by Close in time")
	}

	close(releaseFirst)

	awaitErr := func(name string, ch <-chan error) error {
		t.Helper()
		select {
		case err := <-ch:
			return err
		case <-time.After(2 * time.Second):
			t.Fatalf("%s timed out", name)
			return nil
		}
	}

	if err := awaitErr("first Set", firstDone); err != nil && !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("first Set expected nil or rbierrors.ErrClosed, got: %v", err)
	}
	if err := awaitErr("second Set", secondDone); err != nil && !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("second Set expected nil or rbierrors.ErrClosed, got: %v", err)
	}
	if err := awaitErr("Close", closeDone); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
