package wexec

import (
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

func TestSharedSetOnChangeFailureCommitsRestWithUniqueIndex(t *testing.T) {
	callbackErr := errors.New("on change failed")
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, nil, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	badReq := setAttemptReq(1, 7)
	badReq.onChange = []OnChangeHook{
		func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	goodReq := setAttemptReq(2, 7)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{7}) {
		t.Fatalf("good request payload = %v, want [7]", got)
	}
}

func TestSharedSetUniqueRejectPreservesAcceptedWritesAndIndex(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 2}, {V: 3}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
		{ID: 3, New: unsafe.Pointer(&seed[2])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})
	putAttemptPayload(t, raw, bucket, 3, []byte{3})

	badReq := setAttemptReq(3, 1)
	goodReq := setAttemptReq(2, 4)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("bad request error = %v, want unique error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{4}) {
		t.Fatalf("id=2 payload = %v, want [4]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 3); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=3 payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); !ids.Contains(1) || ids.Contains(3) {
		t.Fatalf("unique value 1 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(3) {
		t.Fatalf("unique value 3 lost id=3")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(4)); !ids.Contains(2) {
		t.Fatalf("unique value 4 missing id=2")
	}
}

func TestRunAtomicUniqueDeleteThenSetReusesFreedValue(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 2}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	deleteReq := deleteAttemptReq(1)
	setReq := setAttemptReq(2, 1)
	defer encodeBufferPool.Put(setReq.setBuffer)

	executeAtomicRequestsForTest(ex, []*request{deleteReq, setReq})

	if deleteReq.Err != nil {
		t.Fatalf("delete request error = %v", deleteReq.Err)
	}
	if setReq.Err != nil {
		t.Fatalf("set request error = %v", setReq.Err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("id=1 payload after delete = %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("id=2 payload = %v, want [1]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); ids.Contains(1) || !ids.Contains(2) {
		t.Fatalf("unique value 1 ids mismatch after reuse")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); ids.Contains(2) {
		t.Fatalf("stale unique value 2 owner remained")
	}
}

func TestSharedUniqueDeleteThenSetReusesFreedValue(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 2}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	deleteReq := deleteAttemptReq(1)
	setReq := setAttemptReq(2, 1)

	executeBatchForTest(ex, []*request{deleteReq, setReq})

	if err := deleteReq.Err; err != nil {
		t.Fatalf("delete request error = %v", err)
	}
	if err := setReq.Err; err != nil {
		t.Fatalf("set request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("id=1 payload after delete = %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("id=2 payload = %v, want [1]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); ids.Contains(1) || !ids.Contains(2) {
		t.Fatalf("unique value 1 ids mismatch after reuse")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); ids.Contains(2) {
		t.Fatalf("stale unique value 2 owner remained")
	}
}

func TestSharedUniqueRejectRepreparesLaterSameID(t *testing.T) {
	seed := attemptRec{V: 1}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed)},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})

	setReq := setAttemptReq(2, 1)
	patchReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(2)}}, true)

	executeBatchForTest(ex, []*request{setReq, patchReq})

	if err := setReq.Err; !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("set request error = %v, want unique error", err)
	}
	if err := patchReq.Err; err != nil {
		t.Fatalf("patch request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("id=2 payload persisted after rejected Set then Patch: %v", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); !ids.Contains(1) || ids.Contains(2) {
		t.Fatalf("unique value 1 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); ids.Contains(2) {
		t.Fatalf("unique value 2 contains rejected id")
	}
}

func TestFrameAttemptValidateCurrentAllowsPriorUniqueDeparture(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 2}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	attempt, err := ex.NewFrameAttempt(tx, 2)
	if err != nil {
		t.Fatalf("NewFrameAttempt: %v", err)
	}
	defer attempt.Cancel()

	deleteBatch := ex.NewBatch()
	deleteBatch.AddDelete(keycodec.DataKeyFromUserKey(uint64(1), false), nil, 0)
	if _, err = attempt.Prepare(&deleteBatch, nil, nil); err != nil {
		t.Fatalf("Prepare delete: %v", err)
	}
	if err = attempt.ValidateCurrent(); err != nil {
		t.Fatalf("ValidateCurrent delete: %v", err)
	}
	attempt.AcceptValidatedCurrent()

	setBatch := ex.NewBatch()
	next := attemptRec{V: 1}
	if err = setBatch.AddSet(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&next), nil, 0); err != nil {
		t.Fatalf("AddSet: %v", err)
	}
	if _, err = attempt.Prepare(&setBatch, nil, nil); err != nil {
		t.Fatalf("Prepare set: %v", err)
	}
	if err = attempt.ValidateCurrent(); err != nil {
		t.Fatalf("ValidateCurrent set after prior departure: %v", err)
	}
}

func TestFrameAttemptValidateCurrentMasksAcceptedSeenWhenCurrentLeavesSameID(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 3}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{3})

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	attempt, err := ex.NewFrameAttempt(tx, 2)
	if err != nil {
		t.Fatalf("NewFrameAttempt: %v", err)
	}
	defer attempt.Cancel()

	first := ex.NewBatch()
	v2 := attemptRec{V: 2}
	if err = first.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v2), nil, 0); err != nil {
		t.Fatalf("AddSet first: %v", err)
	}
	if _, err = attempt.Prepare(&first, nil, nil); err != nil {
		t.Fatalf("Prepare first: %v", err)
	}
	if err = attempt.ValidateCurrent(); err != nil {
		t.Fatalf("ValidateCurrent first: %v", err)
	}
	attempt.AcceptValidatedCurrent()

	second := ex.NewBatch()
	v4 := attemptRec{V: 4}
	if err = second.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v4), nil, 0); err != nil {
		t.Fatalf("AddSet id1: %v", err)
	}
	if err = second.AddSet(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&v2), nil, 0); err != nil {
		t.Fatalf("AddSet id2: %v", err)
	}
	if _, err = attempt.Prepare(&second, nil, nil); err != nil {
		t.Fatalf("Prepare second: %v", err)
	}
	if err = attempt.ValidateCurrent(); err != nil {
		t.Fatalf("ValidateCurrent second: %v", err)
	}
}

func TestFrameAttemptValidateCurrentRejectsReoccupiedBaseUniqueOwner(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 3}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{3})

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	attempt, err := ex.NewFrameAttempt(tx, 2)
	if err != nil {
		t.Fatalf("NewFrameAttempt: %v", err)
	}
	defer attempt.Cancel()

	first := ex.NewBatch()
	v2 := attemptRec{V: 2}
	if err = first.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v2), nil, 0); err != nil {
		t.Fatalf("AddSet first: %v", err)
	}
	if _, err = attempt.Prepare(&first, nil, nil); err != nil {
		t.Fatalf("Prepare first: %v", err)
	}
	if err = attempt.ValidateCurrent(); err != nil {
		t.Fatalf("ValidateCurrent first: %v", err)
	}
	attempt.AcceptValidatedCurrent()

	second := ex.NewBatch()
	v1 := attemptRec{V: 1}
	if err = second.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v1), nil, 0); err != nil {
		t.Fatalf("AddSet second: %v", err)
	}
	if _, err = attempt.Prepare(&second, nil, nil); err != nil {
		t.Fatalf("Prepare second: %v", err)
	}
	if err = attempt.ValidateCurrent(); err != nil {
		t.Fatalf("ValidateCurrent second: %v", err)
	}
	attempt.AcceptValidatedCurrent()

	third := ex.NewBatch()
	if err = third.AddSet(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&v1), nil, 0); err != nil {
		t.Fatalf("AddSet third: %v", err)
	}
	if _, err = attempt.Prepare(&third, nil, nil); err != nil {
		t.Fatalf("Prepare third: %v", err)
	}
	if err = attempt.ValidateCurrent(); !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("ValidateCurrent third = %v, want unique error", err)
	}
}

func TestRunAtomicUniqueDuplicateIDUsesFinalValue(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 2}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	transientReq := setAttemptReq(1, 2)
	defer encodeBufferPool.Put(transientReq.setBuffer)
	finalReq := setAttemptReq(1, 3)
	defer encodeBufferPool.Put(finalReq.setBuffer)

	executeAtomicRequestsForTest(ex, []*request{transientReq, finalReq})

	if transientReq.Err != nil {
		t.Fatalf("transient request error = %v", transientReq.Err)
	}
	if finalReq.Err != nil {
		t.Fatalf("final request error = %v", finalReq.Err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=1 payload = %v, want [3]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("id=2 payload = %v, want [2]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); ids.Contains(1) {
		t.Fatalf("stale unique value 1 owner remained")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); ids.Contains(1) || !ids.Contains(2) {
		t.Fatalf("unique value 2 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(1) {
		t.Fatalf("unique value 3 missing id=1")
	}
}

func TestSharedPatchUniqueRejectPreservesAcceptedWritesAndIndex(t *testing.T) {
	seed := []attemptRec{{V: 1}, {V: 2}, {V: 3}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
		{ID: 3, New: unsafe.Pointer(&seed[2])},
	}, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})
	putAttemptPayload(t, raw, bucket, 3, []byte{3})

	badReq := patchAttemptReq(3, []schema.PatchItem{{Name: "v", Value: byte(2)}}, true)
	goodReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(4)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("bad request error = %v, want unique error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{4}) {
		t.Fatalf("id=1 payload = %v, want [4]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 3); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=3 payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); !ids.Contains(2) || ids.Contains(3) {
		t.Fatalf("unique value 2 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(3) {
		t.Fatalf("unique value 3 lost id=3")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(4)); !ids.Contains(1) {
		t.Fatalf("unique value 4 missing id=1")
	}
}

func TestSharedStringSetUniqueRejectRollsBackCreatedKey(t *testing.T) {
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq("ghost", 1)
	goodReq := stringSetAttemptReq("real", 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("bad request error = %v, want unique error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("real payload = %v, want [2]", got)
	}

	if seq := stringAttemptMapSequence(t, raw, bucketMapName(bucket)); seq != 3 {
		t.Fatalf("string map sequence = %d, want 3", seq)
	}
	if got := readStringAttemptMap(t, raw, bucketMapName(bucket), 1); got != "seed" {
		t.Fatalf("map[1] = %q, want seed", got)
	}
	if got := readStringAttemptMap(t, raw, bucketMapName(bucket), 2); got != "" {
		t.Fatalf("map[2] = %q, want empty", got)
	}
	if got := readStringAttemptMap(t, raw, bucketMapName(bucket), 3); got != "real" {
		t.Fatalf("map[3] = %q, want real", got)
	}
}

func TestSharedStringDeleteSurvivesRejectedSetSameKey(t *testing.T) {
	var events []string
	ex, raw, bucket, mapBucket := newStringAttemptTestExecutor(t, &events, "victim", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	otherIdx := putStringAttemptPayload(t, raw, bucket, mapBucket, "other", []byte{2})
	victim := attemptRec{V: 1}
	other := attemptRec{V: 2}
	snapshotManagerForTest(ex).Publish(snapshot.Build(1, nil, ex.schema, snapshot.CacheConfig{}, ex.schema.Patch.Fields, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&victim)},
		{ID: otherIdx, New: unsafe.Pointer(&other)},
	}))

	key := keycodec.DataKeyFromUserKey("victim", true)
	deleteReq := ex.buildDeleteRequest(key, nil, 0)
	setValue := attemptRec{V: 2}
	setReq, err := ex.buildSetRequest(key, unsafe.Pointer(&setValue), nil, 0)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}

	executeBatchForTest(ex, []*request{&deleteReq})

	if err = deleteReq.Err; err != nil {
		t.Fatalf("delete request error = %v", err)
	}

	executeBatchForTest(ex, []*request{&setReq})

	if err = setReq.Err; !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("set request error = %v, want unique error", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "victim"); got != nil {
		t.Fatalf("victim payload survived rejected set: %v", got)
	}
	if got := readStringAttemptMap(t, raw, mapBucket, 1); got != "" {
		t.Fatalf("map[1] = %q, want empty after delete", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "other"); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("other payload = %v, want [2]", got)
	}
}

func TestSharedStringSetOnChangeFailureDoesNotCreateStringKey(t *testing.T) {
	hookErr := errors.New("on change failed")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq("ghost", 2)
	badReq.onChange = []OnChangeHook{
		func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := stringSetAttemptReq("real", 3)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want on change error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("real payload = %v, want [3]", got)
	}

	mapBucket := bucketMapName(bucket)
	if seq := stringAttemptMapSequence(t, raw, mapBucket); seq != 2 {
		t.Fatalf("string map sequence = %d, want 2", seq)
	}
	if got := readStringAttemptMap(t, raw, mapBucket, 2); got != "real" {
		t.Fatalf("map[2] = %q, want real", got)
	}
}

func TestSharedStringSetOnChangeFailureRetriesNeighborWithRolledBackStringKeys(t *testing.T) {
	callbackErr := errors.New("on change failed")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq("ghost", 2)
	badReq.onChange = []OnChangeHook{
		func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	goodReq := stringSetAttemptReq("real", 3)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("real payload = %v, want [3]", got)
	}

	mapBucket := bucketMapName(bucket)
	if seq := stringAttemptMapSequence(t, raw, mapBucket); seq != 2 {
		t.Fatalf("string map sequence = %d, want 2", seq)
	}
	if got := readStringAttemptMap(t, raw, mapBucket, 2); got != "real" {
		t.Fatalf("map[2] = %q, want real", got)
	}
}

func TestSharedStringSetCommitFailureRollsBackStringKey(t *testing.T) {
	commitErr := errors.New("commit failed")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, func(*bbolt.Tx) error {
		return commitErr
	})

	req := stringSetAttemptReq("ghost", 2)
	executeBatchForTest(ex, []*request{req})

	if err := req.Err; !errors.Is(err, commitErr) {
		t.Fatalf("request error = %v, want commit error", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	mapBucket := bucketMapName(bucket)
	if seq := stringAttemptMapSequence(t, raw, mapBucket); seq != 1 {
		t.Fatalf("string map sequence = %d, want 1", seq)
	}
	if got := readStringAttemptMap(t, raw, mapBucket, 2); got != "" {
		t.Fatalf("map[2] = %q, want empty", got)
	}
}
