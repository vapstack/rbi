package wexec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

func TestStringSetPrepareUsesRequestPhysicalPayloadBuffer(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	rec := attemptRec{V: 9}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey("new", true), unsafe.Pointer(&rec), nil, 0)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	defer ex.releaseRequest(&req)

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	att := attemptStatePool.Get()
	defer attemptStatePool.Put(att)
	att.prepare(tx.Bucket(bucketName), false, ex.ops.Release, 1, true, true, len(ex.schema.Unique))
	att.strmapBucket = tx.Bucket(mapBucketName)
	ex.prepareSet(att, &req, nil)
	if req.Err != nil {
		t.Fatalf("prepareSet req error: %v", req.Err)
	}
	if len(att.ownedBuffers) != 0 {
		t.Fatalf("prepareSet created attempt-owned buffers: %d", len(att.ownedBuffers))
	}
	if len(att.prepared) != 1 {
		t.Fatalf("prepared len=%d want 1", len(att.prepared))
	}
	op := att.prepared[0]
	if !reflect.DeepEqual(op.payload, []byte{9}) {
		t.Fatalf("logical payload=%v want [9]", op.payload)
	}
	if len(op.physical) != stringValuePrefixLen+len(op.payload) {
		t.Fatalf("physical len=%d want %d", len(op.physical), stringValuePrefixLen+len(op.payload))
	}
	if &op.payload[0] != &op.physical[stringValuePrefixLen] {
		t.Fatalf("logical payload does not share physical buffer")
	}
}

func TestSetOnChangePrepareTransfersRequestValue(t *testing.T) {
	var events []string
	ex, raw, bucketName := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	clones := 0
	cloneInto := ex.ops.CloneInto
	ex.ops.CloneInto = func(src unsafe.Pointer, dst unsafe.Pointer) error {
		clones++
		return cloneInto(src, dst)
	}

	rec := attemptRec{V: 7}
	req, err := ex.buildSetRequest(
		keycodec.DataKeyFromUserKey(uint64(1), false),
		unsafe.Pointer(&rec),
		[]OnChangeHook{
			func(_ unsafe.Pointer, _ uint8, _ keycodec.DataKey, _ unsafe.Pointer, newVal unsafe.Pointer) error {
				(*attemptRec)(newVal).V = 9
				return nil
			},
		},
		0,
	)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	if clones != 1 {
		t.Fatalf("buildSetRequest clones=%d want 1", clones)
	}
	reqVal := req.setValue

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	att := attemptStatePool.Get()
	defer attemptStatePool.Put(att)
	att.prepare(tx.Bucket(bucketName), false, ex.ops.Release, 1, true, false, len(ex.schema.Unique))
	ex.prepareSet(att, &req, nil)
	if req.Err != nil {
		t.Fatalf("prepareSet req error: %v", req.Err)
	}
	if clones != 1 {
		t.Fatalf("prepareSet cloned request value again: clones=%d", clones)
	}
	if req.setValue != nil {
		t.Fatal("prepareSet kept request-owned setValue after transfer")
	}
	if len(att.releaseValues) != 1 || att.releaseValues[0] != reqVal {
		t.Fatalf("releaseValues=%v want transferred value %p", att.releaseValues, reqVal)
	}
	if len(att.prepared) != 1 || att.prepared[0].newVal != reqVal {
		t.Fatalf("prepared newVal=%p want %p", att.prepared[0].newVal, reqVal)
	}
	if got := (*attemptRec)(att.prepared[0].newVal).V; got != 9 {
		t.Fatalf("hook-mutated value=%d want 9", got)
	}
	ex.releaseRequest(&req)
}

func TestStringKeyIndexDeleteThenReinsertUsesNewDurableID(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "user", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps.StrKeyIndex = true
	oldIdx := uint64(1)
	keyMap := indexdata.GetPostingMap()
	keyMap["user"] = (posting.List{}).BuildAdded(oldIdx)
	snapshotManagerForTest(ex).Current().KeyIndex = indexdata.NewRegularFieldStorageFromPostingMapOwned(keyMap)

	delReq := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("user", true), nil, 0)
	setReq := stringSetAttemptReq("user", 9)
	executeBatchForTest(ex, []*request{&delReq, setReq})

	if err := delReq.Err; err != nil {
		t.Fatalf("delete request error = %v", err)
	}
	if err := setReq.Err; err != nil {
		t.Fatalf("set request error = %v", err)
	}

	var newIdx uint64
	if err := raw.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketName).Get(keycodec.StringBytes("user"))
		if len(v) < stringValuePrefixLen {
			return errors.New("short string value")
		}
		newIdx = keycodec.U64FromBytes(v[:stringValuePrefixLen])
		return nil
	}); err != nil {
		t.Fatalf("read string idx: %v", err)
	}
	if newIdx == oldIdx {
		t.Fatalf("reinsert reused durable id %d", oldIdx)
	}
	if got := readStringAttemptMap(t, raw, mapBucketName, oldIdx); got != "" {
		t.Fatalf("old string map entry=%q, want empty", got)
	}
	if got := readStringAttemptMap(t, raw, mapBucketName, newIdx); got != "user" {
		t.Fatalf("new string map entry=%q, want user", got)
	}
	ids := indexdata.NewFieldIndexViewFromStorage(snapshotManagerForTest(ex).Current().KeyIndex).LookupPostingRetained("user")
	if ids.Cardinality() != 1 || ids.Contains(oldIdx) || !ids.Contains(newIdx) {
		t.Fatalf("key index cardinality=%d old=%v new=%v", ids.Cardinality(), ids.Contains(oldIdx), ids.Contains(newIdx))
	}
	ids.Release()
}

func TestStringKeyIndexSetThenDeleteSameBatchLeavesNoKeyDelta(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps.StrKeyIndex = true

	setReq := stringSetAttemptReq("ghost", 9)
	delReq := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("ghost", true), nil, 0)
	executeBatchForTest(ex, []*request{setReq, &delReq})

	if err := setReq.Err; err != nil {
		t.Fatalf("set request error = %v", err)
	}
	if err := delReq.Err; err != nil {
		t.Fatalf("delete request error = %v", err)
	}

	if err := raw.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(bucketName).Get(keycodec.StringBytes("ghost")); v != nil {
			return fmt.Errorf("ghost data remained: %x", v)
		}
		c := tx.Bucket(mapBucketName).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if string(v) == "ghost" {
				return fmt.Errorf("ghost reverse map remained at idx %d", keycodec.U64FromBytes(k))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read committed state: %v", err)
	}

	ids := indexdata.NewFieldIndexViewFromStorage(snapshotManagerForTest(ex).Current().KeyIndex).LookupPostingRetained("ghost")
	if !ids.IsEmpty() {
		t.Fatalf("ghost key index posting cardinality=%d", ids.Cardinality())
	}
	ids.Release()
}

func TestStringKeyIndexKeyOnlyExistingSetThenDeleteRemovesSnapshotState(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "user", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("Compile key-only schema: %v", err)
	}

	var oldIdx uint64
	if err = raw.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketName).Get(keycodec.StringBytes("user"))
		if len(v) < stringValuePrefixLen {
			return fmt.Errorf("short string value")
		}
		oldIdx = keycodec.U64FromBytes(v[:stringValuePrefixLen])
		return nil
	}); err != nil {
		t.Fatalf("read old string idx: %v", err)
	}

	seed := attemptRec{V: 1}
	current := snapshot.BuildWithKeyDeltas(1, nil, rt, snapshot.CacheConfig{}, rt.Patch.Fields, []snapshot.BatchEntry{
		{ID: oldIdx, New: unsafe.Pointer(&seed)},
	}, []snapshot.KeyDelta{{ID: oldIdx, Key: "user", Add: true}})
	snapshotManagerForTest(ex).Publish(current)
	ex.schema = rt
	ex.unique = UniqueContext{}
	ex.snapshotOps.Schema = rt
	ex.snapshotOps.PatchFields = rt.Patch.Fields
	ex.snapshotOps.Current = snapshotManagerForTest(ex).Current
	ex.snapshotOps.StrKeyIndex = true

	setReq := stringSetAttemptReq("user", 9)
	delReq := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("user", true), nil, 0)
	executeBatchForTest(ex, []*request{setReq, &delReq})

	if err := setReq.Err; err != nil {
		t.Fatalf("set request error = %v", err)
	}
	if err := delReq.Err; err != nil {
		t.Fatalf("delete request error = %v", err)
	}

	if err = raw.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(bucketName).Get(keycodec.StringBytes("user")); v != nil {
			return fmt.Errorf("user data remained: %x", v)
		}
		var mapKey [8]byte
		if v := tx.Bucket(mapBucketName).Get(keycodec.U64BytesWithBuf(oldIdx, &mapKey)); v != nil {
			return fmt.Errorf("user reverse map remained: %q", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("read committed state: %v", err)
	}

	snap := snapshotManagerForTest(ex).Current()
	if got := snap.Universe.Cardinality(); got != 0 {
		t.Fatalf("snapshot universe cardinality=%d want 0", got)
	}
	ids := indexdata.NewFieldIndexViewFromStorage(snap.KeyIndex).LookupPostingRetained("user")
	if !ids.IsEmpty() {
		t.Fatalf("user key index posting cardinality=%d", ids.Cardinality())
	}
	ids.Release()
}

func TestSharedSetOnChangeFailureCommitsRest(t *testing.T) {
	hookErr := errors.New("on change failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	badReq := setAttemptReq(1, 1)
	badReq.onChange = []OnChangeHook{
		func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want on change error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetEmptyPayloadCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	badReq := setAttemptReq(1, 1)
	badReq.setBuffer = badReq.setBuffer[:0]
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; err == nil || !strings.Contains(err.Error(), "empty record payload") {
		t.Fatalf("bad request error = %v, want empty payload error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedTransparentSetFailureDoesNotHideExistingPatchTarget(t *testing.T) {
	var events []string
	ex, raw, bucket := newTransparentPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	badReq := setAttemptReq(1, 1)
	badReq.setBuffer = badReq.setBuffer[:0]
	patchReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(77)}}, true)

	executeBatchForTest(ex, []*request{badReq, patchReq})

	if err := badReq.Err; err == nil || !strings.Contains(err.Error(), "empty record payload") {
		t.Fatalf("bad request error = %v, want empty payload error", err)
	}
	if err := patchReq.Err; err != nil {
		t.Fatalf("patch request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{77}) {
		t.Fatalf("payload after failed set and patch = %v, want [77]", got)
	}
}

func TestSharedTransparentSetFailureDoesNotClearOldValueForLaterOnChange(t *testing.T) {
	var events []string
	ex, raw, bucket := newTransparentPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	badReq := setAttemptReq(1, 1)
	badReq.setBuffer = badReq.setBuffer[:0]
	setReq := setAttemptReq(1, 20)
	var oldNil bool
	var oldSeen byte
	setReq.onChange = []OnChangeHook{
		func(_ unsafe.Pointer, _ uint8, _ keycodec.DataKey, oldValue, _ unsafe.Pointer) error {
			if oldValue == nil {
				oldNil = true
				return nil
			}
			oldSeen = (*attemptRec)(oldValue).V
			return nil
		},
	}

	executeBatchForTest(ex, []*request{badReq, setReq})

	if err := badReq.Err; err == nil || !strings.Contains(err.Error(), "empty record payload") {
		t.Fatalf("bad request error = %v, want empty payload error", err)
	}
	if err := setReq.Err; err != nil {
		t.Fatalf("set request error = %v", err)
	}
	if oldNil || oldSeen != 10 {
		t.Fatalf("OnChange old value nil=%v seen=%d, want 10", oldNil, oldSeen)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{20}) {
		t.Fatalf("payload after failed set and set = %v, want [20]", got)
	}
}

func TestSharedStringSetKeyTooLargeCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq(strings.Repeat("x", bbolt.MaxKeySize+1), 3)
	goodReq := stringSetAttemptReq("good", 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, berrors.ErrKeyTooLarge) {
		t.Fatalf("bad request error = %v, want key too large", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "good"); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetValidateIndexFailureCommitsRest(t *testing.T) {
	validateErr := errors.New("validate index failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	ex.ops.ValidateIndex = func(ptr unsafe.Pointer) error {
		if (*attemptRec)(ptr).V == 1 {
			return validateErr
		}
		return nil
	}

	badReq := setAttemptReq(1, 1)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, validateErr) {
		t.Fatalf("bad request error = %v, want validate error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetExistingDecodeFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{0xc1})

	badReq := setAttemptReq(1, 9)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{0xc1}) {
		t.Fatalf("bad request payload = %v, want [193]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedDeleteOnChangeFailureCommitsRest(t *testing.T) {
	callbackErr := errors.New("on change failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	badReq := deleteAttemptReq(1)
	badReq.onChange = []OnChangeHook{
		func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	goodReq := deleteAttemptReq(2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("bad request payload = %v, want [1]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("good request payload still present: %v", got)
	}
}

func TestSharedDeleteDecodeFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{0xc1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	badReq := deleteAttemptReq(1)
	goodReq := deleteAttemptReq(2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{0xc1}) {
		t.Fatalf("bad request payload = %v, want [193]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("good request payload still present: %v", got)
	}
}

func TestSharedPatchOnChangeFailureCommitsRest(t *testing.T) {
	hookErr := errors.New("on change failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	badReq.onChange = []OnChangeHook{
		func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(66)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want on change error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{66}) {
		t.Fatalf("good request payload = %v, want [66]", got)
	}
}

func TestSharedPatchExistingDecodeFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{0xc1})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{0xc1}) {
		t.Fatalf("bad request payload = %v, want [193]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestLoadStateDecodeErrorClearsDiscardedStateSlot(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket, mapBucket := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return unsafe.Pointer(&attemptRec{V: data[0]}), nil
	}
	putStringAttemptPayload(t, raw, bucket, mapBucket, "bad", []byte{0xc1})

	err := raw.View(func(tx *bbolt.Tx) error {
		st := attemptState{
			dataBucket: tx.Bucket(bucket),
			states:     make([]recordState, 0, 1),
		}
		req := request{id: keycodec.DataKeyFromUserKey("bad", true)}
		if _, _, err := ex.loadState(&st, &req, true, true); !errors.Is(err, decodeErr) {
			return fmt.Errorf("loadState error = %v, want decode error", err)
		}
		if len(st.states) != 0 {
			return fmt.Errorf("states len = %d, want 0", len(st.states))
		}
		state := st.states[:cap(st.states)][0]
		if state.key != nil || state.value != nil || state.payload != nil {
			return fmt.Errorf("discarded state kept references: %+v", state)
		}
		if state.idx != 0 || state.idxKnown || state.idxNew || state.exists || state.payloadKnown {
			return fmt.Errorf("discarded state kept scalar state: %+v", state)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSharedPatchStrictUnknownFieldFailureCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "missing", Value: byte(55)}}, false)
	goodReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(66)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; err == nil || !strings.Contains(err.Error(), "cannot patch field") {
		t.Fatalf("bad request error = %v, want strict patch error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{66}) {
		t.Fatalf("good request payload = %v, want [66]", got)
	}
}

func TestSharedPatchMissingTargetCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	missingReq := patchAttemptReq(999, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{missingReq, goodReq})

	if err := missingReq.Err; err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 999); got != nil {
		t.Fatalf("missing patch created payload: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedPatchApplyFailureCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: "not-byte"}}, true)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := badReq.Err; err == nil || !strings.Contains(err.Error(), "failed to apply patch") {
		t.Fatalf("bad request error = %v, want apply patch error", err)
	}
	if err := goodReq.Err; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedPatchMissingTargetSkipsOnChange(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	var calls []uint64
	hook := func(_ unsafe.Pointer, _ uint8, key keycodec.DataKey, _, _ unsafe.Pointer) error {
		calls = append(calls, key.Uint())
		return nil
	}
	missingReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	missingReq.onChange = []OnChangeHook{hook}
	presentReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(99)}}, true)
	presentReq.onChange = []OnChangeHook{hook}

	executeBatchForTest(ex, []*request{missingReq, presentReq})

	if err := missingReq.Err; err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := presentReq.Err; err != nil {
		t.Fatalf("present request error = %v", err)
	}
	if !reflect.DeepEqual(calls, []uint64{2}) {
		t.Fatalf("OnChange keys = %v, want [2]", calls)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{99}) {
		t.Fatalf("present request payload = %v, want [99]", got)
	}
}

func TestSharedDeleteMissingTargetSkipsOnChange(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	var calls []uint64
	hook := func(_ unsafe.Pointer, _ uint8, key keycodec.DataKey, _, _ unsafe.Pointer) error {
		calls = append(calls, key.Uint())
		return nil
	}
	missingReq := deleteAttemptReq(1)
	missingReq.onChange = []OnChangeHook{hook}
	presentReq := deleteAttemptReq(2)
	presentReq.onChange = []OnChangeHook{hook}

	executeBatchForTest(ex, []*request{missingReq, presentReq})

	if err := missingReq.Err; err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := presentReq.Err; err != nil {
		t.Fatalf("present request error = %v", err)
	}
	if !reflect.DeepEqual(calls, []uint64{2}) {
		t.Fatalf("OnChange keys = %v, want [2]", calls)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("present request payload = %v, want nil", got)
	}
}
