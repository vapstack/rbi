package posting

import (
	"math/rand"
	"slices"
	"testing"
)

func TestPostingExtraBorrowedReleasePayloadCorruptsSmallOwner(t *testing.T) {
	base := postingFromIDs(3, 7, 11, 19)
	want := []uint64{3, 7, 11, 19}
	if base.IsEmpty() || base.isSingleton() || base.largeRef() != nil {
		t.Fatalf("expected compact owned posting")
	}
	t.Cleanup(func() {
		if got := base.ToArray(); slices.Equal(got, want) && base.Cardinality() == uint64(len(want)) {
			base.Release()
		}
	})

	borrowed := base.Borrow()
	if !borrowed.IsBorrowed() {
		t.Fatalf("expected borrowed view")
	}

	borrowed.ReleasePayload()

	if got := base.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("borrowed ReleasePayload corrupted compact owner: got=%v want=%v", got, want)
	}
	if got := base.Cardinality(); got != uint64(len(want)) {
		t.Fatalf("borrowed ReleasePayload changed compact owner cardinality: got=%d want=%d", got, len(want))
	}
}

func TestPostingExtraBorrowedReleasePayloadCorruptsLargeOwner(t *testing.T) {
	var ids []uint64
	for i := uint64(0); i < MidCap+8; i++ {
		ids = append(ids, i*3+1)
	}
	ids = append(ids, 1<<32|5, 1<<32|9, 2<<32|11)

	base := postingFromIDs(ids...)
	if base.largeRef() == nil {
		t.Fatalf("expected large owned posting")
	}
	t.Cleanup(func() {
		if got := base.ToArray(); slices.Equal(got, ids) && base.Cardinality() == uint64(len(ids)) {
			base.Release()
		}
	})

	borrowed := base.Borrow()
	if !borrowed.IsBorrowed() {
		t.Fatalf("expected borrowed view")
	}

	borrowed.ReleasePayload()

	if got := base.ToArray(); !slices.Equal(got, ids) {
		t.Fatalf("borrowed ReleasePayload corrupted large owner: got=%v want=%v", got, ids)
	}
	if got := base.Cardinality(); got != uint64(len(ids)) {
		t.Fatalf("borrowed ReleasePayload changed large owner cardinality: got=%d want=%d", got, len(ids))
	}
}

func TestPostingExtraIteratorLosesDataAfterOwnerRelease(t *testing.T) {
	var ids []uint64
	for i := uint64(0); i < MidCap+8; i++ {
		ids = append(ids, i*2+1)
	}

	base := postingFromIDs(ids...)
	if base.largeRef() == nil {
		t.Fatalf("expected large posting")
	}

	it := base.Iter()
	defer it.Release()

	var got []uint64
	if it.HasNext() {
		got = append(got, it.Next())
	}

	base.Release()

	for it.HasNext() {
		got = append(got, it.Next())
	}

	if !slices.Equal(got, ids) {
		t.Fatalf("iterator lost owner payload after Release: got=%v want=%v", got, ids)
	}
}

func TestPostingExtraBuildAddedShuffledDenseRangeOptimizesSafely(t *testing.T) {
	const total = 220

	input := make([]uint64, total)
	want := make([]uint64, total)
	for i := range input {
		input[i] = uint64(i + 1)
		want[i] = uint64(i + 1)
	}

	rng := rand.New(rand.NewSource(20260411))
	rng.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})

	var ids List
	for _, id := range input {
		ids = ids.BuildAdded(id)
	}
	defer func() {
		ids.Release()
	}()

	if got := ids.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("BuildAdded shuffled range mismatch before optimize: got=%v want=%v", got, want)
	}

	ids = ids.BuildOptimized()
	if got := ids.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("BuildAdded shuffled range mismatch after optimize: got=%v want=%v", got, want)
	}
}

func TestPostingExtraBuildMergedOwnedShuffledPartitionsOptimizeSafely(t *testing.T) {
	const (
		total    = 220
		workers  = 4
		partSize = total / workers
	)

	input := make([]uint64, total)
	want := make([]uint64, total)
	for i := range input {
		input[i] = uint64(i + 1)
		want[i] = uint64(i + 1)
	}

	rng := rand.New(rand.NewSource(20260412))
	rng.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})

	parts := make([]List, workers)
	for w := 0; w < workers; w++ {
		start := w * partSize
		end := start + partSize
		if w == workers-1 {
			end = len(input)
		}
		for _, id := range input[start:end] {
			parts[w] = parts[w].BuildAdded(id)
		}
	}

	var merged List
	for i := range parts {
		merged = merged.BuildMergedOwned(parts[i])
		parts[i] = List{}
	}
	defer func() {
		merged.Release()
	}()

	if got := merged.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("BuildMergedOwned shuffled partitions mismatch before optimize: got=%v want=%v", got, want)
	}

	merged = merged.BuildOptimized()
	if got := merged.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("BuildMergedOwned shuffled partitions mismatch after optimize: got=%v want=%v", got, want)
	}
}
