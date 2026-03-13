package main

import (
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

func runDedicatedWriteTouchLastSeen(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	_ []string,
) (string, error) {
	curr := atomic.LoadUint64(maxIDPtr)
	if curr == 0 {
		return "write_touch_last_seen_skip_empty", nil
	}

	id := pickReadID(rng, curr)
	patch := []rbi.Field{{Name: "last_login", Value: time.Now().Unix()}}
	if rng.Float64() < 0.22 {
		patch = append(patch, rbi.Field{Name: "status", Value: "active"})
	}
	if rng.Float64() < 0.06 {
		patch = append(patch, rbi.Field{Name: "plan", Value: weightedPlan(rng)})
	}
	return "write_touch_last_seen", db.Patch(id, patch)
}

func runDedicatedWriteVoteKarma(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	_ []string,
) (string, error) {
	curr := atomic.LoadUint64(maxIDPtr)
	if curr == 0 {
		return "write_vote_karma_skip_empty", nil
	}

	id := pickReadID(rng, curr)
	patch := []rbi.Field{{Name: "score", Value: sampleScore(rng)}}
	if rng.Float64() < 0.20 {
		patch = append(patch, rbi.Field{Name: "last_login", Value: time.Now().Unix() - int64(rng.IntN(6*3600))})
	}
	if rng.Float64() < 0.10 {
		patch = append(patch, rbi.Field{Name: "is_verified", Value: rng.Float64() < 0.70})
	}
	return "write_vote_karma", db.Patch(id, patch)
}

func runDedicatedWriteProfileEdit(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	_ []string,
) (string, error) {
	curr := atomic.LoadUint64(maxIDPtr)
	if curr == 0 {
		return "write_profile_edit_skip_empty", nil
	}

	id := pickReadID(rng, curr)
	patch := []rbi.Field{
		{Name: "country", Value: Countries[rng.IntN(len(Countries))]},
		{Name: "plan", Value: weightedPlan(rng)},
	}
	if rng.Float64() < 0.30 {
		patch = append(patch, rbi.Field{Name: "tags", Value: pickUserTags(rng)})
	}
	if rng.Float64() < 0.18 {
		patch = append(patch, rbi.Field{Name: "roles", Value: sampleRoleSet(rng)})
	}
	return "write_profile_edit", db.Patch(id, patch)
}

func runDedicatedWriteModerationAction(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	_ []string,
) (string, error) {
	curr := atomic.LoadUint64(maxIDPtr)
	if curr == 0 {
		return "write_moderation_action_skip_empty", nil
	}
	id := pickReadID(rng, curr)

	status := weightedStatus(rng)
	if status == "inactive" {
		status = "pending"
	}
	patch := []rbi.Field{{Name: "status", Value: status}}
	if rng.Float64() < 0.35 {
		patch = append(patch, rbi.Field{Name: "roles", Value: sampleRoleSet(rng)})
	}
	if rng.Float64() < 0.20 {
		patch = append(patch, rbi.Field{Name: "is_verified", Value: false})
	}
	return "write_moderation_action", db.Patch(id, patch)
}

func runDedicatedWriteInsert(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	_ []string,
) (string, error) {
	newID := atomic.AddUint64(maxIDPtr, 1)
	user := generateUser(rng, newID)
	return "write_insert_signup", db.Set(newID, user)
}

func runDedicatedWriteDelete(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	_ []string,
) (string, error) {
	curr := atomic.LoadUint64(maxIDPtr)
	if curr < 1_000 {
		return "write_delete_account_skip_small", nil
	}
	id := pickReadID(rng, curr)
	return "write_delete_account", db.Delete(id)
}

func runRandomWriteScenario(db *rbi.DB[uint64, UserBench], rng *rand.Rand, maxIDPtr *uint64) (string, error) {
	action := rng.Float64()

	switch {
	case action < 0.62:
		return runDedicatedWriteTouchLastSeen(db, rng, maxIDPtr, nil)
	case action < 0.82:
		return runDedicatedWriteVoteKarma(db, rng, maxIDPtr, nil)
	case action < 0.93:
		return runDedicatedWriteProfileEdit(db, rng, maxIDPtr, nil)
	case action < 0.97:
		return runDedicatedWriteModerationAction(db, rng, maxIDPtr, nil)
	case action < 0.995:
		return runDedicatedWriteInsert(db, rng, maxIDPtr, nil)
	default:
		return runDedicatedWriteDelete(db, rng, maxIDPtr, nil)
	}
}
