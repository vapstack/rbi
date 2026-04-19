package main

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi"
)

type weightedWorkload struct {
	Weight float64
	Run    WorkloadFunc
}

func runWeighted(_ *rand.Rand, variants ...weightedWorkload) WorkloadFunc {
	total := 0.0
	for _, v := range variants {
		if v.Weight > 0 {
			total += v.Weight
		}
	}
	if total <= 0 {
		return func(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
			return "", fmt.Errorf("empty weighted workload")
		}
	}
	return func(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
		x := rng.Float64() * total
		acc := 0.0
		for _, v := range variants {
			if v.Weight <= 0 {
				continue
			}
			acc += v.Weight
			if x <= acc {
				return v.Run(ctx, rng)
			}
		}
		return variants[len(variants)-1].Run(ctx, rng)
	}
}

func runReadProfileByIDItems(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_profile_by_id_items"
	currentMaxID := atomic.LoadUint64(ctx.MaxIDPtr)
	id := pickReadID(rng, currentMaxID)
	done := traceQuery(ctx, queryName)
	defer done()
	item, err := ctx.DB.Get(id)
	ctx.DB.ReleaseRecords(item)
	return queryName, err
}

func runReadAccountByEmailItems(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_account_by_email_items"
	if len(ctx.EmailSamples) == 0 {
		return runReadProfileByIDItems(ctx, rng)
	}
	email := pickSampleEmail(rng, ctx.EmailSamples)
	q := qx.Query(qx.EQ("email", email)).Limit(1)
	done := traceQuery(ctx, queryName)
	defer done()
	items, err := ctx.DB.Query(q)
	ctx.DB.ReleaseRecords(items...)
	return queryName, err
}

func runReadMemberDirectoryPrefixItems(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_member_directory_prefix_items"
	prefixes := []string{"u_", "user_", "dev_", "mod_", "news_"}
	prefix := prefixes[rng.IntN(len(prefixes))]
	q := qx.Query(
		qx.PREFIX("name", prefix),
		qx.EQ("status", "active"),
	).Sort("name", qx.ASC).Limit(12)
	done := traceQuery(ctx, queryName)
	defer done()
	items, err := ctx.DB.Query(q)
	ctx.DB.ReleaseRecords(items...)
	return queryName, err
}

func runReadRecentCountryActiveItems(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_recent_country_active_items"
	country := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.EQ("country", country),
		qx.EQ("status", "active"),
	).Sort("last_login", qx.DESC).Limit(20)
	done := traceQuery(ctx, queryName)
	defer done()
	items, err := ctx.DB.Query(q)
	ctx.DB.ReleaseRecords(items...)
	return queryName, err
}

func runReadActiveRegionProItems(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_active_region_pro_items"
	now := time.Now().Unix()
	c1 := Countries[rng.IntN(len(Countries))]
	c2 := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.AND(
			qx.EQ("status", "active"),
			qx.GTE("last_login", now-72*3600),
			qx.IN("country", []string{c1, c2}),
			qx.NOTIN("plan", []string{"free"}),
		),
	).Sort("last_login", qx.DESC).Limit(40)
	done := traceQuery(ctx, queryName)
	defer done()
	items, err := ctx.DB.Query(q)
	ctx.DB.ReleaseRecords(items...)
	return queryName, err
}

func runReadFrontpageCandidateKeys(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_frontpage_candidate_keys"
	now := time.Now().Unix()
	tags := pickRandomSlice(rng, AllTags, 3)
	if len(tags) < 2 {
		tags = []string{"technology", "programming"}
	}
	q := qx.Query(
		qx.AND(
			qx.EQ("status", "active"),
			qx.HASANY("tags", tags),
			qx.GTE("score", 120.0),
			qx.GTE("last_login", now-45*24*3600),
		),
	).Sort("score", qx.DESC).Limit(100)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.QueryKeys(q)
	return queryName, err
}

func runReadModerationQueueKeys(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_moderation_queue_keys"
	now := time.Now().Unix()
	tags := pickRandomSlice(rng, AllTags, 2)
	if len(tags) < 2 {
		tags = []string{"politics", "cryptocurrency"}
	}
	q := qx.Query(
		qx.OR(
			qx.EQ("status", "pending"),
			qx.EQ("status", "suspended"),
			qx.AND(
				qx.EQ("status", "active"),
				qx.HASANY("tags", tags),
				qx.GTE("score", 4000.0),
				qx.GTE("created_at", now-180*24*3600),
			),
		),
	).Sort("created_at", qx.DESC).Limit(100)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.QueryKeys(q)
	return queryName, err
}

func runReadLeaderboardTopItems(ctx *WorkloadContext, _ *rand.Rand) (string, error) {
	const queryName = "read_leaderboard_top_items"
	now := time.Now().Unix()
	q := qx.Query(
		qx.AND(
			qx.EQ("status", "active"),
			qx.GTE("score", 250.0),
			qx.GTE("last_login", now-180*24*3600),
		),
	).Sort("score", qx.DESC).Limit(50)
	done := traceQuery(ctx, queryName)
	defer done()
	items, err := ctx.DB.Query(q)
	ctx.DB.ReleaseRecords(items...)
	return queryName, err
}

func runReadSignupDashboardCount(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_signup_dashboard_count"
	now := time.Now().Unix()
	c1 := Countries[rng.IntN(len(Countries))]
	c2 := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.AND(
			qx.GTE("created_at", now-21*24*3600),
			qx.EQ("is_verified", true),
			qx.IN("country", []string{c1, c2}),
			qx.NOTIN("status", []string{"banned"}),
		),
	)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.Count(q.Filter)
	return queryName, err
}

func runReadInactiveCleanupKeys(ctx *WorkloadContext, _ *rand.Rand) (string, error) {
	const queryName = "read_inactive_cleanup_keys"
	now := time.Now().Unix()
	q := qx.Query(
		qx.AND(
			qx.LT("last_login", now-365*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.NOTIN("plan", []string{"pro", "enterprise"}),
			qx.LT("score", 120.0),
		),
	).Limit(250)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.QueryKeys(q)
	return queryName, err
}

func runReadStaffAuditFeedKeys(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_staff_audit_feed_keys"
	now := time.Now().Unix()
	country := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.AND(
			qx.HASANY("roles", []string{"moderator", "admin", "staff", "bot"}),
			qx.GTE("last_login", now-120*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.EQ("country", country),
		),
	).Sort("last_login", qx.DESC).Limit(120)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.QueryKeys(q)
	return queryName, err
}

func runReadDiscoveryExploreKeys(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_discovery_explore_keys"
	now := time.Now().Unix()
	tags := pickRandomSlice(rng, AllTags, 2)
	if len(tags) < 2 {
		tags = []string{"technology", "gaming"}
	}
	country := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("status", "active"),
				qx.HASANY("tags", tags),
				qx.GTE("score", 320.0),
				qx.GTE("last_login", now-30*24*3600),
			),
			qx.AND(
				qx.EQ("country", country),
				qx.EQ("is_verified", true),
				qx.GTE("created_at", now-180*24*3600),
				qx.GTE("score", 180.0),
			),
			qx.AND(
				qx.IN("plan", []string{"pro", "enterprise"}),
				qx.HASANY("roles", []string{"moderator", "admin", "staff"}),
				qx.GTE("last_login", now-14*24*3600),
			),
		),
	).Sort("created_at", qx.DESC).Limit(150)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.QueryKeys(q)
	return queryName, err
}

func runReadDormantArchivePageKeys(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "read_dormant_archive_page_keys"
	now := time.Now().Unix()
	skip := 1500 + rng.IntN(3500)
	q := qx.Query(
		qx.AND(
			qx.NOTIN("status", []string{"banned"}),
			qx.LT("last_login", now-180*24*3600),
			qx.NOTIN("plan", []string{"enterprise"}),
			qx.LT("score", 180.0),
		),
	).Sort("last_login", qx.ASC).Offset(skip).Limit(100)
	done := traceQuery(ctx, queryName)
	defer done()
	_, err := ctx.DB.QueryKeys(q)
	return queryName, err
}

func runWriteTouchLastSeen(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_touch_last_seen"
	curr := atomic.LoadUint64(ctx.MaxIDPtr)
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
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Patch(id, patch)
}

func runWriteVoteKarma(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_vote_karma"
	curr := atomic.LoadUint64(ctx.MaxIDPtr)
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
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Patch(id, patch)
}

func runWriteProfileEdit(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_profile_edit"
	curr := atomic.LoadUint64(ctx.MaxIDPtr)
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
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Patch(id, patch)
}

func runWriteModerationAction(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_moderation_action"
	curr := atomic.LoadUint64(ctx.MaxIDPtr)
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
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Patch(id, patch)
}

func runWriteBulkPatch(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_bulk_patch"
	curr := atomic.LoadUint64(ctx.MaxIDPtr)
	if curr == 0 {
		return "write_bulk_patch_skip_empty", nil
	}
	id := pickReadID(rng, curr)
	patch := []rbi.Field{
		{Name: "country", Value: Countries[rng.IntN(len(Countries))]},
		{Name: "plan", Value: weightedPlan(rng)},
		{Name: "status", Value: weightedStatus(rng)},
		{Name: "age", Value: 13 + rng.IntN(58)},
		{Name: "score", Value: sampleScore(rng)},
		{Name: "is_verified", Value: rng.Float64() < 0.58},
		{Name: "last_login", Value: time.Now().Unix() - int64(rng.IntN(14*24*3600))},
		{Name: "tags", Value: pickUserTags(rng)},
	}
	if rng.Float64() < 0.75 {
		patch = append(patch, rbi.Field{Name: "roles", Value: sampleRoleSet(rng)})
	}
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Patch(id, patch)
}

func runWriteFullSetExisting(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_full_set_existing"
	curr := atomic.LoadUint64(ctx.MaxIDPtr)
	if curr == 0 {
		return "write_full_set_skip_empty", nil
	}
	id := pickReadID(rng, curr)
	user := generateUser(rng, id)
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Set(id, user)
}

func runWriteInsert(ctx *WorkloadContext, rng *rand.Rand) (string, error) {
	const queryName = "write_insert_signup"
	newID := atomic.AddUint64(ctx.MaxIDPtr, 1)
	user := generateUser(rng, newID)
	done := traceQuery(ctx, queryName)
	defer done()
	return queryName, ctx.DB.Set(newID, user)
}
