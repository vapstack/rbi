package main

import (
	"math/rand/v2"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi"
)

func runRandomReadScenario(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	currentMaxID uint64,
	emailSamples []string,
) (string, error) {
	if currentMaxID == 0 {
		return "read_skip_no_data", nil
	}

	r := rng.Float64()
	if r < 0.42 {
		return runReadUserLookupItems(db, rng, currentMaxID, emailSamples)
	}
	switch {
	case r < 0.62:
		return runReadUserByNamePrefixItems(db, rng, currentMaxID, emailSamples)
	case r < 0.74:
		return runReadUserCommunityFrontpageKeys(db, rng, currentMaxID, emailSamples)
	case r < 0.82:
		return runReadUserOnlineRegionItems(db, rng, currentMaxID, emailSamples)
	case r < 0.88:
		return runReadUserLeaderboardItems(db, rng, currentMaxID, emailSamples)
	case r < 0.92:
		return runReadUserGrowthCohortCount(db, rng, currentMaxID, emailSamples)
	case r < 0.95:
		return runReadUserModerationQueueKeys(db, rng, currentMaxID, emailSamples)
	case r < 0.975:
		return runReadUserLapsedCleanupKeys(db, rng, currentMaxID, emailSamples)
	case r < 0.99:
		return runReadUserSafetyAuditKeys(db, rng, currentMaxID, emailSamples)
	case r < 0.997:
		return runReadUserDiscoveryORKeys(db, rng, currentMaxID, emailSamples)
	default:
		return runReadUserDeepPaginationKeys(db, rng, currentMaxID, emailSamples)
	}
}

func readScenarios() []readScenario {
	return []readScenario{
		{Name: "read_user_by_email_items", Weight: 4, Run: runReadUserLookupItems},
		{Name: "read_user_by_name_prefix_items", Weight: 2, Run: runReadUserByNamePrefixItems},
		{Name: "read_user_community_frontpage_keys", Weight: 2, Run: runReadUserCommunityFrontpageKeys},
		{Name: "read_user_online_region_items", Weight: 1, Run: runReadUserOnlineRegionItems},
		{Name: "read_user_leaderboard_items", Weight: 1, Run: runReadUserLeaderboardItems},
		{Name: "read_user_growth_cohort_count", Weight: 1, Run: runReadUserGrowthCohortCount},
		{Name: "read_user_moderation_queue_keys", Weight: 1, Run: runReadUserModerationQueueKeys},
		{Name: "read_user_lapsed_cleanup_keys", Weight: 1, Run: runReadUserLapsedCleanupKeys},
		{Name: "read_user_safety_audit_keys", Weight: 1, Run: runReadUserSafetyAuditKeys},
		{Name: "read_user_discovery_or_keys", Weight: 1, Run: runReadUserDiscoveryORKeys},
		{Name: "read_user_deep_pagination_keys", Weight: 1, Run: runReadUserDeepPaginationKeys},
	}
}

func runReadUserLookupItems(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	currentMaxID uint64,
	emailSamples []string,
) (string, error) {
	if len(emailSamples) > 0 {
		email := pickSampleEmail(rng, emailSamples)
		q := qx.Query(qx.EQ("email", email)).Max(1)
		items, err := db.Query(q)
		db.ReleaseRecords(items...)
		return "read_user_by_email_items", err
	}

	id := pickReadID(rng, currentMaxID)
	q := qx.Query(qx.EQ("id", id)).Max(1)
	items, err := db.Query(q)
	db.ReleaseRecords(items...)
	return "read_user_by_id_eq_items", err
}

func runReadUserByNamePrefixItems(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
	prefixes := []string{"u_", "user_", "mod_", "dev_", "news_"}
	prefix := prefixes[rng.IntN(len(prefixes))]
	country := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.PREFIX("name", prefix),
		qx.EQ("country", country),
		qx.NOTIN("status", []string{"banned", "suspended"}),
	).By("last_login", qx.DESC).Max(20)
	items, err := db.Query(q)
	db.ReleaseRecords(items...)
	return "read_user_by_name_prefix_items", err
}

func runReadUserOnlineRegionItems(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
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
	).By("last_login", qx.DESC).Max(40)
	items, err := db.Query(q)
	db.ReleaseRecords(items...)
	return "read_user_online_region_items", err
}

func runReadUserCommunityFrontpageKeys(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
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
	).By("score", qx.DESC).Max(120)
	_, err := db.QueryKeys(q)
	return "read_user_community_frontpage_keys", err
}

func runReadUserModerationQueueKeys(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
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
	).By("created_at", qx.DESC).Max(100)
	_, err := db.QueryKeys(q)
	return "read_user_moderation_queue_keys", err
}

func runReadUserLeaderboardItems(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
	now := time.Now().Unix()
	q := qx.Query(
		qx.AND(
			qx.EQ("status", "active"),
			qx.GTE("score", 250.0),
			qx.GTE("last_login", now-180*24*3600),
		),
	).By("score", qx.DESC).Skip(rng.IntN(400)).Max(80)
	items, err := db.Query(q)
	db.ReleaseRecords(items...)
	return "read_user_leaderboard_items", err
}

func runReadUserGrowthCohortCount(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
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
	_, err := db.Count(q)
	return "read_user_growth_cohort_count", err
}

func runReadUserLapsedCleanupKeys(
	db *rbi.DB[uint64, UserBench],
	_ *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
	now := time.Now().Unix()
	q := qx.Query(
		qx.AND(
			qx.LT("last_login", now-365*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.NOTIN("plan", []string{"pro", "enterprise"}),
			qx.LT("score", 120.0),
		),
	).Max(250)
	_, err := db.QueryKeys(q)
	return "read_user_lapsed_cleanup_keys", err
}

func runReadUserSafetyAuditKeys(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
	now := time.Now().Unix()
	country := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.AND(
			qx.HASANY("roles", []string{"moderator", "admin", "staff", "bot"}),
			qx.GTE("last_login", now-120*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.EQ("country", country),
		),
	).By("last_login", qx.ASC).Max(120)
	_, err := db.QueryKeys(q)
	return "read_user_safety_audit_keys", err
}

func runReadUserDiscoveryORKeys(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
	now := time.Now().Unix()
	tags := pickRandomSlice(rng, AllTags, 2)
	if len(tags) < 2 {
		tags = []string{"technology", "gaming"}
	}
	country := Countries[rng.IntN(len(Countries))]
	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.PREFIX("email", "user1"),
				qx.GTE("score", 220.0),
				qx.GTE("last_login", now-90*24*3600),
			),
			qx.AND(
				qx.EQ("country", country),
				qx.HASANY("tags", tags),
				qx.GTE("created_at", now-180*24*3600),
			),
			qx.AND(
				qx.EQ("is_verified", true),
				qx.IN("plan", []string{"pro", "enterprise"}),
				qx.GTE("score", 300.0),
			),
		),
	).By("created_at", qx.DESC).Max(150)
	_, err := db.QueryKeys(q)
	return "read_user_discovery_or_keys", err
}

func runReadUserDeepPaginationKeys(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	_ uint64,
	_ []string,
) (string, error) {
	tags := pickRandomSlice(rng, AllTags, 3)
	if len(tags) < 3 {
		tags = []string{"programming", "security", "gaming"}
	}
	skip := 1000 + rng.IntN(5000)
	q := qx.Query(
		qx.AND(
			qx.HASANY("tags", tags),
			qx.GTE("score", 150.0),
			qx.NOTIN("status", []string{"banned", "suspended"}),
		),
	).By("score", qx.DESC).Skip(skip).Max(100)
	_, err := db.QueryKeys(q)
	return "read_user_deep_pagination_keys", err
}
