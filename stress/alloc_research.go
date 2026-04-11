package main

import (
	"fmt"
	"strings"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi"
)

type researchAllocRunKind uint8

const (
	researchAllocRunItems researchAllocRunKind = iota
	researchAllocRunKeys
	researchAllocRunCount
)

const defaultAllocTurnoverRingSize = 64

var researchAllocClassItems = StressClassInfo{
	ID:    101,
	Alias: "alloc_items",
	Name:  "alloc_items",
	Role:  RoleRead,
}

var researchAllocClassKeys = StressClassInfo{
	ID:    102,
	Alias: "alloc_keys",
	Name:  "alloc_keys",
	Role:  RoleRead,
}

var researchAllocClassCount = StressClassInfo{
	ID:    103,
	Alias: "alloc_count",
	Name:  "alloc_count",
	Role:  RoleRead,
}

type researchAllocQuerySpec struct {
	classInfo StressClassInfo
	queryInfo StressQueryInfo
	runKind   researchAllocRunKind
	build     func(now int64) *qx.QX
}

func defaultResearchAllocQueries() []researchAllocQuerySpec {
	return []researchAllocQuerySpec{
		{
			classInfo: researchAllocClassItems,
			queryInfo: StressQueryInfo{Name: "read_active_region_pro_items", Weight: 1},
			runKind:   researchAllocRunItems,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.EQ("status", "active"),
						qx.GTE("last_login", now-72*3600),
						qx.IN("country", []string{"US", "DE"}),
						qx.NOTIN("plan", []string{"free"}),
					),
				).By("last_login", qx.DESC).Max(40)
			},
		},
		{
			classInfo: researchAllocClassItems,
			queryInfo: StressQueryInfo{Name: "read_leaderboard_top_items", Weight: 1},
			runKind:   researchAllocRunItems,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.EQ("status", "active"),
						qx.GTE("score", 250.0),
						qx.GTE("last_login", now-180*24*3600),
					),
				).By("score", qx.DESC).Max(50)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "read_signup_dashboard_count", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.GTE("created_at", now-21*24*3600),
						qx.EQ("is_verified", true),
						qx.IN("country", []string{"US", "DE"}),
						qx.NOTIN("status", []string{"banned"}),
					),
				)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "read_frontpage_candidate_keys", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.EQ("status", "active"),
						qx.HASANY("tags", []string{"technology", "programming", "databases"}),
						qx.GTE("score", 120.0),
						qx.GTE("last_login", now-45*24*3600),
					),
				).By("score", qx.DESC).Max(100)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "read_moderation_queue_keys", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.EQ("status", "pending"),
						qx.EQ("status", "suspended"),
						qx.AND(
							qx.EQ("status", "active"),
							qx.HASANY("tags", []string{"politics", "cryptocurrency"}),
							qx.GTE("score", 4000.0),
							qx.GTE("created_at", now-180*24*3600),
						),
					),
				).By("created_at", qx.DESC).Max(100)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "read_staff_audit_feed_keys", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.HASANY("roles", []string{"moderator", "admin", "staff", "bot"}),
						qx.GTE("last_login", now-120*24*3600),
						qx.NOTIN("status", []string{"banned"}),
						qx.EQ("country", "US"),
					),
				).By("last_login", qx.DESC).Max(120)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "read_discovery_explore_keys", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.EQ("status", "active"),
							qx.HASANY("tags", []string{"technology", "gaming"}),
							qx.GTE("score", 320.0),
							qx.GTE("last_login", now-30*24*3600),
						),
						qx.AND(
							qx.EQ("country", "US"),
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
				).By("created_at", qx.DESC).Max(150)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "read_dormant_archive_page_keys", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.NOTIN("status", []string{"banned"}),
						qx.LT("last_login", now-180*24*3600),
						qx.NOTIN("plan", []string{"enterprise"}),
						qx.LT("score", 180.0),
					),
				).By("last_login", qx.ASC).Skip(2500).Max(100)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "read_inactive_cleanup_keys", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(now int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.LT("last_login", now-365*24*3600),
						qx.NOTIN("status", []string{"banned"}),
						qx.NOTIN("plan", []string{"pro", "enterprise"}),
						qx.LT("score", 120.0),
					),
				).Max(250)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_realistic_feed_eligible", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.EQ("status", "active"),
					qx.NOTIN("plan", []string{"free"}),
					qx.GTE("score", 120.0),
					qx.HASANY("tags", []string{"go", "security", "ops"}),
				)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_realistic_moderation_queue", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.EQ("status", "trial"),
							qx.HASANY("roles", []string{"moderator", "admin"}),
						),
						qx.AND(
							qx.EQ("status", "paused"),
							qx.GTE("age", 25),
						),
						qx.AND(
							qx.EQ("plan", "enterprise"),
							qx.HASANY("tags", []string{"security", "ops"}),
						),
					),
				)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_realistic_discovery_or", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.PREFIX("email", "user1"),
							qx.EQ("status", "active"),
							qx.GTE("score", 60.0),
						),
						qx.AND(
							qx.EQ("country", "DE"),
							qx.HASANY("tags", []string{"rust", "go"}),
							qx.GTE("age", 24),
						),
						qx.AND(
							qx.EQ("plan", "enterprise"),
							qx.HASANY("roles", []string{"admin", "support"}),
							qx.NOTIN("status", []string{"banned"}),
						),
					),
				)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_realistic_security_audit", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.HASANY("roles", []string{"admin", "support"}),
						qx.NOTIN("status", []string{"banned"}),
						qx.GTE("score", 50.0),
						qx.IN("country", []string{"US", "DE", "GB", "FR"}),
					),
				)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_realistic_cohort_retention", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.AND(
						qx.NOTIN("status", []string{"banned"}),
						qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
						qx.GTE("age", 25),
						qx.LTE("age", 45),
						qx.HASANY("tags", []string{"go", "db", "security"}),
						qx.GTE("score", 80.0),
					),
				)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_gap_broadprefix_mixed", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.PREFIX("email", "user"),
					qx.EQ("status", "active"),
					qx.NOTIN("plan", []string{"free"}),
				)
			},
		},
		{
			classInfo: researchAllocClassCount,
			queryInfo: StressQueryInfo{Name: "count_gap_heavyor_multibranch", Weight: 1},
			runKind:   researchAllocRunCount,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.EQ("country", "DE"),
							qx.HASANY("tags", []string{"rust", "go"}),
							qx.GTE("score", 40.0),
						),
						qx.AND(
							qx.PREFIX("email", "user1"),
							qx.EQ("status", "active"),
						),
						qx.AND(
							qx.EQ("plan", "enterprise"),
							qx.GTE("age", 30),
						),
						qx.AND(
							qx.HASANY("roles", []string{"admin"}),
							qx.NOTIN("status", []string{"banned"}),
						),
						qx.AND(
							qx.CONTAINS("name", "user-1"),
							qx.GTE("score", 20.0),
						),
					),
				)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_heavy_range_order_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.EQ("status", "active"),
					qx.GTE("age", 30),
					qx.LT("age", 50),
				).By("age", qx.ASC).Max(100)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_heavy_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.EQ("country", "DE"),
							qx.EQ("plan", "enterprise"),
							qx.HASANY("tags", []string{"go", "security", "ops"}),
						),
						qx.AND(
							qx.PREFIX("email", "user1"),
							qx.LT("age", 25),
						),
						qx.HASNONE("roles", []string{"admin"}),
					),
				).Max(10)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_realistic_autocomplete_prefix_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(qx.PREFIX("email", "user10")).Max(10)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_realistic_autocomplete_order_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.PREFIX("email", "user10"),
					qx.EQ("status", "active"),
				).By("email", qx.ASC).Max(10)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_realistic_autocomplete_complex_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.PREFIX("email", "user10"),
					qx.EQ("status", "active"),
					qx.NOTIN("plan", []string{"free"}),
				).By("score", qx.DESC).Max(10)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_gap_crm_multibranch_or_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.PREFIX("email", "user1"),
							qx.EQ("status", "active"),
						),
						qx.AND(
							qx.SUFFIX("email", "@example.com"),
							qx.NOTIN("country", []string{"US", "GB"}),
							qx.GTE("score", 50.0),
						),
						qx.AND(
							qx.EQ("plan", "enterprise"),
							qx.HASANY("tags", []string{"security", "ops"}),
						),
					),
				).Max(150)
			},
		},
		{
			classInfo: researchAllocClassKeys,
			queryInfo: StressQueryInfo{Name: "keys_gap_heavyor_order_limit", Weight: 1},
			runKind:   researchAllocRunKeys,
			build: func(_ int64) *qx.QX {
				return qx.Query(
					qx.OR(
						qx.AND(
							qx.EQ("country", "DE"),
							qx.HASANY("tags", []string{"rust", "go"}),
							qx.GTE("score", 40.0),
						),
						qx.AND(
							qx.PREFIX("email", "user1"),
							qx.EQ("status", "active"),
						),
						qx.AND(
							qx.EQ("plan", "enterprise"),
							qx.GTE("age", 30),
						),
					),
				).By("score", qx.DESC).Max(120)
			},
		},
	}
}

func filterResearchAllocQueries(classFilter, queryFilter []string) ([]researchAllocQuerySpec, error) {
	specs := defaultResearchAllocQueries()
	classFilter = dedupeStrings(classFilter)
	queryFilter = dedupeStrings(queryFilter)
	if len(classFilter) == 0 && len(queryFilter) == 0 {
		return specs, nil
	}

	knownClasses := make(map[string]struct{}, 8)
	knownQueries := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		knownClasses[strings.ToLower(spec.classInfo.Name)] = struct{}{}
		knownClasses[strings.ToLower(spec.classInfo.Alias)] = struct{}{}
		knownQueries[strings.ToLower(spec.queryInfo.Name)] = struct{}{}
	}
	for _, name := range classFilter {
		if _, ok := knownClasses[strings.ToLower(name)]; !ok {
			return nil, fmt.Errorf("unknown alloc research class filter %q", name)
		}
	}
	for _, name := range queryFilter {
		if _, ok := knownQueries[strings.ToLower(name)]; !ok {
			return nil, fmt.Errorf("unknown alloc research query filter %q", name)
		}
	}

	classSet := make(map[string]struct{}, len(classFilter))
	for _, name := range classFilter {
		classSet[strings.ToLower(name)] = struct{}{}
	}
	querySet := make(map[string]struct{}, len(queryFilter))
	for _, name := range queryFilter {
		querySet[strings.ToLower(name)] = struct{}{}
	}

	out := make([]researchAllocQuerySpec, 0, len(specs))
	for _, spec := range specs {
		if len(classSet) > 0 {
			if _, ok := classSet[strings.ToLower(spec.classInfo.Name)]; !ok {
				if _, ok := classSet[strings.ToLower(spec.classInfo.Alias)]; !ok {
					continue
				}
			}
		}
		if len(querySet) > 0 {
			if _, ok := querySet[strings.ToLower(spec.queryInfo.Name)]; !ok {
				continue
			}
		}
		out = append(out, spec)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no alloc research queries remain after applying filters")
	}
	return out, nil
}

func runResearchAllocQuery(db *rbi.DB[uint64, UserBench], kind researchAllocRunKind, q *qx.QX) error {
	switch kind {
	case researchAllocRunItems:
		items, err := db.Query(q)
		db.ReleaseRecords(items...)
		return err
	case researchAllocRunKeys:
		_, err := db.QueryKeys(q)
		return err
	case researchAllocRunCount:
		_, err := db.Count(q)
		return err
	default:
		return fmt.Errorf("unknown alloc run kind %d", kind)
	}
}

type allocTurnoverEntry struct {
	id        uint64
	basePatch []rbi.Field
	altPatch  []rbi.Field
	altActive bool
}

type allocTurnoverRing struct {
	next    int
	entries []allocTurnoverEntry
}

var allocTurnoverCountries = [...]string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
var allocTurnoverPlans = [...]string{"free", "basic", "pro", "enterprise"}
var allocTurnoverStatuses = [...]string{"active", "trial", "paused", "banned"}

func buildAllocTurnoverRing(handle *DBHandle, limit int) (*allocTurnoverRing, error) {
	if handle == nil || handle.DB == nil {
		return nil, fmt.Errorf("alloc turnover requires an open DB handle")
	}
	if limit <= 0 {
		limit = defaultAllocTurnoverRingSize
	}
	ords := allocTurnoverSampleOrdinals(int(handle.StartRecords), limit)
	if len(ords) == 0 {
		return nil, fmt.Errorf("alloc turnover ring has no sample ordinals")
	}

	entries := make([]allocTurnoverEntry, 0, len(ords))
	want := 0
	pos := 0
	err := handle.DB.ScanKeys(0, func(id uint64) (bool, error) {
		pos++
		if want >= len(ords) {
			return false, nil
		}
		if pos != ords[want] {
			return true, nil
		}
		rec, getErr := handle.DB.Get(id)
		if getErr != nil {
			return false, getErr
		}
		if rec == nil {
			want++
			return true, nil
		}
		entries = append(entries, allocTurnoverEntry{
			id:        id,
			basePatch: allocBasePatch(rec),
			altPatch:  allocAltPatch(rec),
		})
		handle.DB.ReleaseRecords(rec)
		want++
		return want < len(ords), nil
	})
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("alloc turnover ring is empty")
	}
	return &allocTurnoverRing{entries: entries}, nil
}

func (r *allocTurnoverRing) apply(db *rbi.DB[uint64, UserBench]) error {
	if r == nil || len(r.entries) == 0 {
		return nil
	}
	entry := &r.entries[r.next]
	r.next++
	if r.next == len(r.entries) {
		r.next = 0
	}
	patch := entry.altPatch
	if entry.altActive {
		patch = entry.basePatch
	}
	if err := db.Patch(entry.id, patch); err != nil {
		return err
	}
	entry.altActive = !entry.altActive
	return nil
}

func allocTurnoverSampleOrdinals(total, limit int) []int {
	if total <= 0 || limit <= 0 {
		return nil
	}
	if total < limit {
		limit = total
	}
	out := make([]int, 0, limit)
	last := 0
	for i := 0; i < limit; i++ {
		ord := 1
		if limit > 1 {
			ord = 1 + (i*(total-1))/(limit-1)
		}
		if ord == last {
			continue
		}
		out = append(out, ord)
		last = ord
	}
	return out
}

func allocCloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func allocToggleSliceValue(in []string, value string) []string {
	count := 0
	for _, v := range in {
		if v != value {
			count++
		}
	}
	if count != len(in) {
		out := make([]string, 0, count)
		for _, v := range in {
			if v != value {
				out = append(out, v)
			}
		}
		return out
	}
	out := allocCloneStrings(in)
	out = append(out, value)
	return out
}

func allocNextStringCycle(values []string, cur string) string {
	for i := range values {
		if values[i] == cur {
			return values[(i+1)%len(values)]
		}
	}
	return values[0]
}

func allocToggleInt(cur, minValue, maxValue int) int {
	if cur < maxValue {
		return cur + 1
	}
	if cur > minValue {
		return cur - 1
	}
	return cur
}

func allocToggleFloat(cur, delta, minValue, maxValue float64) float64 {
	next := cur + delta
	if next > maxValue {
		next = cur - delta
	}
	if next < minValue {
		next = cur
	}
	return next
}

func allocBasePatch(rec *UserBench) []rbi.Field {
	return []rbi.Field{
		{Name: "country", Value: rec.Country},
		{Name: "plan", Value: rec.Plan},
		{Name: "status", Value: rec.Status},
		{Name: "age", Value: rec.Age},
		{Name: "score", Value: rec.Score},
		{Name: "tags", Value: allocCloneStrings(rec.Tags)},
		{Name: "roles", Value: allocCloneStrings(rec.Roles)},
	}
}

func allocAltPatch(rec *UserBench) []rbi.Field {
	return []rbi.Field{
		{Name: "country", Value: allocNextStringCycle(allocTurnoverCountries[:], rec.Country)},
		{Name: "plan", Value: allocNextStringCycle(allocTurnoverPlans[:], rec.Plan)},
		{Name: "status", Value: allocNextStringCycle(allocTurnoverStatuses[:], rec.Status)},
		{Name: "age", Value: allocToggleInt(rec.Age, 18, 77)},
		{Name: "score", Value: allocToggleFloat(rec.Score, 3.25, 0, 1000)},
		{Name: "tags", Value: allocToggleSliceValue(rec.Tags, "ops")},
		{Name: "roles", Value: allocToggleSliceValue(rec.Roles, "admin")},
	}
}
