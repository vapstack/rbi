package main

import (
	"fmt"
	"strings"
)

type stressQuerySpec struct {
	info StressQueryInfo
	run  WorkloadFunc
}

type stressClassSpec struct {
	info    StressClassInfo
	run     WorkloadFunc
	queries []stressQuerySpec
}

func DefaultStressClassCatalog() []StressClassInfo {
	specs := defaultStressClassSpecs()
	out := make([]StressClassInfo, 0, len(specs))
	for _, spec := range specs {
		info := spec.info
		if len(info.Queries) > 0 {
			info.Queries = append([]StressQueryInfo(nil), info.Queries...)
		}
		out = append(out, info)
	}
	return out
}

func DefaultClassDefs() map[string]ClassDef {
	specs := defaultStressClassSpecs()
	out := make(map[string]ClassDef, len(specs))
	for _, spec := range specs {
		out[spec.info.Name] = buildClassDef(spec)
	}
	return out
}

func buildClassDef(spec stressClassSpec) ClassDef {
	run := spec.run
	if len(spec.queries) > 0 {
		variants := make([]weightedWorkload, 0, len(spec.queries))
		for _, query := range spec.queries {
			variants = append(variants, weightedWorkload{
				Weight: query.info.Weight,
				Run:    query.run,
			})
		}
		run = runWeighted(nil, variants...)
	}
	return ClassDef{
		Name:    spec.info.Name,
		Role:    spec.info.Role,
		Workers: spec.info.DefaultWorkers,
		Run:     run,
	}
}

func defaultStressClassSpecs() []stressClassSpec {
	return []stressClassSpec{
		{
			info: StressClassInfo{
				ID:             1,
				Alias:          "r_idx",
				Name:           ClassReadIndexed,
				Role:           RoleRead,
				DefaultWorkers: 512,
				Queries: []StressQueryInfo{
					{Name: "read_profile_by_id_items", Weight: 0.65},
					{Name: "read_account_by_email_items", Weight: 0.35},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "read_profile_by_id_items", Weight: 0.65},
					run:  runReadProfileByIDItems,
				},
				{
					info: StressQueryInfo{Name: "read_account_by_email_items", Weight: 0.35},
					run:  runReadAccountByEmailItems,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             2,
				Alias:          "r_smp",
				Name:           ClassReadSimple,
				Role:           RoleRead,
				DefaultWorkers: 128,
				Queries: []StressQueryInfo{
					{Name: "read_member_directory_prefix_items", Weight: 0.55},
					{Name: "read_recent_country_active_items", Weight: 0.45},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "read_member_directory_prefix_items", Weight: 0.55},
					run:  runReadMemberDirectoryPrefixItems,
				},
				{
					info: StressQueryInfo{Name: "read_recent_country_active_items", Weight: 0.45},
					run:  runReadRecentCountryActiveItems,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             3,
				Alias:          "r_med",
				Name:           ClassReadMedium,
				Role:           RoleRead,
				DefaultWorkers: 96,
				Queries: []StressQueryInfo{
					{Name: "read_active_region_pro_items", Weight: 0.40},
					{Name: "read_leaderboard_top_items", Weight: 0.35},
					{Name: "read_signup_dashboard_count", Weight: 0.25},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "read_active_region_pro_items", Weight: 0.40},
					run:  runReadActiveRegionProItems,
				},
				{
					info: StressQueryInfo{Name: "read_leaderboard_top_items", Weight: 0.35},
					run:  runReadLeaderboardTopItems,
				},
				{
					info: StressQueryInfo{Name: "read_signup_dashboard_count", Weight: 0.25},
					run:  runReadSignupDashboardCount,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             4,
				Alias:          "r_meh",
				Name:           ClassReadMedHeavy,
				Role:           RoleRead,
				DefaultWorkers: 48,
				Queries: []StressQueryInfo{
					{Name: "read_frontpage_candidate_keys", Weight: 0.40},
					{Name: "read_moderation_queue_keys", Weight: 0.35},
					{Name: "read_staff_audit_feed_keys", Weight: 0.25},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "read_frontpage_candidate_keys", Weight: 0.40},
					run:  runReadFrontpageCandidateKeys,
				},
				{
					info: StressQueryInfo{Name: "read_moderation_queue_keys", Weight: 0.35},
					run:  runReadModerationQueueKeys,
				},
				{
					info: StressQueryInfo{Name: "read_staff_audit_feed_keys", Weight: 0.25},
					run:  runReadStaffAuditFeedKeys,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             5,
				Alias:          "r_hvy",
				Name:           ClassReadHeavy,
				Role:           RoleRead,
				DefaultWorkers: 24,
				Queries: []StressQueryInfo{
					{Name: "read_discovery_explore_keys", Weight: 0.45},
					{Name: "read_dormant_archive_page_keys", Weight: 0.35},
					{Name: "read_inactive_cleanup_keys", Weight: 0.20},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "read_discovery_explore_keys", Weight: 0.45},
					run:  runReadDiscoveryExploreKeys,
				},
				{
					info: StressQueryInfo{Name: "read_dormant_archive_page_keys", Weight: 0.35},
					run:  runReadDormantArchivePageKeys,
				},
				{
					info: StressQueryInfo{Name: "read_inactive_cleanup_keys", Weight: 0.20},
					run:  runReadInactiveCleanupKeys,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             6,
				Alias:          "w_fst",
				Name:           ClassWriteFast,
				Role:           RoleWrite,
				DefaultWorkers: 128,
			},
			run: runWriteTouchLastSeen,
		},
		{
			info: StressClassInfo{
				ID:             7,
				Alias:          "w_smp",
				Name:           ClassWriteSimple,
				Role:           RoleWrite,
				DefaultWorkers: 64,
				Queries: []StressQueryInfo{
					{Name: "write_profile_edit", Weight: 0.65},
					{Name: "write_vote_karma", Weight: 0.35},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "write_profile_edit", Weight: 0.65},
					run:  runWriteProfileEdit,
				},
				{
					info: StressQueryInfo{Name: "write_vote_karma", Weight: 0.35},
					run:  runWriteVoteKarma,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             8,
				Alias:          "w_med",
				Name:           ClassWriteMedium,
				Role:           RoleWrite,
				DefaultWorkers: 48,
				Queries: []StressQueryInfo{
					{Name: "write_bulk_patch", Weight: 0.70},
					{Name: "write_moderation_action", Weight: 0.30},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "write_bulk_patch", Weight: 0.70},
					run:  runWriteBulkPatch,
				},
				{
					info: StressQueryInfo{Name: "write_moderation_action", Weight: 0.30},
					run:  runWriteModerationAction,
				},
			},
		},
		{
			info: StressClassInfo{
				ID:             9,
				Alias:          "w_hvy",
				Name:           ClassWriteHeavy,
				Role:           RoleWrite,
				DefaultWorkers: 24,
				Queries: []StressQueryInfo{
					{Name: "write_full_set_existing", Weight: 0.55},
					{Name: "write_insert_signup", Weight: 0.45},
				},
			},
			queries: []stressQuerySpec{
				{
					info: StressQueryInfo{Name: "write_full_set_existing", Weight: 0.55},
					run:  runWriteFullSetExisting,
				},
				{
					info: StressQueryInfo{Name: "write_insert_signup", Weight: 0.45},
					run:  runWriteInsert,
				},
			},
		},
	}
}

func validateClassCatalog() error {
	catalog := DefaultStressClassCatalog()
	defs := DefaultClassDefs()
	for _, info := range catalog {
		if _, ok := defs[info.Name]; !ok {
			return fmt.Errorf("missing class def for %q", info.Name)
		}
	}
	return nil
}

func filterStressClassSpecs(specs []stressClassSpec, classFilter, queryFilter []string) ([]stressClassSpec, error) {
	classFilter = dedupeStrings(classFilter)
	queryFilter = dedupeStrings(queryFilter)
	if len(classFilter) == 0 && len(queryFilter) == 0 {
		return specs, nil
	}

	if err := validateStressFilterNames(specs, classFilter, queryFilter); err != nil {
		return nil, err
	}

	classSet := make(map[string]struct{}, len(classFilter))
	for _, name := range classFilter {
		classSet[strings.ToLower(name)] = struct{}{}
	}
	querySet := make(map[string]struct{}, len(queryFilter))
	for _, name := range queryFilter {
		querySet[strings.ToLower(name)] = struct{}{}
	}

	filtered := make([]stressClassSpec, 0, len(specs))
	for _, spec := range specs {
		if len(classSet) > 0 {
			if _, ok := classSet[strings.ToLower(spec.info.Name)]; !ok {
				if _, ok := classSet[strings.ToLower(spec.info.Alias)]; !ok {
					continue
				}
			}
		}
		if len(querySet) == 0 {
			filtered = append(filtered, spec)
			continue
		}
		if len(spec.queries) == 0 {
			continue
		}
		keptQueries := make([]stressQuerySpec, 0, len(spec.queries))
		totalWeight := 0.0
		for _, query := range spec.queries {
			if _, ok := querySet[strings.ToLower(query.info.Name)]; !ok {
				continue
			}
			keptQueries = append(keptQueries, query)
			totalWeight += query.info.Weight
		}
		if len(keptQueries) == 0 {
			continue
		}
		if totalWeight <= 0 {
			totalWeight = float64(len(keptQueries))
			for i := range keptQueries {
				keptQueries[i].info.Weight = 1
			}
		}
		infoQueries := make([]StressQueryInfo, 0, len(keptQueries))
		for i := range keptQueries {
			keptQueries[i].info.Weight = keptQueries[i].info.Weight / totalWeight
			infoQueries = append(infoQueries, keptQueries[i].info)
		}
		spec.info.Queries = infoQueries
		spec.queries = keptQueries
		filtered = append(filtered, spec)
	}

	if len(filtered) == 0 {
		return nil, fmt.Errorf("no classes remain after applying stress filters")
	}
	return filtered, nil
}

func validateStressFilterNames(specs []stressClassSpec, classFilter, queryFilter []string) error {
	if len(classFilter) > 0 {
		knownClasses := make(map[string]struct{}, len(specs)*2)
		for _, spec := range specs {
			knownClasses[strings.ToLower(spec.info.Name)] = struct{}{}
			knownClasses[strings.ToLower(spec.info.Alias)] = struct{}{}
		}
		for _, name := range classFilter {
			if _, ok := knownClasses[strings.ToLower(name)]; ok {
				continue
			}
			return fmt.Errorf("unknown stress class filter %q", name)
		}
	}
	if len(queryFilter) > 0 {
		knownQueries := make(map[string]struct{}, 32)
		for _, spec := range specs {
			for _, query := range spec.queries {
				knownQueries[strings.ToLower(query.info.Name)] = struct{}{}
			}
		}
		for _, name := range queryFilter {
			if _, ok := knownQueries[strings.ToLower(name)]; ok {
				continue
			}
			return fmt.Errorf("unknown stress query filter %q", name)
		}
	}
	return nil
}
