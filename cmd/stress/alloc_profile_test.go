package main

import (
	"os"
	"testing"
	"time"
)

func TestSelectFocusedAllocTargetRequiresSingleQuery(t *testing.T) {
	_, err := selectFocusedAllocTarget(options{
		AllocSource: allocSourceWorkload,
		ClassFilter: []string{"r_idx"},
	})
	if err == nil {
		t.Fatal("expected error for class with multiple queries")
	}
}

func TestSelectFocusedAllocTargetSingleWorkloadQuery(t *testing.T) {
	target, err := selectFocusedAllocTarget(options{
		AllocSource: allocSourceWorkload,
		ClassFilter: []string{"r_med"},
		QueryFilter: []string{"read_signup_dashboard_count"},
	})
	if err != nil {
		t.Fatalf("selectFocusedAllocTarget: %v", err)
	}
	if target.classInfo.Name != ClassReadMedium {
		t.Fatalf("class name = %q, want %q", target.classInfo.Name, ClassReadMedium)
	}
	if target.queryInfo.Name != "read_signup_dashboard_count" {
		t.Fatalf("query name = %q, want read_signup_dashboard_count", target.queryInfo.Name)
	}
	if target.prepare == nil {
		t.Fatal("target prepare = nil, want prepare function")
	}
}

func TestSelectFocusedAllocTargetSinglePreparedQuery(t *testing.T) {
	target, err := selectFocusedAllocTarget(options{
		AllocSource: allocSourcePrepared,
		ClassFilter: []string{"alloc_count"},
		QueryFilter: []string{"count_realistic_discovery_or"},
	})
	if err != nil {
		t.Fatalf("selectFocusedAllocTarget(prepared): %v", err)
	}
	if target.classInfo.Name != "alloc_count" {
		t.Fatalf("class name = %q, want alloc_count", target.classInfo.Name)
	}
	if target.queryInfo.Name != "count_realistic_discovery_or" {
		t.Fatalf("query name = %q, want count_realistic_discovery_or", target.queryInfo.Name)
	}
	if target.prepare == nil {
		t.Fatal("target prepare = nil, want prepare function")
	}
}

func TestSelectFocusedAllocTargetSinglePreparedKeysHeavyLimit(t *testing.T) {
	target, err := selectFocusedAllocTarget(options{
		AllocSource: allocSourcePrepared,
		ClassFilter: []string{"alloc_keys"},
		QueryFilter: []string{"keys_heavy_limit"},
	})
	if err != nil {
		t.Fatalf("selectFocusedAllocTarget(prepared keys): %v", err)
	}
	if target.classInfo.Name != "alloc_keys" {
		t.Fatalf("class name = %q, want alloc_keys", target.classInfo.Name)
	}
	if target.queryInfo.Name != "keys_heavy_limit" {
		t.Fatalf("query name = %q, want keys_heavy_limit", target.queryInfo.Name)
	}
	if target.prepare == nil {
		t.Fatal("target prepare = nil, want prepare function")
	}
}

func TestParseOptionsAllocProfileEnablesHeadless(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-alloc-profile", "/tmp/focused.allocs.pprof",
		"-duration", "12s",
		"-alloc-source", "prepared",
		"-alloc-mode", "turnover",
		"-alloc-scope", "query",
		"-alloc-ops", "40",
		"-alloc-warmup-ops", "32",
		"-alloc-turnover-ring", "48",
		"-alloc-memrate", "1",
		"-class", "alloc_count",
		"-query", "count_realistic_discovery_or",
	}

	opts, err := parseOptions(catalog)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if !opts.Headless {
		t.Fatal("Headless = false, want true")
	}
	if opts.AllocProfile != "/tmp/focused.allocs.pprof" {
		t.Fatalf("AllocProfile = %q, want /tmp/focused.allocs.pprof", opts.AllocProfile)
	}
	if opts.Duration != 12*time.Second {
		t.Fatalf("Duration = %s, want 12s", opts.Duration)
	}
	if opts.AllocSource != allocSourcePrepared {
		t.Fatalf("AllocSource = %q, want %q", opts.AllocSource, allocSourcePrepared)
	}
	if opts.AllocMode != allocModeTurnover {
		t.Fatalf("AllocMode = %q, want %q", opts.AllocMode, allocModeTurnover)
	}
	if opts.AllocScope != allocScopeQuery {
		t.Fatalf("AllocScope = %q, want %q", opts.AllocScope, allocScopeQuery)
	}
	if opts.AllocWarmupOps != 32 {
		t.Fatalf("AllocWarmupOps = %d, want 32", opts.AllocWarmupOps)
	}
	if opts.AllocOps != 40 {
		t.Fatalf("AllocOps = %d, want 40", opts.AllocOps)
	}
	if opts.AllocTurnoverRing != 48 {
		t.Fatalf("AllocTurnoverRing = %d, want 48", opts.AllocTurnoverRing)
	}
	if opts.AllocMemProfileRate != 1 {
		t.Fatalf("AllocMemProfileRate = %d, want 1", opts.AllocMemProfileRate)
	}
}

func TestParseOptionsAllocProfileValidatesScope(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-alloc-profile", "/tmp/focused.allocs.pprof",
		"-duration", "2s",
		"-alloc-scope", "bogus",
		"-class", "alloc_count",
		"-query", "count_realistic_discovery_or",
	}

	if _, err := parseOptions(catalog); err == nil {
		t.Fatal("expected alloc-scope validation error")
	}
}

func TestParseOptionsAllocProfileRequiresDuration(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-alloc-profile", "/tmp/focused.allocs.pprof",
		"-class", "alloc_count",
		"-query", "count_realistic_discovery_or",
	}

	if _, err := parseOptions(catalog); err == nil {
		t.Fatal("expected duration/alloc-ops validation error for alloc-profile")
	}
}

func TestParseOptionsAllocProfileAllowsAllocOpsWithoutDuration(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-alloc-profile", "/tmp/focused.allocs.pprof",
		"-alloc-ops", "25",
		"-class", "alloc_count",
		"-query", "count_realistic_discovery_or",
	}

	opts, err := parseOptions(catalog)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.AllocOps != 25 {
		t.Fatalf("AllocOps = %d, want 25", opts.AllocOps)
	}
}

func TestParseOptionsAllocProfileValidatesTurnoverRing(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-alloc-profile", "/tmp/focused.allocs.pprof",
		"-duration", "2s",
		"-alloc-mode", "turnover",
		"-alloc-turnover-ring", "0",
		"-class", "alloc_keys",
		"-query", "keys_realistic_autocomplete_prefix_limit",
	}

	if _, err := parseOptions(catalog); err == nil {
		t.Fatal("expected alloc-turnover-ring validation error")
	}
}
