package main

import "testing"

func TestWorkerMetricsCountsAndSnapshot(t *testing.T) {
	m := newWorkerMetrics(32, 16, 1, true, true)
	m.observe("q1", 1000, 0)
	m.observe("q1", 2000, 1)
	m.observe("q2", 3000, 0)

	total, errors := m.counts()
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	if errors != 1 {
		t.Fatalf("errors = %d, want 1", errors)
	}

	snap := m.snapshot(true)
	if snap.total != 3 {
		t.Fatalf("snapshot total = %d, want 3", snap.total)
	}
	if snap.errors != 1 {
		t.Fatalf("snapshot errors = %d, want 1", snap.errors)
	}
	if snap.lastQuery != "q2" {
		t.Fatalf("lastQuery = %q, want q2", snap.lastQuery)
	}
	if got := snap.queries["q1"].count; got != 2 {
		t.Fatalf("q1 count = %d, want 2", got)
	}
	if got := snap.queries["q1"].errors; got != 1 {
		t.Fatalf("q1 errors = %d, want 1", got)
	}
	if got := snap.queries["q2"].count; got != 1 {
		t.Fatalf("q2 count = %d, want 1", got)
	}
	if len(snap.latency) != 3 {
		t.Fatalf("latency sample size = %d, want 3", len(snap.latency))
	}
}

func TestWorkerMetricsWithoutQueryTrackingSkipsPerQueryState(t *testing.T) {
	m := newWorkerMetrics(32, 16, 1, false, false)
	m.observe("q1", 1000, 0)
	m.observe("q2", 2000, 1)

	snap := m.snapshot(true)
	if snap.total != 2 {
		t.Fatalf("snapshot total = %d, want 2", snap.total)
	}
	if snap.errors != 1 {
		t.Fatalf("snapshot errors = %d, want 1", snap.errors)
	}
	if len(snap.queries) != 0 {
		t.Fatalf("queries = %v, want none", snap.queries)
	}
}

func TestWorkerMetricsWithoutQueryLatencyKeepsCountsOnly(t *testing.T) {
	m := newWorkerMetrics(32, 16, 1, true, false)
	m.observe("q1", 1000, 0)
	m.observe("q1", 2000, 1)

	snap := m.snapshot(true)
	if got := snap.queries["q1"].count; got != 2 {
		t.Fatalf("q1 count = %d, want 2", got)
	}
	if got := snap.queries["q1"].errors; got != 1 {
		t.Fatalf("q1 errors = %d, want 1", got)
	}
	if got := len(snap.queries["q1"].latency); got != 0 {
		t.Fatalf("q1 latency sample size = %d, want 0", got)
	}
}
