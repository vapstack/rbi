# stress

Interactive and headless DB stress utility for `rbi`.

It opens a benchmark database, starts worker groups by workload class, 
collects lightweight runtime stats, shows a live table in interactive mode, 
and writes a final JSON report on exit.

If the target DB is empty, the runner seeds it with the standard benchmark dataset.

## Build / Run

Interactive mode:

```bash
go run ./stress -db bench.db
```

Headless timed mode:

```bash
go run ./stress -db bench.db -out stress_report.json -duration 30s -r_smp 8 -w_med 4
```

Focused profiling run for one problematic query:

```bash
go run ./stress \
  -db stress/stress.db \
  -headless \
  -duration 20s \
  -class r_med \
  -query read_signup_dashboard_count \
  -r_med 32 \
  -cpu-profile /tmp/stress-count.cpu.pprof \
  -heap-profile /tmp/stress-count.heap.pprof
```

Focused alloc-profile run for one exact query class:

```bash
go run ./stress \
  -db stress/stress.db \
  -alloc-profile /tmp/discovery.allocs.pprof \
  -alloc-ops 64 \
  -alloc-source prepared \
  -alloc-mode turnover \
  -alloc-scope query \
  -alloc-warmup-ops 64 \
  -class alloc_count \
  -query count_realistic_discovery_or
```

Focused alloc-profile on the original stochastic stress workload path:

```bash
go run ./stress \
  -db stress/stress.db \
  -alloc-profile /tmp/discovery-workload.allocs.pprof \
  -duration 15s \
  -alloc-source workload \
  -class r_hvy \
  -query read_discovery_explore_keys
```

Headless until `Ctrl+C`:

```bash
go run ./stress -db bench.db -headless -out stress_report.json -r_idx 128 -w_fst 16
```

## Main Flags

- `-db` : path to Bolt DB file
- `-seed-records` : when set, ensure the DB contains at least this many records before starting
- `-out`, `-report` : output JSON report path
- `-cpu-profile` : write CPU profile for the run
- `-heap-profile` : write heap profile at shutdown, before `FreeOSMemory`
- `-alloc-profile` : run one focused filtered query and write an `allocs` profile
- `-alloc-source` : focused alloc source, `prepared` or `workload`
- `-alloc-mode` : focused alloc execution mode, `hot` or `turnover`
- `-alloc-scope` : focused alloc scope, `full` or `query`
- `-pprof-http` : expose `net/http/pprof`
- `-duration` : fixed run duration; enables headless mode automatically
- `-headless`, `-no-ui` : disable interactive UI
- `-alloc-warmup-ops` : warmup query ops before focused alloc profiling
- `-alloc-ops` : fixed measured op count for focused alloc profiling
- `-alloc-memrate` : `runtime.MemProfileRate` during focused alloc profiling
- `-alloc-turnover-ring` : turnover ring size for prepared alloc profiling
- `-no-cache` : disable rbi runtime caches and numeric range bucket acceleration
- interactive mode shows per-query counters/TPS/latency by default
- `-query-stats` : enable the same per-query breakdowns/latency collector in headless runs and
  reports
- `-class` : keep only specific classes by alias/name, comma-separated
- `-query` : keep only specific query names, comma-separated
- `-refresh` : UI/stat counter refresh interval
- `-telemetry` : memory / snapshot / autobatch sampling interval
- `-trace-sample` : planner trace sampling (`-1` disable, `0` every query, `N` every Nth query);
  default is off
- `-trace-top` : how many slowest sampled planner traces to keep in the report
- `-bolt-no-sync` : open Bolt with `NoSync=true`
- `-analyze-interval` : override `rbi.Options.AnalyzeInterval`
- `-a` : set the same initial worker count for all classes
- `-r` : set the same initial worker count for all read classes
- `-w` : set the same initial worker count for all write classes

Per-class initial worker counts can be set by alias or full class name. Group flags are resolved
after class/query filters:
`-a` applies first, then `-r` / `-w`, and per-class flags override group values.

Aliases:

- `r_idx` = `read_indexed` (point lookup)
- `r_smp` = `read_simple`
- `r_med` = `read_medium`
- `r_meh` = `read_medium_heavy`
- `r_hvy` = `read_heavy`
- `w_fst` = `write_fast`
- `w_smp` = `write_simple`
- `w_med` = `write_medium`
- `w_hvy` = `write_heavy`

Examples:

```bash
go run ./stress -db bench.db -r 4 -w 2
go run ./stress -db bench.db -a 2 -r 24 -w_hvy 1
go run ./stress -db bench.db -r_idx 8 -w_fst 4
go run ./stress -db bench.db -read_simple 8 -write_medium 4
```

## Interactive Commands

In interactive mode, worker counts can be changed at runtime:

- `<id> <count>` : set absolute worker count for class
- `<id>+<n>` : add workers
- `<id>-<n>` : remove workers
- `r <count>` : set all read classes to the same worker count
- `w <count>` : set all write classes to the same worker count
- `a <count>` : set all classes to the same worker count

After each command, the current stats window is reset and counting starts from zero again.
The final JSON report now also stores these windows as `phases`: the initial phase plus one phase
per manual command, with per-phase class/query/totals and telemetry samples.

## Output

The final report is written to `stress_report.json` by default, or to the path given by `-out` /
`-report`.

The report contains:

- per-class stats
- per-query stats inside each class in interactive mode by default
- per-query latency breakdowns in reports when `-query-stats` is enabled
- active class/query filters used for the run
- per-worker stats
- sampled planner diagnostics by class and query
- slowest sampled planner traces with plan / rows examined / scan width / dedupe info
- memory samples
- snapshot samples
- autobatch samples
- final `index_stats` with per-field posting bytes, key bytes, layout bytes, and approximate heap totals
- `phases`: archived stats windows between manual interactive commands

On `Ctrl+C`, the utility stops workers, writes the final report, runs `debug.FreeOSMemory()`, 
closes the DB, and exits.

Only one `stress` process should target a given DB file at a time. `report saved` is not the end of
DB ownership: the process still has to release OS memory and finish `DB.Close()`. The stress helper
uses a fixed short open timeout internally to catch accidental parallel runs; do not start another
`stress`/`bench` process until the prior one has fully exited and released the DB.

`-alloc-profile` is a separate focused mode. It requires filters that leave exactly one class and one
query, performs warmup outside the measured phase, enables `runtime.MemProfileRate` only for the
measured window, and writes `pprof` `allocs` data for that window. Use `-alloc-ops` when you need
hot/turnover profiles to be directly comparable per operation.

`-alloc-source=prepared` runs benchmark-like prepared queries that are built once outside the
measured loop, so the profile reflects engine execution rather than `qx.Query(...)` construction.
Prepared research classes are:

- `alloc_items`
- `alloc_keys`
- `alloc_count`

Prepared query names include both stress-aligned and benchmark-aligned scenarios, for example:

- `read_discovery_explore_keys`
- `read_frontpage_candidate_keys`
- `count_realistic_discovery_or`
- `count_gap_heavyor_multibranch`
- `keys_heavy_limit`
- `keys_realistic_autocomplete_prefix_limit`
- `keys_gap_crm_multibranch_or_limit`

`-alloc-mode=turnover` applies one benchmark-like patch before each profiled op.

`-alloc-scope=query` excludes the turnover patch itself from the `allocs` profile and records only
the reader/query phase after each patch. `-alloc-scope=full` profiles the whole cold cycle,
including turnover/publish work.
