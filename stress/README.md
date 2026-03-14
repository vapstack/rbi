# stress

Interactive and headless DB stress utility for `rbi`.

It opens a benchmark database, starts worker groups by workload class, collects lightweight runtime stats, shows a live table in interactive mode, and writes a final JSON report on exit.
The workload catalog, query mix, seeding helpers, and telemetry code are self-contained in `stress/` and do not depend on `bench/runner`.

If the target DB is empty, the runner seeds it with the standard benchmark dataset.

## Build / Run

Interactive mode:

```bash
go run ./stress -db bench.db -r_smp 32 -w_med 4
```

Headless timed mode:

```bash
go run ./stress -db bench.db -out stress_report.json -duration 30s -r_smp 32 -w_med 4
```

Headless until `Ctrl+C`:

```bash
go run ./stress -db bench.db -headless -out stress_report.json -r_idx 128 -w_fst 16
```

## Main Flags

- `-db` : path to Bolt DB file
- `-out`, `-report` : output JSON report path
- `-duration` : fixed run duration; enables headless mode automatically
- `-headless`, `-no-ui` : disable interactive UI
- `-refresh` : UI/stat counter refresh interval
- `-telemetry` : memory / snapshot / autobatch sampling interval
- `-trace-sample` : planner trace sampling (`-1` disable, `0` every query, `N` every Nth query)
- `-trace-top` : how many slowest sampled planner traces to keep in the report
- `-bolt-no-sync` : open Bolt with `NoSync=true`
- `-analyze-interval` : override `rbi.Options.AnalyzeInterval`

Per-class initial worker counts can be set by alias or full class name.

Aliases:

- `r_idx` = `read_indexed`
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
go run ./stress -db bench.db -r_idx 256 -w_fst 32
go run ./stress -db bench.db -read_simple 64 -write_medium 8
```

## Interactive Commands

In interactive mode, worker counts can be changed at runtime:

- `<id> <count>` : set absolute worker count for class
- `<id>+<n>` : add workers
- `<id>-<n>` : remove workers
- `r <count>` : set all read classes to the same worker count
- `w <count>` : set all write classes to the same worker count
- `a <count>` : set all classes to the same worker count

Examples:

```text
3 10
1+5
7-2
r 1
w 4
a 0
```

After each command, the current stats window is reset and counting starts from zero again.

## Output

The final report is written to `stress_report.json` by default, or to the path given by `-out` / `-report`.

The report contains:

- per-class stats
- per-query stats inside each class
- per-worker stats
- sampled planner diagnostics by class and query
- slowest sampled planner traces with plan / rows examined / scan width / dedupe info
- memory samples
- snapshot samples
- autobatch samples

On `Ctrl+C`, the utility stops workers, writes the final report, runs `debug.FreeOSMemory()`, closes the DB, and exits.
