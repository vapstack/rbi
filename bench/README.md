## Bench / Stress

Exactly one mode flag is required:

- Matrix mixed profile sweep\
  `go run ./bench -matrix`
- Read-only per-query sweep\
  `go run ./bench -sweep`
- Dedicated read/write stress (closed-loop workers)\
  `go run ./bench -stress`
- Ceiling / saturation studies (open-loop dispatch + per-class queues)\
  `go run ./bench -ceiling`

### Flags

- `-matrix` — run matrix mode (profiles x worker grid)
- `-sweep` — run read-only sweep mode
- `-stress` — run dedicated read/write stress mode
- `-ceiling` — run ceiling mode with class dispatchers/queues

- `-case-duration` — case duration for `matrix` and `stress`
- `-sweep-duration` — case duration for `sweep`
- `-ceiling-stage-duration` — stage duration for `ceiling`
- `-report-every` — progress log interval
- `-out` — output JSON report path
- `-email-sample` — number of sampled emails for single-record read scenario

- `-stress-no-read` — disable read workers in `stress`
- `-stress-no-write` — disable write workers in `stress`

- `-ceiling-dispatch-quantum` — dispatcher quantum for offered load generation
- `-ceiling-queue-cap` — per-class queue capacity
- `-ceiling-suites` — `all` or list: `read_fast_ceiling,write_update_ceiling,write_insert_ceiling,read_fast_under_write,slow_vs_fast,mixed_fast_slow_write,low_write_regression`
- `-ceiling-read-fast-grid` — target ops/s grid for fast-read ceiling
- `-ceiling-write-grid` — target ops/s grid for write ceiling/interference suites
- `-ceiling-read-slow-grid` — target ops/s grid for slow-read interference
- `-ceiling-low-write-grid` — low-write grid for early-regression sweep
- `-ceiling-read-fast-workers` / `-ceiling-read-slow-workers` — executors for read classes
- `-ceiling-write-update-workers` / `-ceiling-write-insert-workers` — executors for write classes
- `-ceiling-mixed-slow-ops` — fixed slow-read offered load in mixed suite
- `-ceiling-baseline-fraction` — fraction of discovered fast-read ceiling used as baseline
- `-ceiling-min-throughput-ratio` — completed/offered saturation threshold
- `-ceiling-regress-throughput-drop-pct` — fast-read throughput drop threshold for regression
- `-ceiling-regress-p99-increase-pct` — fast-read p99 increase threshold for regression

`ceiling` report includes `ceiling.summary` with detected knee/saturation/regression stages and safe targets.

- `-disable-indexing` — disable in-memory index updates on write path (diagnostic)
- `-bolt-no-sync` — set bbolt `NoSync=true` (unsafe diagnostic mode)
- `-snapshot-diagnostics` — print snapshot/compactor diagnostics in progress logs
- `-analyze-interval` — planner analyze interval (`0` = default, `<0` = disable)

- `-snapshot-registry-max` — override `Options.SnapshotRegistryMax`
- `-snapshot-delta-compact-field-keys` — override `Options.SnapshotDeltaCompactFieldKeys`
- `-snapshot-delta-compact-field-ops` — override `Options.SnapshotDeltaCompactFieldOps`
- `-snapshot-delta-compact-max-fields-per-publish` — override `Options.SnapshotDeltaCompactMaxFieldsPerPublish`
- `-snapshot-delta-compact-universe-ops` — override `Options.SnapshotDeltaCompactUniverseOps`
- `-snapshot-delta-layer-max-depth` — override `Options.SnapshotDeltaLayerMaxDepth`
