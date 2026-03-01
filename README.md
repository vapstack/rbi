# rbi

[![GoDoc](https://pkg.go.dev/badge/github.com/vapstack/rbi.svg)](https://pkg.go.dev/github.com/vapstack/rbi)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/rbi/master/LICENSE)

> This package should be considered experimental.

## Roaring Bolt Indexer

A secondary index layer for [bbolt](https://github.com/etcd-io/bbolt).

It turns a key-value store into a document-oriented database with rich
query capabilities, while preserving bbolt’s ACID guarantees for data storage.
Indexes are kept fully in memory and built on top of
[roaring64](https://github.com/RoaringBitmap/roaring) for fast set operations.

### Properties

* **ACID** – data durability is delegated to bbolt.
* **Index-only filtering** – disk is never touched.
* **Document-oriented** – queries return whole records, not individual fields.
* **Strong typing** – generic API with user-defined key and value types.

### Features

* Automatic indexing of exported struct fields
* Fine-grained control via struct tags (`db`, `dbi`, `rbi`)
* Efficient query building via [qx package](https://github.com/vapstack/qx):
    - comparisons: `EQ`, `GT`, `GTE`, `LT`, `LTE`
    - slices: `IN`, `HAS`, `HASANY`
    - strings: `PREFIX`, `SUFFIX`, `CONTAINS`
    - logical: `AND`, `OR`, `NOT`
* Index-based ordering with offset / limit
* Partial updates (`Patch*`) with minimal index churn
* Batch writes (`BatchSet`, `BatchPatch`, `BatchDelete`)
* Uniqueness constraints

### LLM notice
Starting from v0.7, parts of the code, documentation and tests were created 
or improved with the assistance of LLM. Code created with LLM has been tested 
and verified, but may still contain inefficiencies.

## Usage

```go
package main

import (
    "fmt"

    "github.com/vapstack/qx"
    "github.com/vapstack/rbi"
    "go.etcd.io/bbolt"
)

type User struct {
    ID      uint64   `db:"id"`
    Name    string   `db:"name"`
    Age     int      `db:"age"`
    Active  bool     `db:"active"`
    Tags    []string `db:"tags"`
    Meta    string   `rbi:"-"` // not indexed
    Exclude string   `db:"-"`  // not indexed
}

func main() {

    bolt, err := bbolt.Open("test.db", 0600, nil)
    if err != nil {
        panic(err)
    }
    db, err := rbi.New[uint64, User](bolt, nil)
    if err != nil {
        _ = bolt.Close()
        panic(err)
    }
    defer db.Close()
    defer bolt.Close()

    err = db.Set(1, &User{
        Name:   "Alice",
        Age:    30,
        Active: true,
        Tags:   []string{"admin", "dev"},
    })
    if err != nil {
        panic(err)
    }

    err = db.Set(2, &User{
        ID:     2,
        Name:   "Bob",
        Age:    40,
        Active: false,
        Tags:   []string{"dev"},
    })
    if err != nil {
        panic(err)
    }

    q := qx.Query(
        qx.OR(
            qx.AND(
                qx.EQ("active", true),
                qx.HAS("tags", []string{"dev"}),
            ),
            qx.GT("age", 35),
        ),
    ).
        By("age", qx.ASC).
        Max(10)

    users, err := db.QueryItems(q)
    if err != nil {
        panic(err)
    }

    for _, u := range users {
        fmt.Printf("%v (%v)\n", u.Name, u.Age)
    }
}
```

## API

For the full API reference see
[GoDoc](https://pkg.go.dev/github.com/vapstack/rbi).

### Writing data

* `Set(id, value)` – insert or replace a record and update affected indexes.
* `BatchSet(ids, values)` – batch variant of `Set`, significantly faster for bulk inserts.
* `Patch(id, fields)` – apply partial updates and update only changed indexes.
* `PatchStrict(id, fields)` – like `Patch`, but fails on unknown fields.
* `PatchIfExists(id, fields)` / `PatchStrictIfExists(id, fields)` – patch only existing records.
* `BatchPatch(ids, fields)` / `BatchPatchStrict(ids, fields)` – batch patch variants.
* `Delete(id)` – remove a record and its index entries.
* `BatchDelete(ids)` – batch variant of `Delete`.

### Querying

Queries are constructed using the [`qx`](https://github.com/vapstack/qx) package.

Field names refer to the names specified in `db` tags.\
If a field does not have a `db` tag, the Go struct field name is used.

```go
q := qx.Query(
    qx.EQ("field", val),
    qx.IN("department", []string{"it", "management"}),
    qx.HASANY("tags", []string{"go", "java"}),
    qx.OR(
        qx.PREFIX("name", "A"),
        qx.GT("age", 50),
        qx.LTE("score", 99.5),
    ),
).
    By("field", qx.DESC).
    Skip(10).
    Max(50)
```

Query methods:

* `QueryItems(q)` – return matching records
* `QueryKeys(q)` – return matching IDs
* `Count(q)` – return result cardinality (ignoring offset/limit)

## Query execution model

Queries run entirely in-memory; stored records are never scanned.

The runtime uses a single planner/executor pipeline:
1. Normalize query tree into a deterministic internal form.
2. Compile leaf predicates into bitmap/iterator-backed checks.
3. Select an execution strategy by shape and cost.
4. Execute using shared iterator/bitmap contracts and tracing hooks.

Leaf predicates are resolved via field indexes, producing either bitmaps
of matching record IDs or index-backed iterators.
Logical operators (`AND`, `OR`, `NOT`) are applied using bitmap operations;
large result sets may be represented as negative sets to avoid materializing
large bitmaps.

For ordered queries, the ordered field index is traversed directly and
intersected with compiled predicates. Offset and limit are applied during
traversal when possible.

For limit-driven candidate plans, a selective index yields candidate IDs,
remaining predicates are checked via index lookups, and execution stops once
enough results are collected.

Only the final set of matching record IDs is materialized.
For `QueryItems`, record values are fetched from bbolt only for IDs that have
passed all filters and limits.

`QueryItems` runs against an index snapshot aligned with a bbolt read transaction.
When an exact snapshot for transaction is not immediately available,
it uses bounded waiting with a few fallbacks.
If none of the available paths can provide a valid snapshot within the retry
budget, `QueryItems` returns an error.
Retry budget is `30 * SnapshotPinWaitTimeout` (default: `30s`, because
`SnapshotPinWaitTimeout` default is `1s`).

## Configuration

All runtime controls are configured through `Options`.
Recommended pattern is to start with defaults and override only required values.

```go
opts := rbi.DefaultOptions()

// Planner/trace settings
opts.AnalyzeInterval = 30 * time.Minute // < 0 disables periodic analyze loop
opts.TraceSink = func(ev rbi.TraceEvent) { /* log/collect trace */ }
opts.TraceSampleEvery = 1000 // 0 means "every query" when TraceSink is set

// Online calibration settings
opts.CalibrationEnabled = true           // false disables calibration (default)
opts.CalibrationSampleEvery = 32         // 0 uses default (16)
opts.CalibrationPersistPath = "planner_calibration.json" // optional auto load/save

// Single-op write batcher settings
opts.BatchWindow = 200 * time.Microsecond
opts.BatchMax = 16
opts.BatchMaxQueue = 512 // <= 0 means unbounded queue
opts.BatchAllowCallbacks = true // true allows combining ops with PreCommit callbacks

db, err := rbi.New[uint64, User](bolt, opts)
if err != nil {
    panic(err)
}
```

## Ordering Limitations

The package currently supports ordering by **a single indexed field only**.

Queries that specify more than one ordering expression are rejected with an
error. This restriction is intentional and allows to execute ordered queries
directly via index traversal without materializing or re-sorting intermediate
result sets.

If multi-column ordering is required, it must be implemented at the application
level.

## Struct Tags and Indexing

By default, **all exported struct fields** are indexed using the Go field name.

To exclude a field from indexing, use one of:
- `db:"-"`
- `dbi:"-"`
- `rbi:"-"`

> Excluding large fields (blobs, binary data)
> is strongly recommended unless you actually query on them.

## Slice Fields

Slice-typed fields are indexed element-wise and support `HAS`, `HASNOT`, `HASANY`, 
`HASNONE` operations.

Equality for slice fields is implemented as **set equality**, not array equality.\
This means `["a", "b", "a"] == ["a", "b"]`

## Unique Constraints

Tagging a field with:

```go
`rbi:"unique"`
```

enforces a uniqueness constraint for that field.

* Only scalar (non-slice) fields can be unique.
* Uniqueness is enforced across single and batch writes (`Set`, `Patch*`, `BatchSet`, `BatchPatch*`).
* Violations return `ErrUniqueViolation` before committing the transaction.
* Uniqueness guarantees rely on indexes and are unavailable when indexing is disabled.

## Custom Indexing with `ValueIndexer`

Scalar values and slices of scalars are indexed by default.
Custom types may implement:

```go
type ValueIndexer interface {
    IndexingValue() string
}
```

The returned value is used as the indexed representation.

### Contract:
- Must return a stable, deterministic value.
- Equal values must produce equal indexing values.
- `nil` handling is the responsibility of the implementation.
- `IndexingValue` may be called on a nil receiver.

> Incorrect implementations may cause panics or undefined query behavior.

## Patch Resolution Rules

`Patch` accepts string field identifiers and resolves them in the following order:
1. Struct field name
2. `db` tag
3. `json` tag

This allows JSON payloads to be applied directly without additional mapping.

```go
type User struct {
    // Indexed as "UserName". Patchable via "UserName"
    UserName string
    
    // Indexed as "email". Patchable via "Email", "email", or "mail".
    Email string `db:"email" json:"mail"`
    
    // Not indexed. Patchable via "Password" or "pass".
    Password string `db:"-" json:"pass"`
    
    // Not indexed. Patchable via "Meta".
    Meta string `rbi:"-"`
}
```

## Index Persistence and Recovery

Indexes are persisted only on `Close()`.

An `.rbo` marker file is created on startup. If the marker is present during
the next open, it indicates an unclean shutdown and automatically triggers a
full index rebuild from the stored data.

## Memory Usage

All secondary indexes are kept in memory.\
Memory usage is roughly proportional to:
* number of indexed fields,
* number of records,
* cardinality and distribution of indexed values.

Memory usage can also grow in these cases:
- High write rate with slow compaction (larger snapshot-delta overlays)
- Larger snapshot registry and deeper delta chains
- Large delta compact thresholds combined with write bursts
- Long-lived readers that keep old snapshots pinned (delays snapshot cleanup)

Memory stabilizes when write rate, compaction throughput, and
snapshot retention are balanced. It grows when write churn consistently outruns
compaction/cleanup.

Careful index and snapshot configuration is recommended for large datasets.

## Multiple Instances

Multiple `DB` instances may safely operate on the same bbolt database.\
Each instance maintains its own in-memory index.

## Bucket name

`DB` stores all records in a single top-level bbolt bucket.

By default, the bucket name is derived from the value type.
A custom bucket name can be provided via `Options` 
if explicit control is required (e.g. when value type is renamed).


## Encoding and schema evolution

Values are encoded using [msgpack](https://github.com/vmihailenco/msgpack).

Msgpack provides good performance, compact binary representation, and a
flat encoding model similar to JSON. This makes it tolerant to many
schema changes, including field reordering and movement between embedded
and top-level structs. Unlike `gob`, field decoding does not depend on 
the exact structural layout of the type.

Most schema changes are handled gracefully:

* **Adding fields** – a new index is created for the field; existing records
  simply have no value for it.
* **Removing fields** – the corresponding index is removed, but encoded data
  for the field remains on disk until the record is updated.
* **Renaming fields** – the old index is removed and a new one is created;
  stored data remains until records are updated.
* **Changing field types** – affected indexes are rebuilt; decoding behavior
  and compatibility are the responsibility of the user.

Indexes for affected fields are automatically rebuilt when schema changes are
detected.

## Non-goals

This package does not aim to be a relational database or a SQL engine.

* no projections (`SELECT field1, field2`),
* no joins,
* no aggregation functions (for now),
* no query-time computed fields.

The focus is on fast selection of complete documents.

## Performance notes

- Package is read-optimized.
- Prefer batch writes over single inserts, if possible.
- Always use limits if you do not need the whole set.
- Not all logical branches are currently optimized.

There is still room for optimization, but the current performance is already
suitable for many workloads.

### Query performance expectations

Query performance is shape-dependent. Some classes are fast by design,
some are highly data-dependent, and some are inherently expensive.

#### 1. Fast-by-design classes

These classes are consistently fast when predicates are selective and `LIMIT` is small.
Typical behavior is low-microsecond to sub-microsecond in the current benchmark profile.

- Unique/equality point query with `LIMIT 1`
  - `O(1)` index lookup + `O(1)` result extraction
- Top-N on ordered field (`ORDER BY field LIMIT N`, small `N`)
  - `O(N)` in best case (early stop on first buckets)
- Selective `IN`/`HAS`/`HASANY` with `LIMIT`
  - approximately `O(k + N)`, where `k` is number of touched postings
- Selective prefix with small limit
  - `O(log M + span(prefix) + N)`
- Selective range + order + limit
  - `O(scanned_buckets + checked_rows)` with early stop

#### 2. Data-dependent classes

These can differ by one to two orders of magnitude for the same query shape.
The main reason is that planner/executor cost is dominated by data distribution.

- Range queries (`GT/GTE/LT/LTE`)
- Prefix/text-like filters (`PREFIX`, `SUFFIX`, `CONTAINS`)
- Moderate `OR` expressions with mixed predicates

What mostly determines runtime:
- Predicate selectivity and field cardinality
- Overlap between OR branches (high overlap increases redundant checks + dedupe work)
- Order-field cardinality/skew (high-cardinality order can increase scan/probe cost)
- Prefix span size (short/broad prefix can degenerate into near full-range scan)
- Offset depth (deep skip forces extra scanning even with small limit)
- Negative predicates (`NOT*`) that reduce early-stop opportunities

#### 3. Inherently heavy classes

These are expensive in almost any workload because they force broad candidate
enumeration, global deduplication, expensive ordering, and/or large materialization.

- Wide `OR` trees with ordering and deep pagination:
  - Needs branch-level scanning, dedupe, global rank merge, then skip large prefixes
  - Practical complexity often approaches `O(total_examined_rows)`, with large constants
- Broad text scans (`CONTAINS` / `SUFFIX`) without a selective anchor:
  - Often requires scanning many index keys/buckets before filtering
  - Little opportunity for early pruning without additional anchors

> For heavy and data-dependent classes, benchmark with your real data distribution.\
> Synthetic uniform datasets often hide worst-case behavior.

### Write performance expectations

Write speed depends on how many index entries are touched per operation
(changed fields, slice fan-out, uniqueness checks), and on bbolt fsync/IO.
Insertions are typically more expensive than updates. 
`Patch*` is usually faster than full `Set*` when only a subset of indexed fields changes.

Batch APIs (`BatchSet`, `BatchPatch`, `BatchDelete`) significantly reduce per-record overhead.

## Contributing

Pull requests are welcome.\
For major changes, please open an issue first.
