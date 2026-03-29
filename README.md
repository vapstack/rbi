# rbi

[![GoDoc](https://pkg.go.dev/badge/github.com/vapstack/rbi.svg)](https://pkg.go.dev/github.com/vapstack/rbi)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/rbi/master/LICENSE)

> **This package should be considered experimental**

## Roaring Bolt Indexer

A secondary index layer for [bbolt](https://github.com/etcd-io/bbolt).

It turns a key-value store into a document-oriented database with rich
query capabilities, while preserving bbolt’s ACID guarantees for data storage.
Indexes are kept fully in memory and built on a heavily reworked fork of
[roaring64](https://github.com/RoaringBitmap/roaring) for compact memory layout and fast set operations.

### Properties

* ACID – data durability is delegated to bbolt.
* Index-only filtering – disk is never touched.
* Document-oriented – queries return whole records, not individual fields.
* Strong typing – generic API with user-defined key and value types.

### Features

* Automatic indexing of exported struct fields
* Fine-grained control via struct tags (`db`, `dbi`, `rbi`)
* Efficient query building via [qx package](https://github.com/vapstack/qx):
  - comparisons: `EQ`, `GT`, `GTE`, `LT`, `LTE`
  - slices: `IN`, `HAS`, `HASANY`
  - strings: `PREFIX`, `SUFFIX`, `CONTAINS`
  - logical: `AND`, `OR`, `NOT`
* Index-based ordering with offset / limit, including `ByArrayPos` and `ByArrayCount`
* Partial updates (`Patch*`) with minimal index churn
* Batch and auto-batched writes
* Uniqueness constraints
* Optional runtime diagnostics: query tracing, planner/snapshot/auto-batch stats, and online
  calibration

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
    defer bolt.Close()
	
    db, err := rbi.New[uint64, User](bolt, rbi.Options{})
    if err != nil {
        _ = bolt.Close()
        panic(err)
    }
    defer db.Close()

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

    users, err := db.Query(q)
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

* `Set` – insert or replace a record and update affected indexes
* `BatchSet` – batch variant of `Set`, significantly faster for bulk inserts
* `Patch` – apply partial updates and update only changed indexes
* `BatchPatch` – batch patch variant
* `Delete` – remove a record and its index entries
* `BatchDelete` – batch variant of `Delete`

Do not write directly to the bucket managed by `rbi` through raw Bolt APIs,
(including `SetSequence`/`NextSequence`).

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

* `Query` – return matching records
* `QueryKeys` – return matching IDs
* `Count` – return result cardinality (ignoring offset/limit)
* `SeqScan` - performs a sequential scan over all records starting at the given key
* `ScanKeys` – iterate the current in-memory key set without opening a Bolt read transaction

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
For `Query`, record values are fetched from bbolt only for IDs that have
passed all filters and limits.

`Query` runs against an index snapshot aligned with a bbolt read transaction.
Readers open a bbolt read transaction, read the managed bucket sequence, and
pin the snapshot registered for that exact sequence. RBI pre-registers the next
snapshot before commit, so readers do not wait for post-commit publication and
are not affected by unrelated writes to other bbolt buckets.

## Configuration

All runtime controls are configured through `Options`.
Recommended pattern is to set only the fields you need.

```go
db, err := rbi.New[uint64, User](bolt, rbi.Options{

    // Planner/trace settings
    AnalyzeInterval: 30 * time.Minute, // < 0 disables periodic analyze loop
    TraceSink: func(ev rbi.TraceEvent) { /* log/collect trace */ },
    TraceSampleEvery: 1000, // 0 uses default (1), < 0 disables tracing    
    
    // Online calibration settings
    CalibrationEnabled: true, // false disables calibration (default)
    CalibrationSampleEvery: 32, // 0 uses default (16), < 0 disables sampled calibration
    PersistCalibration: true, // optional auto load/save to .cal file
    
    // Single-op auto-batcher settings
    AutoBatchWindow: 100 * time.Microsecond,
    AutoBatchMax: 128,
    AutoBatchMaxQueue: 512, // < 0 means unbounded queue, 0 uses default
})
if err != nil {
    panic(err)
}
```

### Hooks

Write methods accept `ExecOption` values, and the same options may also be
passed to `New` to become defaults for the whole DB instance.

Available hooks/options:

- `BeforeProcess` - cheap mutable pre-processing hook.
  For `Set`/`BatchSet` it runs on the caller-owned value before RBI starts
  encoding or batching; for `Patch`/`BatchPatch` it runs on the mutable
  post-patch value before `BeforeStore`.

* `BeforeStore` - runs for inserts and updates before RBI encodes the final value.
  It may modify `newValue`.

- `BeforeCommit` - runs inside the Bolt write transaction after the record
  has been written, but before commit. Useful for audit records and other
  writes to neighboring buckets.

* `NoBatch` - forces a write call to execute in its own internal batch. For
  `Batch*` methods this is redundant because they are already isolated.

- `CloneFunc` - optional helper for `Set`/`BatchSet` with `BeforeStore`.
  It can be used when the value becomes encodable only after normalization, or
  simply as a faster cloning path than RBI's fallback msgpack snapshotting.
  If `CloneFunc` is omitted and `*V` implements `Clone() *V`, RBI uses that
  method automatically.

* `PatchStrict` - makes `Patch`/`BatchPatch` reject unknown fields.

Important notes:

- `BeforeProcess` may run at different stages depending on the write method.
  Do not retain the value pointer after the hook returns. With `Set`/`BatchSet`, 
  it mutates the caller-owned value directly. RBI does not protect against 
  aliasing and does not restore the value if the later write fails.
- All writes go through the internal batcher. `Batch*` methods keep their
  explicit per-call isolation and are never merged with neighboring writes.
- Under batching/retry, `BeforeProcess` on `Patch`/`BatchPatch`,
  `BeforeStore`, and `BeforeCommit` may run more than once for the same
  logical write, so external side effects should be idempotent.
- `BeforeCommit` must use the provided `*bbolt.Tx` directly and must not call
  methods on the same `DB` instance. Depending on execution mode, RBI may hold
  internal locks while running `BeforeCommit`, so re-entering the same `DB`
  can deadlock or become mode-dependent.
- `BeforeCommit` must not modify the bucket managed by RBI itself.

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

## Custom indexing with `ValueIndexer`

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

## Patch resolution rules

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

## Index persistence and recovery

Indexes are persisted only on `Close`.

RBI reserves the bucket sequence counter and advances it on each
successful write. The `.rbi` file stores the bucket sequence it was
built from and is loaded only when the current bucket sequence matches.

After a successful `Close`, a fresh `.rbi` file is written from the current
in-memory snapshot and can be reused on the next open.

## Memory usage

All secondary indexes are kept in memory.\
Memory usage is roughly proportional to:
* number of indexed fields,
* number of records,
* cardinality and distribution of indexed values.

Careful index configuration is recommended for very large datasets.

## Multiple instances

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

### Query performance

Query performance is shape-dependent. Current code has several specialized
fast paths, but broader shapes depend on data distribution, chosen execution
path, and whether snapshot-local runtime caches are already warm.

#### 1. Usually fast

These classes have dedicated execution paths or strong early-stop behavior.
On the current synthetic benchmark profile, pure point/top-N/autocomplete
shapes are typically sub- to low-single-microsecond.

- Positive `EQ` on a unique scalar field, with or without explicit `LIMIT 1`
  - Candidate set is bounded to at most one row.
- Small top-N on an indexed scalar field (`ORDER BY field LIMIT N`)
  - Ordered scan can stop as soon as enough rows are found.
- Narrow `PREFIX ... LIMIT` without extra ordering, or `PREFIX` on the order field
  - Binary-search into the matching key span, then scan until limit.
- Selective conjunctions of `EQ` / `IN` / `HAS` / `HASANY` with small `LIMIT`
  - Usually lead-posting iteration plus candidate checks.

#### 2. Data-dependent or cache-sensitive

These can differ by one or more orders of magnitude for the same query shape.
The main reason is that planner/executor cost is dominated by span width,
cardinality, overlap, ordering, and cache warmth.

- Numeric range queries (`GT/GTE/LT/LTE`), especially with another `ORDER BY` field
  - Current code uses numeric-range bucket reuse plus bounded materialized-predicate caching, so warm snapshots can be much faster than cold-cache runs.
- Broad `PREFIX` on a non-order field, especially with a different `ORDER BY`
- Prefix/text-like filters (`SUFFIX`, `CONTAINS`)
- Moderate `OR` expressions with mixed predicates

What mostly determines runtime:
- Predicate selectivity and field cardinality
- Overlap between `OR` branches (high overlap increases redundant checks + dedupe work)
- Order-field cardinality/skew
- Prefix/range span size
- Offset depth / requested window
- Whether bounded snapshot caches already contain reusable materialized spans
- Negative predicates (`NOT*`) that reduce early-stop opportunities

#### 3. Usually heavy

These are expensive in almost any workload because they force broad candidate
enumeration, global deduplication, expensive ordering, and/or large materialization.

- Wide `OR` trees with ordering and deep pagination:
  - Needs branch-level scanning, dedupe, global rank merge, then skip large prefixes
  - Practical complexity often approaches `O(total_examined_rows)`, with large constants
- Broad text scans (`CONTAINS` / `SUFFIX`) without a selective anchor:
  - Often requires scanning many index keys/buckets before filtering
  - Little opportunity for early pruning without additional anchors
- Queries that need most or all matches rather than a small prefix of results:
  - Early-stop advantages disappear and materialization cost starts to dominate

### Write performance

Write speed depends on how many index entries are touched per operation
(changed fields, slice fan-out, uniqueness checks), and on bbolt fsync/IO.
Insertions are typically more expensive than updates.

Batch APIs (`BatchSet`, `BatchPatch`, `BatchDelete`) significantly reduce per-record overhead.

## Contributing

Pull requests are welcome.\
For major changes, please open an issue first.
