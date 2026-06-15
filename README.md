## Roaring Bolt Indexer

[![GoDoc](https://pkg.go.dev/badge/github.com/vapstack/rbi.svg)](https://pkg.go.dev/github.com/vapstack/rbi)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/rbi/master/LICENSE)

> This package should be considered experimental

Embedded indexed database layer on top of [bbolt](https://github.com/etcd-io/bbolt).

Indexes are kept fully in memory and built on a heavily reworked fork of
[roaring64](https://github.com/RoaringBitmap/roaring)
for compact memory layout and fast set operations.

RBI is not a replacement for a relational or analytical database.

### Features

* ACID – data durability is delegated to bbolt.
* Fast index-only filtering/sorting – disk is never touched.
* Document-oriented – queries return whole records, not individual fields.
* Strong typing – generic API with user-defined key and value types.
* Opt-in indexing via `rbi` struct tags or `Options`
* Efficient query building via [qx package](https://github.com/vapstack/qx):
  - comparisons: `EQ`, `GT`, `GTE`, `LT`, `LTE`
  - membership: `IN`, `NOTIN`
  - slices: `HASALL`, `HASANY`, `HASNONE`
  - strings: `PREFIX`, `SUFFIX`, `CONTAINS`
  - logical: `AND`, `OR`, `NOT`
* Index-based ordering with offset/limit, including sorting by array position/count
* Simple index-backed aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `DISTINCT`
* Partial updates (`Patch*`) with minimal index churn
* Batch and auto-batched writes
* Uniqueness constraints
* Optional transparent mode for typed/generic work with plain bbolt db when index is not needed
* Optional runtime diagnostics: query tracing and planner/snapshot/auto-batch stats

### LLM notice
Starting from v0.7, parts of the code, documentation and tests were created 
or improved with the assistance of LLM. Code created with LLM has been tested 
and verified, but may still contain minor inefficiencies or style issues.

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
    ID      uint64   `db:"id"` // not indexed

    Name    string   `db:"name"   rbi:"index"`
    Email   string   `db:"email"  rbi:"unique"` // with unique constraint
    Age     int      `db:"age"    rbi:"index"`
    Active  bool     `db:"active" rbi:"index"`
    Tags    []string `db:"tags"   rbi:"index"`
    Spent   int64    `db:"spent"  rbi:"measure"` // aggregate-only

    Meta    string   `db:"meta"   rbi:"-"` // not indexed
    Exclude string                         // not indexed
}

func main() {

    bolt, err := bbolt.Open("test.db", 0600, nil)
    if err != nil {
        panic(err)
    }
    defer bolt.Close()
	
    db, err := rbi.New[uint64, User](bolt, rbi.Options{})
    if err != nil {
        panic(err)
    }
    defer db.Close()

    err = db.Set(1, &User{
        ID:     1,
        Name:   "Alice",
        Email:  "alice@example.com",
        Age:    30,
        Active: true,
        Tags:   []string{"admin", "dev"},
        Spent:  120,
    })
    if err != nil {
        panic(err)
    }

    err = db.Set(2, &User{
        ID:     2,
        Name:   "Bob",
        Email:  "bob@example.com",
        Age:    40,
        Active: false,
        Tags:   []string{"dev"},
        Spent:  80,
    })
    if err != nil {
        panic(err)
    }

    q := qx.Query(
        qx.EQ("active", true),
        qx.HASALL("tags", []string{"dev"}),
        qx.GT("age", 25),
    ).
        Sort("age", qx.ASC).
        Limit(10)

    users, err := db.Query(q)
    if err != nil {
        panic(err)
    }

    for _, u := range users {
        fmt.Printf("%v (%v)\n", u.Name, u.Age)
    }

    totals, err := db.Aggregate(
        qx.Metrics(
            qx.ROWCOUNT().AS("users"),
            qx.SUM("spent").AS("spent"),
        ).Group(
            "age",
        ).Where(
            qx.EQ("active", true),
        ),
    )
    if err != nil {
        panic(err)
    }
    fmt.Println(totals.Layout, totals.Rows)
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

Do not write directly to buckets managed by RBI through raw Bolt APIs.

### Automatic batching

Single-record writes always go through internal auto-batcher.
It cannot be disabled; `AutoBatchMax`, `AutoBatchWindow` and
`AutoBatchMaxQueue` only tune its behavior.

The batcher groups concurrent writes, can isolate requests that fail from the
rest of the batch, and retries operations where the write contract allows it.
This significantly improves throughput under active parallel write load.
For synchronous bulk operations where the caller already has many rows ready,
prefer explicit `BatchSet`, `BatchPatch` or `BatchDelete`.

### Querying

Queries are constructed using the [`qx`](https://github.com/vapstack/qx) package.

Field names refer to the names specified in `db` tags.\
If a field does not have a `db` tag, the Go struct field name is used.

Predicates and ordering can only reference fields with regular indexes (`index`/`unique`).

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
    Sort("field", qx.DESC).
    Offset(10).
    Limit(50)
```

**Query methods:**

* `Query` - return matching records
* `QueryKeys` - return matching IDs
* `Count` - return result cardinality
* `SeqScan` - performs a sequential scan over all records starting at the given key
* `ScanKeys` - traverse live keys starting at the given key
* `Aggregate` - evaluate grouped or ungrouped reductions over indexed and measure fields

### Supported `qx` subset:

- Supported predicate helpers:\
  `AND`, `OR`, `NOT`, `EQ`, `NE`/`NOTEQ`, `GT`, `GTE`, `LT`, `LTE`, `IN`,
  `NOTIN`, `HAS`, `HASALL`, `HASANY`, `HASNONE`, `ISNULL`, `NOTNULL`,
  `PREFIX`, `SUFFIX`, and `CONTAINS`.
- Predicate left-hand side must always be a source field reference.
- Predicate right-hand side must always be a literal value.
- No computed expressions on any side.
- Outside aggregation, ordering supports exactly one expression, and only these forms:
  - By field:\
    `Sort("field", ASC|DESC)` or `SortBy(REF("field"), ASC)`
  - By slice-field length:\
    `SortBy(qx.LEN(qx.REF("field")), ASC|DESC)`
  - By field value position in the provided slice:\
    `SortBy(qx.POS(qx.REF("field"), []T{...}), ASC|DESC)`
- `qx.POS` ordering requires a literal priority list/array value.\
  Scalar-string `POS(field, "alice bob")` is not supported.
- Projection is not supported, queries return whole records.

### Primary-key queries

`$key` is a reserved synthetic field that exposes record primary keys to
queries. It is not a struct field and cannot be declared with tags.
`Options.Index["$key"]` is also invalid.

For numeric-key databases, `$key` becomes available automatically in indexed mode.
It uses the runtime key universe and does not need separate key storage.

For string-key DB, support is opt-in using `EnableStringKeyIndex` option.
A separate in-memory unique string index is maintained when enabled.

### Aggregation

`Aggregate` method evaluates simple reductions against the same in-memory
snapshot model as queries. Filters use the same predicate subset and must
reference indexed fields or `$key`.

```go
res, err := db.Aggregate(
    qx.Where(qx.HAS("tags", "dev")).
        Group("active").
        Metrics(
            qx.ROWCOUNT().AS("rows"),
            qx.COUNT("spent").AS("spent_count"),
            qx.SUM("spent").AS("spent_sum"),
            qx.AVG("spent").AS("spent_avg"),
            qx.MIN("age").AS("min_age"),
            qx.MAX("age").AS("max_age"),
        ),
)
```

Currently supported:
- `SUM`, `AVG`, `MIN`, `MAX` (`SUM`/`AVG` require numeric fields)
- `ROWCOUNT()`, `COUNT(field)`, `COUNT(DISTINCT field)`
- Standalone ungrouped `DISTINCT(field)` as the only metric, e.g. `qx.Aggregate(qx.DISTINCT("country"))`;
  it returns one row per distinct value and includes `NULL` when present
- Grouping by regular indexed scalar fields
- `HAVING` with simple predicates over aggregate outputs
- Ordering by one or more aggregate outputs

Metric fields may be regular scalar indexes or numeric measure indexes.

Aggregate `HAVING` supports `AND`, `OR`, `NOT`, `EQ`, `NE`, `GT`, `GTE`, `LT`,
`LTE`, `IN`, `EXISTS`, and `ISNULL`; the left side must be `OUT`, and
the right side must be a literal. Aggregate ordering supports only `OUT`,
not computed expressions.

Aggregation does not support projection, computed expressions, grouping by
measure fields, slice fields, `DISTINCT` over measure/slice fields, or
standalone `DISTINCT(field)` combined with grouping or other metrics.
`$key` is supported only in filters.

## Ordering limitations

Non-aggregate queries support ordering by **a single indexed field only**.

Queries that specify more than one non-aggregate ordering expression are
rejected with an error. This restriction allows ordered queries to execute
directly via index traversal without materializing or re-sorting intermediate 
result sets.

If multi-column ordering is required, it must be implemented at the application level.

Aggregate queries have separate output ordering and can use multiple `SortOut` keys.

## Composite indexes

Composite indexes are not supported. All indexes live in memory, so storing
field combinations can multiply memory use by the number of distinct values in
the combined fields.

The query engine is optimized to use the existing single-field indexes
efficiently: it intersects bitmap-backed predicates, scans ordered indexes with
early offset/limit stops, uses selective predicates as candidate sources, and
reuses bounded materialized range/predicate state on warm snapshots.

## Transparent mode

If a DB has no indexed fields and no enabled string key index, RBI
automatically switches to transparent mode. This can be useful as a
strongly-typed generic API over a bbolt bucket without maintaining secondary
indexes.

In transparent mode:
- read/write/scan methods continue to work
- query/count/aggregation methods return `ErrNoIndex`
- minimal overhead (no index maintenance) 

## Query execution

Index operations run entirely in-memory; stored records are never scanned.

`Query` is aligned with a bbolt read transaction: the index snapshot and values
come from the same committed bucket sequence, record values are read only after
filtering, ordering and windowing have selected the final record IDs.

`QueryKeys`, `Count` and `Aggregate` operate on the current published index
snapshot and do not read record values from bbolt.

## Configuration

All runtime controls are configured through `Options`.
Recommended pattern is to set only the required fields.

```go
db, err := rbi.New[uint64, User](bolt, rbi.Options{

    // Planner/trace settings
    AnalyzeInterval: 30 * time.Minute, // < 0 disables periodic analyze loop
    TraceSink: func(ev rbitrace.Event) { /* log/collect trace */ },
    TraceSampleEvery: 1000, // 0 uses default (1), < 0 disables tracing    

    // Single-op auto-batcher settings
    AutoBatchWindow: 100 * time.Microsecond,
    AutoBatchMax: 128,
    AutoBatchMaxQueue: 512, // < 0 means unbounded queue, 0 uses default
    
    // ...
})
```

### Hooks

Write methods accept `ExecOption` values, and the same options may also be
passed to `New` to become defaults for the whole DB instance.

**Available hooks/options:**

- `BeforeProcess` - cheap mutable pre-processing hook.
  For `Set`/`BatchSet` it runs on the caller-owned value before RBI starts
  encoding or batching; for `Patch`/`BatchPatch` it runs on the mutable
  post-patch value before `BeforeStore`.

* `BeforeStore` - runs for inserts and updates before RBI encodes the final value.
  It may modify `newValue`.

- `BeforeCommit` - runs inside the Bolt write transaction after the record
  has been written, but before commit. Useful for audit records and other
  writes to neighboring non-RBI buckets.

* `NoBatch` - forces a write call to execute in its own internal batch. For
  `Batch*` methods this is redundant because they are already isolated.

- `CloneFunc` - optional helper for `Set`/`BatchSet` with `BeforeStore`.
  It can be used when the value becomes encodable only after normalization, or
  simply as a faster cloning path than RBI's fallback encode/decode snapshotting.
  If `CloneFunc` is omitted and `*V` implements `Clone() *V`, RBI uses that
  method automatically.

* `PatchStrict` - makes `Patch`/`BatchPatch` reject unknown fields.

**Important notes:**

- `BeforeProcess` may run at different stages depending on the write method.
  Do not retain the value pointer after the hook returns. With `Set`/`BatchSet`,
  it mutates the caller-owned value directly. RBI does not protect against
  aliasing and does not restore the value if the later write fails.

* `BeforeStore` and `BeforeCommit` receive RBI-owned record pointers for 
  decoded old values and mutable working copies.
  Do not retain `oldValue` or `newValue` after the hook returns;
  copy the data you need instead.

- All writes go through the internal batcher. `Batch*` methods keep their
  explicit per-call isolation and are never merged with neighboring writes.

* Under batching/retry, `BeforeProcess` on `Patch`/`BatchPatch`,
  `BeforeStore`, and `BeforeCommit` may run more than once for the same
  logical write, so external side effects should be idempotent.

- `BeforeCommit` must use the provided `*bbolt.Tx` directly and must not call
  methods on the same `DB` instance. Depending on execution mode, RBI may hold
  internal locks while running `BeforeCommit`, so re-entering the same `DB`
  can deadlock or become mode-dependent.

* `BeforeCommit` must not modify any buckets managed by RBI.

## Struct tags and indexing

Fields can be indexed in two ways:
1. By `rbi` struct tags, used when `Options.Index == nil`.
2. By `Options.Index`, which ignores all `rbi` tags when non-nil.

Supported tag values are `index`, `unique`, `measure`, and `-`.\
Unknown values or multiple values in one tag are rejected.

```go
type User struct {
    Email string `db:"email" rbi:"unique"`  // regular index + uniqueness
    Age   int    `db:"age"   rbi:"index"`   // regular inverted index
    Spent int64  `db:"spent" rbi:"measure"` // aggregation-only measure
    Cache string `db:"cache"`               // not indexed
}
```

The same declaration can be made from `Options`:

```go
db, err := rbi.New[uint64, User](bolt, rbi.Options{
    Index: map[string]rbi.IndexKind{
        "email": rbi.IndexUnique,
        "Age":   rbi.IndexDefault,
        "spent": rbi.IndexMeasure,
    },
})
```

`Options.Index` keys may refer to Go field names or `db` tag values.
A non-nil empty map disables all indexes.

Regular indexes power filtering, ordering, `DISTINCT`, grouping,
and ordinary-field aggregation. Unique indexes add a uniqueness constraint.
Measure indexes store numeric field values by record ID for aggregation only;
they are not usable in query predicates, ordering, or `GROUP BY`.

## Indexed string size limit

Indexed string keys are limited to 65,535 bytes per value. This applies to: 
scalar `string` fields, individual elements of `[]string` fields, and values 
returned by `ValueIndexer` (including slice elements that implement it).

The limit is measured in bytes, not runes.

Writes that produce a longer indexed string value are rejected before commit.
If existing data already contains such values, index rebuild/load will fail.

## Slice fields

Slice-typed fields are indexed element-wise and support
`HAS`, `HASALL`, `HASANY`, `HASNONE` operations.

Equality for slice fields is implemented as **set equality**, not array equality.\
This means `["a", "b", "a"] == ["a", "b"]`

## Unique constraints

Tagging a field with:
```go
`rbi:"unique"` // or via Options.Index with rbi.IndexUnique
```
enforces a uniqueness constraint for that field.

* Only scalar (non-slice) fields can be unique.
* Uniqueness is enforced across single and batch writes (`Set`, `Patch*`, `BatchSet`, `BatchPatch*`).
* Violations return `ErrUniqueViolation` before committing the transaction.

## Measure indexes

Measure index is a specialized numeric index used only by aggregation.
It is useful when aggregate results are calculated over filtered record sets 
and the measured numeric field has high cardinality.

Measure indexes are not general-purpose query indexes. They cannot be used for
filtering or ordering and exist only to speed up a narrow set of aggregate
plans. In many cases a regular `rbi:"index"` is faster: `MIN`/`MAX` over an
ordinary index can be almost immediate, and `SUM`/`AVG` over low- and
medium-cardinality fields is often much faster through ordinary index buckets.

A field can be either `rbi:"index"` or `rbi:"measure"`, not both.

## Custom indexing with `ValueIndexer`

Scalar values and slices of scalars can be indexed out of the box 
once the field is tagged for indexing.\
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
- `IndexingValue` must not return more than 65,535 bytes.

> Incorrect implementations may cause panics or undefined query behavior.

## Patch resolution rules

`Patch` accepts string field identifiers matching any registered alias of a field:

- Go struct field name
- `db` tag value, unless it is `db:"-"`
- `json` tag name, unless it is `json:"-"` or empty

```go
type User struct {
    // Indexed as "UserName". Patchable via "UserName"
    UserName string `rbi:"index"`
    
    // Indexed as "email". Patchable via "Email", "email", or "mail".
    Email string `db:"email" json:"mail" rbi:"index"`
    
    // Not indexed. Patchable via "Password" or "pass".
    Password string `db:"-" json:"pass"`
    
    // Not indexed. Patchable via "Meta".
    Meta string
}
```

## Creating a patch

`MakePatch` builds a complete patch from fields changed between two values.

By default, field names use `db` tags when present and otherwise fall back to
the Go field name only when that name is an unambiguous patch identifier. If any
changed field has no safe default name, `MakePatch` returns an error.

With `PatchJSON`, names use explicit, non-empty `json` tags and otherwise fall
back to unambiguous Go field names. Changed fields without a safe JSON patch
name, including `json:"-"` fields, return an error instead of being dropped.

Float semantics are canonical: `-0` equals `+0`, and all NaN values compare
equal. `MakePatch` applies the same semantics to schema-known values it can
compare without walking arbitrary object graphs: direct float fields,
direct float pointer fields, float slices, and acyclic value composites
made of structs/arrays. For maps, interfaces, and arbitrary reference graphs,
MakePatch uses Go reflect equality.

### Ownership and safety

`MakePatch` and `MakePatchInto` copy changed values into the patch,
including unexported nested fields (if any), so later mutations of `newVal`
do not affect ordinary patch data. This is intended for record data graphs:
scalars, structs, slices, maps, pointers, and interfaces containing data values.

They are not general object cloners. Runtime state such as `sync`/`atomic`
values, locks, channels, functions, and other unsafe resources are not supported
and are not diagnosed. These methods do not return errors for such values and 
copy safety is provided on a best-effort basis.

## Index persistence and recovery

Indexes are persisted only on `Close`.

RBI reserves the bucket sequence counter and advances it on each
successful write. The `.rbi` file stores the bucket sequence it was
built from and is loaded only when the current bucket sequence matches.

After a successful `Close`, a fresh `.rbi` file is written from the current
in-memory snapshot and can be reused on the next open.

Transparent mode does not load or write `.rbi` files.

## Memory usage

All secondary indexes are kept in memory.
Memory usage for a single indexed field mostly depends on distinct value count
and average value length in bytes.

### Rough planning estimates

- `N` = number of non-nil records for the field
- `D` = number of distinct indexed values for the field
- `K` = key size in bytes
   (8 for numeric fields, average string length in bytes for strings)

String-backed indexes have a per-value limit of 65,535 bytes.
The estimates below assume indexed string values are comfortably below that limit.
Very long strings reduce chunk density and can increase chunk/page overhead.

### Unique string fields

```text
ApproxMem(field) ~= N * (K + 24...26)
```

For 10,000,000 rows with an average string length of 22 bytes, the estimate is:\
`10,000,000 * (22 + 25) ~= 448 MiB`

### Unique numeric fields

```text
ApproxMem(field) ~= N * (28...30)
```

For 10,000,000 rows this is roughly:\
`10,000,000 * (29) ~= 277 MiB`

### Non-unique scalar fields

```text
ApproxMem(field) ~= D * (K + 20...24) + PostingBytes
```

**PostingBytes** depends on data distribution.
Low-cardinality fields are often much cheaper than unique fields because large
postings compress well.

### Slice fields

Slice fields are indexed element-wise. Each distinct element of the slice is
treated like a regular indexed value of the same type. RBI also maintains a
small slice-length helper index for `LEN` queries and length ordering.

### Measure indexes

Measure indexes store a record id and encoded numeric value for each non-nil measure value.

```text
ApproxMem(field) ~= N * (16...18)
```

For 10,000,000 measured rows this is roughly:\
`10,000,000 * (17) ~= 162 MiB`

### Runtime query caches

Runtime query caches are snapshot-local and bounded by `Options`.
With defaults, the materialized predicate cache keeps up to 24 regular postings
and skips regular entries above 64K ids. Numeric range acceleration keeps one
small descriptor per hot numeric field and up to 4 cached full-span postings per
field, using the same cardinality guard.

A bitmap-shaped 64K posting is about 8 KiB of posting payload, so 24 such cache
entries are about 192 KiB before cache metadata. Less fragmented postings can be
smaller; sparse or oversized hot postings can be larger, but cache memory is
usually small compared with the main index.

### Snapshots and pinned reads

Published index snapshots are immutable and updated with copy-on-write. A
pinned read transaction can temporarily keep an older snapshot alive, but
unchanged storage is shared by reference, so this overhead is usually modest.

### GC pressure

RBI is designed to keep allocations as close to zero as practical, but some
allocations are unavoidable: bbolt operations and value encoding/decoding.
To reduce allocations further, call `ReleaseRecords` when you are done
with records so they can be returned to the internal pool.

RBI uses semi-manual memory management to minimize GC pressure.
Most internal and intermediate structures are pooled and reused.
Index structures use arenas and ownership-aware copy-on-write behaviour.

## Multiple instances

Multiple different `DB` instances may safely operate on the same bbolt database.\
Each instance maintains its own in-memory index.

## Bucket name

`DB` stores all records in a single top-level bbolt bucket.

By default, the bucket name is derived from the value type.
A custom bucket name can be provided via `Options` 
if explicit control is required (e.g. when value type is renamed).

## Encoding and schema evolution

Values are encoded using [msgpack](https://github.com/vmihailenco/msgpack) by
default. If `*V` implements `Codec`, it is used instead.

Msgpack provides good performance, compact binary representation, and a
flat encoding model similar to JSON. This makes it tolerant to many
schema changes, including field reordering and movement between embedded
and top-level structs. Unlike `gob`, field decoding does not depend on 
the exact structural layout of the type.

Most schema changes are handled gracefully:

- **Adding fields** – if the new field is indexed, a new index is
  created for it; existing records have zero value for it.
- **Removing fields** – if the field was indexed, the corresponding index is
  removed, but encoded data for the field remains on disk until the record is
  updated.
- **Renaming fields** – if an indexed field is renamed, the old index is
  removed and a new one is created; stored data remains until records are
  updated.
- **Changing field types** – affected indexes for indexed fields are rebuilt;
  decoding behavior and compatibility are the responsibility of the user.

Indexes for affected tagged fields are automatically rebuilt when schema
changes are detected.

### Custom encoding

Type can implement `Codec` interface:
```go
type Codec interface {
    EncodeRBI(io.Writer) error
    DecodeRBI(io.Reader) error
}
```
If it does, RBI uses it instead of msgpack for all encoding/decoding work.
Fallback decoding, if required, is the responsibility of the implementation.

## Storage notes

RBI stores records in the configured bbolt bucket using a key layout that
depends on `K`.

For `DB[uint64, V]`, bbolt keys are 8-byte big-endian `uint64` values.

For `DB[string, V]`, bbolt keys are the string keys themselves as raw bytes.
The stored value has an 8-byte prefix with the internal numeric id,
followed by the encoded record payload. String-key DB also maintains a reverse
mapping bucket, which has the same name as data bucket but with `.rbimap` suffix.

## Limitations

This package does not aim to be a relational database or a SQL engine.

- no projections (`SELECT field1, field2`)
- no joins
- no query-time computed fields

## Performance notes

- Package is read-optimized.
- Prefer batch writes over single inserts, if possible.
- Always use limits when the whole result set is not needed.
- Complex logical and ordered shapes are data-dependent; benchmark important
  production queries on representative data.

There is still room for optimization, but the current performance is already
suitable for many workloads.

### Query performance

Query performance is shape-dependent. Current code has many specialized
fast paths, but broader shapes depend on data distribution, chosen execution
path, and whether snapshot-local runtime caches are already warm.

#### 1. Usually fast

These classes have dedicated execution paths or strong early-stop behavior.

- Positive `EQ` on a unique scalar field.
  - Candidate set is bounded to at most one row.
- Small top-N on an indexed scalar field or slice length.
  - Ordered scan can stop as soon as enough rows are found.
- Narrow `PREFIX ... LIMIT` without extra ordering, or `PREFIX` on the order field.
  - Binary-search into the matching key span, then scan until limit.
- Selective conjunctions of `EQ` / `IN` / `HASALL` / `HASANY` with small `LIMIT`.
  - Usually lead-posting iteration plus candidate checks.
- Narrow `POS` ordering over a small literal priority list.
  - Best when the filter and requested window are both tight.

#### 2. Data-dependent or cache-sensitive

These can differ by one or more orders of magnitude for the same query shape.
The main reason is that planner/executor cost is dominated by span width,
cardinality, overlap, ordering, and cache warmth.

- Numeric range queries (`GT/GTE/LT/LTE`), especially with another `ORDER BY` field.
  - Warm snapshots can be much faster when reusable range work is already cached.
- Broad `PREFIX` on a non-order field, especially with a different `ORDER BY`.
- Substring/suffix filters (`CONTAINS`, `SUFFIX`), especially without a selective indexed anchor.
- `OR` expressions with mixed predicates
  - Specialized routes exist, but branch count, overlap, ordering and limit/window
    strongly affect the selected path.
- Broad `POS` ordering or deep offsets over slice/order fields.

What mostly determines runtime:
- Predicate selectivity and field cardinality
- Overlap between `OR` branches (high overlap increases redundant checks + dedupe work)
- Order-field cardinality/skew
- Prefix/range span size
- Offset depth and requested window
- Whether bounded snapshot caches already contain reusable materialized spans
- Negative predicates (`NOT*`) that reduce early-stop opportunities

#### 3. Usually heavy

These are expensive in almost any workload because they force broad candidate
enumeration, global deduplication, expensive ordering, and/or large materialization.

- Wide `OR` trees with ordering and deep pagination:
  - Needs branch-level scanning, dedupe, global rank merge, then skip large prefixes
  - Practical complexity often approaches `O(total_examined_rows)`, with large constants
- Broad suffix/substring scans (`SUFFIX`, `CONTAINS`) without a selective anchor:
  - Often requires scanning many index keys/buckets before filtering
  - Little opportunity for early pruning without additional anchors
- Record-returning queries that need most or all matches rather than a small prefix:
  - Early-stop advantages disappear and materialization cost starts to dominate

### Aggregation performance

`Count` and `Aggregate` use index snapshots and do not read record values from
bbolt. Cost depends on the filter, metric fields, grouping cardinality,
and whether the result must be ordered or filtered with `HAVING`.

- `ROWCOUNT`, simple `COUNT`, and `MIN`/`MAX` over ordinary indexes are usually very cheap.
- `SUM/AVG` over low- or medium-cardinality ordinary indexes also very fast.
- `COUNT(DISTINCT)` and `DISTINCT` depend mostly on distinct key count and filters.
- `measure` indexes help mainly for filtered aggregation over high-cardinality numeric fields.
- Grouped aggregation becomes heavier as group cardinality, metric count,
  `HAVING`, `SortOut`, offset and limit work increase.

### Write performance

Write speed depends on how many index entries are touched per operation
(changed fields, slice fan-out, uniqueness checks), and on bbolt fsync/IO.
Insertions are typically more expensive than updates.

Batch APIs (`BatchSet`, `BatchPatch`, `BatchDelete`) significantly reduce per-record overhead.

Throughput of many parallel single operations can be increased
using `AutoBatchMax`, `AutoBatchWindow` and `AutoBatchMaxQueue`. 


## Contributing

Documentation and bug fixes are welcome as pull requests.\
For major changes, please open an issue first.
