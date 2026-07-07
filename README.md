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

* ACID – bbolt under the hood.
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
* Partial updates (`Patch`) with minimal index churn
* Transactional automatically batched writes
* Uniqueness constraints
* Optional transparent mode for typed/generic work with plain bbolt db when index is not needed
* Optional runtime diagnostics: query tracing, planner and snapshot stats

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
    Email   string   `db:"email"  rbi:"unique"` // unique constraint
    Age     int      `db:"age"    rbi:"index"`
    Active  bool     `db:"active" rbi:"index"`
    Tags    []string `db:"tags"   rbi:"index"`
    Spent   int64    `db:"spent"  rbi:"measure"` // aggregate-only

    Meta    string   `db:"meta"` // not indexed
    Exclude string   `db:"-"`    // not stored
}

func main() {

    bolt, err := bbolt.Open("test.db", 0600, nil)
    if err != nil {
        panic(err)
    }
    defer bolt.Close()

    users, err := rbi.Open[uint64, User](bolt, rbi.Options{})
    if err != nil {
        panic(err)
    }
    defer users.Close()

    err = rbi.Update(func(tx *rbi.Tx) error {

        if err := users.Set(tx, 1, &User{
            ID:     1,
            Name:   "Alice",
            Email:  "alice@example.com",
            Age:    30,
            Active: true,
            Tags:   []string{"admin", "dev"},
            Spent:  120,
        }); err != nil {
            return err
        }

        return users.Set(tx, 2, &User{
            ID:     2,
            Name:   "Bob",
            Email:  "bob@example.com",
            Age:    40,
            Active: false,
            Tags:   []string{"dev"},
            Spent:  80,
        })
    })
    if err != nil {
        panic(err)
    }

    tx := rbi.BeginView()
    defer tx.Close()

    u1, err := users.Get(tx, 1)
    if err != nil {
        panic(err)
    }
    fmt.Printf("%v (%v)\n", u1.Name, u1.Age)

    records, err := users.Query(tx,
        qx.Where(
            qx.EQ("active", true),
            qx.HASALL("tags", []string{"dev"}),
            qx.GT("age", 25),
        ).
        Sort("age", qx.ASC).
        Limit(10),
    )
    if err != nil {
        panic(err)
    }

    for _, u := range records {
        fmt.Printf("%v (%v)\n", u.Name, u.Age)
    }

    totals, err := users.Aggregate(tx,
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

## Supported data types

- `bool`, `string`, `float32`, `float64`
- signed and unsigned integers (except `uintptr`)
- named scalar types based on the kinds above
- exact `time.Time` and `*time.Time`
- pointers, slices, arrays, structs, and maps containing supported types

Map keys are stricter: string, bool, signed and unsigned integers, arrays of
supported key values, and structs with only exported supported key fields.

Named wrappers around `time.Time` are not supported. Recursive type graphs are
rejected during schema construction. Unexported fields are ignored.

## API

For the full API reference see
[GoDoc](https://pkg.go.dev/github.com/vapstack/rbi).

### Transactions

- `View`/`BeginView` create a **read-only** transaction.
- `IndexView`/`BeginIndexView` create an **index-only** transaction.
- `Update`/`BeginUpdate` create a **write-only** transaction.
  Writes queued in one `Tx` are committed as one atomic logical write.

Read-write transactions are not supported.

### Writing data

* `Set(tx, id, value)` – insert or replace a record and update affected indexes
* `Patch(tx, id, patch)` – apply partial updates and update only changed indexes
* `Delete(tx, id)` – remove a record and its index entries

Do not write directly to buckets managed by RBI through raw Bolt APIs.

### Automatic batching

All writes go through the internal write scheduler.
It cannot be disabled; `BatchSoftLimit` controls the soft batch size limit.

The scheduler groups compatible concurrent logical writes and can isolate
request-level failures where the write contract allows it.
This significantly improves throughput under active parallel write load.

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

* `Query(tx, q)` - return matching records
* `QueryKeys(tx, q)` - return matching IDs
* `Scan(tx, seek, fn)` - perform a sequential scan over all records starting at the given key
* `ScanKeys(tx, seek, fn)` - traverse live keys starting at the given key
* `Aggregate(tx, q)` - evaluate grouped or ungrouped reductions over indexed and measure fields
* `Count(tx, exprs...)` - return result cardinality; it is a reduced non-allocating version of `Aggregate`

### Supported `qx` subset:

- Predicate left-hand side must always be a source field reference.
- Predicate right-hand side must always be a literal value.
- No computed expressions on any side.


- Supported predicate helpers:\
  `AND`, `OR`, `NOT`, `EQ`, `NE`/`NOTEQ`, `GT`, `GTE`, `LT`, `LTE`, `IN`,
  `NOTIN`, `HAS`, `HASALL`, `HASANY`, `HASNONE`, `ISNULL`, `NOTNULL`,
  `PREFIX`, `SUFFIX`, and `CONTAINS`.


- Outside aggregation, ordering supports exactly one expression, and only these forms:
  - By field:\
    `Sort("field", ASC|DESC)` or `SortBy(REF("field"), ASC)`
  - By slice-field length:\
    `SortBy(qx.LEN(qx.REF("field")), ASC|DESC)`
  - By field value position in the provided slice:\
    `SortBy(qx.POS(qx.REF("field"), []T{...}), ASC|DESC)`\
    It requires a literal priority list/array value.\
    Scalar-string `POS(field, "alice bob")` is not supported.


- Projection is not supported, queries return whole records.

### Primary-key queries

`$key` is a reserved synthetic field that exposes record primary keys to
queries. It is not a struct field and cannot be declared with tags.
`Options.Index["$key"]` is also invalid.

For convenience, `rbi.Key` constant can be used.

For numeric-key collections, `$key` becomes available automatically in indexed mode.
It uses the runtime key universe and does not need separate key storage.

For string-key collections, support is opt-in using `EnableStringKeyIndex` option.
A separate in-memory unique string index is maintained when enabled.

### Aggregation

`Aggregate` method evaluates simple reductions against the same in-memory
snapshot model as queries. Filters use the same predicate subset and must
reference indexed fields.

```go
tx := rbi.BeginIndexView()
defer tx.Close()

res, err := users.Aggregate(
    tx,
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
- Grouping by regular indexed scalar fields
- `HAVING` with simple predicates over aggregate outputs
- Ordering by one or more aggregate outputs
- Standalone ungrouped `DISTINCT(field)` as the only metric, e.g. `qx.Aggregate(qx.DISTINCT("country"))`;
  it returns one row per distinct value and includes `NULL` when present

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

Queries that specify more than one ordering expression are rejected with an error.
This restriction allows ordered queries to execute directly via index traversal
without materializing or re-sorting intermediate result sets.

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

If a `Collection` has no indexed fields and no enabled string key index, RBI
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

`QueryKeys`, `Count` and `Aggregate` operate on the index snapshot pinned by
the provided transaction and do not read record values from bbolt.

## Configuration

All runtime controls are configured through `Options`.
Recommended pattern is to set only the required fields.

```go
users, err := rbi.New[uint64, User](bolt, rbi.Options{

    // Planner/trace settings
    AnalyzeInterval: 30 * time.Minute, // < 0 disables periodic analyze loop
    TraceSink: func(ev rbitrace.Event) { /* log/collect trace */ },
    TraceSampleEvery: 1000, // 0 uses default (1), < 0 disables tracing    

    // Write scheduler settings
    BatchSoftLimit: 128,
    
    // ...
})
```

### Hooks

Write methods accept `ExecOption` values, and the same options may also be
passed to `Open` to become defaults for the whole Collection instance.

**Available hooks / options:**

- `PatchStrict` - makes `Patch` reject unknown fields.


- `OnChange` - runs for inserted, updated, and deleted records.
  For inserts and updates it receives a mutable `newValue`.
  For deletes `newValue` is nil. For inserts `oldValue` is nil.
  Hook may modify `newValue` (when it is non-nil).
  New writes issued through the provided `*Tx` commit atomically 
  with the original tx. Do not initiate new writes with another `*Tx`
  from inside `OnChange`. Do not modify `oldValue`, and do not retain
  `oldValue` or `newValue` after the callback returns.


## Struct tags and indexing

Fields can be indexed in two ways:
1. By `rbi` struct tags, used when `Options.Index` is nil.
2. By `Options.Index`, which overrides index declarations when non-nil.

Supported `rbi` tag values are `index`, `unique`, `measure`, and `-`.\
Unknown values or multiple values in one tag are rejected.

Fields tagged `db:"-"` or `rbi:"-"` are completely excluded from RBI storage,
patching and indexing (including declarations through `Options.Index`).

```go
type User struct {
    Email string `db:"email" rbi:"unique"`  // regular index + uniqueness
    Age   int    `db:"age"   rbi:"index"`   // regular inverted index
    Spent int64  `db:"spent" rbi:"measure"` // aggregation-only measure
    Cache string `db:"cache"`               // not indexed
    Temp  string `db:"-"`                   // not stored
    Blob  []byte `db:"blob"  rbi:"-"`       // not stored
}
```

The same declaration can be made from `Options`:

```go
users, err := rbi.Open[uint64, User](bolt, rbi.Options{
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
* Uniqueness is enforced across queued writes in one transaction.
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
- The returned string must not exceed 65535 bytes.
- Indexed fields must have concrete declared types. Interface-declared indexed
  fields are not supported, including `any`, `ValueIndexer`, and custom
  interfaces embedding `ValueIndexer`.
- For a type `T` that implements `ValueIndexer` with a value receiver,
  nil `*T` scalar values are indexed as null.
- In slice indexes, nil `*T` elements for value-receiver `T` do not emit
  index keys.
- Nil pointer-receiver values are passed to `IndexingValue`;
  nil handling is the responsibility of the implementation.
- If the field is used in range queries, lexicographic ordering of indexing
  strings must match the intended value ordering.
- `ValueIndexer` defines only index key projection, not a storage override.

> Incorrect implementations may cause panics or undefined query behavior.

## Patch resolution rules

`Patch` accepts string field identifiers matching any registered alias of a field:

- Go struct field name
- `db` tag value
- `json` tag name, unless it is `json:"-"` or empty

Fields tagged `db:"-"` or `rbi:"-"` are not registered for patching.

```go
type User struct {
    // Indexed as "UserName". Patchable via "UserName"
    UserName string `rbi:"index"`
    
    // Indexed as "email". Patchable via "Email", "email", or "mail".
    Email string `db:"email" json:"mail" rbi:"index"`
    
    // Not indexed. Patchable via "Password" or "pass".
    Password string `json:"pass"`
    
    // Not indexed. Patchable via "Meta".
    Meta string
}
```

## Creating a patch

`MakePatch` builds a complete patch from exported fields changed between two
values. If `oldVal` is nil, every patchable exported field in `newVal` is
emitted. If `newVal` is nil, the patch is empty.

By default, field names use `db` tags when present and otherwise fall back to
the Go field name when that name is an unambiguous patch identifier.
If any changed field has no safe default name, `MakePatch` returns an error.

With `PatchJSON`, names use explicit, non-empty `json` tags and otherwise fall
back to unambiguous Go field names. Changed fields without a safe JSON patch
name, including `json:"-"` fields, return an error instead of being dropped.

Float equality is canonical across scalar and nested values: `-0` equals `+0`,
and all NaN values compare equal. `time.Time` values compare with `Time.Equal`.
Maps are compared by key presence and recursively by value.

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

High-cardinality regular fields whose value buckets are mostly single-record
use a compact singleton layout and are closer to unique indexes.
When a field has both singleton and multi-record buckets, the result falls
between the unique and generic estimates.

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
With defaults, the materialized predicate cache keeps up to 32 regular postings
and skips regular entries above 128K ids. Numeric range acceleration keeps one
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

To reduce allocations further:
- use `ReleaseRecords` when you are done with records so they can be returned to the internal pool.
- use `tx.Release` instead of `tx.Close` to return `*Tx` object to the internal pool.

However, these options require strict lifecycle and ownership rules.

RBI uses semi-manual memory management to minimize GC pressure.
Most internal and intermediate structures are pooled and reused.
Index structures use arenas and ownership-aware copy-on-write behaviour.

## Multiple instances

Multiple different collections may safely operate on the same bbolt database.
Collections opened on one `*bbolt.DB` share one write scheduler,
snapshot generations and read visibility.
Each collection is one typed participant with its own bucket,
schema and in-memory index.

Prefer long-lived `Collection` instances.
Repeated open/close of buckets on one root is allowed, but collection ordinals
are not reused while the root is alive, so the root participant high-watermark
can grow until every collection on that `*bbolt.DB` has closed and the root
is reaped. High-churn open/close on a single root is not the target use case.

## Bucket name

Each collection stores all records in a single top-level bbolt bucket.

By default, the bucket name is derived from the value type.
A custom bucket name can be provided via `Options` 
if explicit control is required (e.g. when value type is renamed).

## Encoding and schema evolution

Values are encoded with internal codec.

The encoded form stores exported fields by stable storage name instead of
positional encoding. This keeps decoding tolerant to many schema
changes, including field reordering and movement between embedded and
top-level structs when the storage name remains the same. Unlike `gob`, field
decoding does not depend on the exact structural layout of the type.

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
  numeric changes decode when the stored value fits the destination type.
  Other type compatibility remains the responsibility of the user.

Indexes for affected tagged fields are automatically rebuilt when schema
changes are detected.

## Storage notes

RBI stores records in the configured bbolt bucket using a key layout that
depends on `K`.

For `Collection[uint64, V]`, bbolt keys are 8-byte big-endian `uint64` values.

For `Collection[string, V]`, bbolt keys are the string keys themselves as raw bytes.
The stored value has an 8-byte prefix with the internal numeric id,
followed by the encoded record payload. String-key collections also maintains
a reverse mapping bucket, which has the same name as data bucket but with 
`.rbimap` suffix.

## Limitations

This package does not aim to be a relational database or a SQL engine.

- no projections (`SELECT field1, field2`)
- no joins
- no query-time computed fields

## Performance notes

- Package is read-optimized.
- Prefer batch writes over single inserts, if possible.
- Always use limits when the whole result set is not needed.
- Complex logical and ordered shapes are data-dependent.

There is still room for optimization, but the current performance is already
suitable for many workloads.

### Query performance

Query performance is shape-dependent. Current code has many specialized
fast paths, but broader shapes depend on data distribution, chosen execution
path, and snapshot-local runtime caches.

#### 1. Usually fast

These classes have dedicated execution paths or strong early-stop behavior.

- Positive `EQ` on a unique scalar field.
  - Candidate set is bounded to at most one row.
- Small top-N on an indexed scalar field or slice length.
  - Ordered scan can stop as soon as enough rows are found.
- Narrow `PREFIX ... LIMIT` without extra ordering, or `PREFIX` on the order field.
  - Binary search into the matching key span, then scan until limit.
- Selective conjunctions of `EQ` / `IN` / `HASALL` / `HASANY` with small `LIMIT`.
  - Lead posting iteration plus candidate checks.
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

Queueing multiple writes in one transaction reduces per-record overhead and
keeps the group atomic.

Throughput of many parallel single operations can be increased using `BatchSoftLimit`.

## Contributing

Documentation and bug fixes are welcome as pull requests.\
For major changes, please open an issue first.
