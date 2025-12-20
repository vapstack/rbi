# rbi

[![GoDoc](https://godoc.org/github.com/vapstack/rbi?status.svg)](https://godoc.org/github.com/vapstack/rbi)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/rbi/master/LICENSE)

### Roaring Bolt Indexer

A high-performance in-memory secondary index layer for
[bbolt](https://github.com/etcd-io/bbolt) written in pure Go.

It turns a simple key-value store into a document-oriented database with rich
query capabilities, while preserving bbolt’s ACID guarantees for data storage.

Indexes are kept fully in memory and built on top of
[roaring64](https://github.com/RoaringBitmap/roaring), allowing fast set
operations, range scans, ordering, and filtering with microsecond-level latency.

## Key Properties

* **ACID data** – data durability is delegated to bbolt.
* **Index-only queries** – all queries are evaluated via in-memory indexes.
* **Document-centric** – queries select whole records, not individual fields.
* **Strong typing** – generic API with user-defined key and value types.
* **Index-driven execution** – filters and ordering are applied using
  index structures and bitmap algebra.
* **Crash-safe** – indexes are automatically rebuilt after unclean shutdowns.

## Features

* Automatic indexing of exported struct fields
* Fine-grained control via struct tags
* Efficient query building via [qx package](https://github.com/vapstack/qx):
    - comparisons: `EQ`, `GT`, `GTE`, `LT`, `LTE`
    - slice operations: `IN`, `HAS`, `HASANY`
    - string operations: `PREFIX`, `SUFFIX`, `CONTAINS`
    - logical operators: `AND`, `OR`, `NOT`
* Index-based ordering with offset/limit
* Partial updates (`Patch`) with minimal index churn
* Batch writes (`SetMany`, `PatchMany`)
* Optional uniqueness constraints

## Quick Start

```go
package main

import (
    "fmt"

    "github.com/vapstack/qx"
    "github.com/vapstack/rbi"
)

type User struct {
    ID     uint64   `db:"-"`  // not indexed
    Name   string   `db:"name"`
    Age    int      `db:"age"`
    Active bool     `db:"active"`
    Tags   []string `db:"tags"`
    Meta   string   `rbi:"-"` // not indexed
}

func main() {

    db, err := rbi.Open[uint64, User]("test.db", 0600, nil)
    if err != nil {
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
        Limit(10)

    users, err := db.QueryItems(q)
    if err != nil {
        panic(err)
    }

    for _, u := range users {
        fmt.Printf("%v (%v)\n", u.Name, u.Age)
    }
}
```

## API Overview

For the full API reference see the
[GoDoc](https://godoc.org/github.com/vapstack/rbi).

### Writing Data

* `Set(id, value)`\
  Inserts or replaces a record and updates indexes for modified fields.

* `SetMany(ids, values)`\
  Batch variant of `Set`, significantly faster for bulk inserts.

* `Patch(id, fields)`\
  Partial update mechanism:
    - decodes the existing record,
    - applies field-level changes,
    - computes a diff,
    - updates only affected index entries.

* `Delete(id)`\
  Removes a record and its index entries.

### Querying

Queries are constructed using the [`qx`](https://github.com/vapstack/qx) package.

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

Execution methods:

* `QueryItems(q)` – returns matching records
* `QueryKeys(q)` – returns matching IDs only
* `QueryBitmap(expr)` – returns a Roaring bitmap of matching IDs
* `Count(q)` – returns cardinality of the result set

## Query Execution Model

Queries are evaluated exclusively through secondary indexes and bitmap algebra.

At a high level:

1. **Expression evaluation**\
   Each leaf condition (e.g. `age > 30`, `tags HAS "go"`) is resolved into a
   bitmap of matching record IDs using the corresponding index.

2. **Bitmap composition**\
   Logical operators (`AND`, `OR`, `NOT`) are executed using Roaring bitmap
   operations. Intermediate results may be represented either as positive sets
   or as *negative* sets (universe minus exclusions) to keep large result sets
   efficient.

3. **Ordering and limiting**\
   When `Order` is present, the index of the ordered field is traversed,
   intersecting buckets with the filtered bitmap and applying `Offset`
   and `Limit` incrementally.

4. **Materialization**  
   Only the final set of matching IDs is materialized. Record values are fetched
   from bbolt only for IDs that survive all filters.

## Ordering Limitations

The package currently supports ordering by **a single indexed field only**.

Queries that specify more than one ordering expression are rejected with an
error. This restriction is intentional and allows to execute ordered queries
directly via index traversal without materializing or re-sorting intermediate
result sets.

If multi-column ordering is required, it must be implemented at the application
level after fetching the result set.

## Struct Tags and Indexing

By default, **all exported struct fields** are indexed using the Go field name.

To exclude a field from indexing, set one of the following tags to `"-"`:

* `db:"-"`
* `dbi:"-"`
* `rbi:"-"`

> Excluding large or opaque fields (JSON blobs, encrypted payloads, binary data)
> is strongly recommended unless you actually query on them.

## Slice Fields

Slice-typed fields are indexed element-wise and support `HAS`, `HASNOT`, `HASANY`, 
`HASNONE` operations.

## Unique Constraints

Tagging a field with:

```go
`rbi:"unique"`
```

enforces a uniqueness constraint for that field.

* Only scalar (non-slice) fields can be unique.
* Uniqueness is enforced across single and batch writes (`SetMany`, `PatchMany`).
* Violations return `ErrUniqueViolation` before committing the transaction.
* Uniqueness guarantees rely on indexes and are unavailable when indexing is
  explicitly disabled.

## Custom Indexing with `ValueIndexer`

The package indexes scalar values and slices of scalar values by default.
For custom or complex types, it provides the `ValueIndexer` interface:

```go
type ValueIndexer interface {
    IndexingValue() string
}
```

If a field value implements `ValueIndexer`, indexing engine uses the value 
returned by `IndexingValue()` as the indexed representation of that field.

This allows user-defined types to participate in indexing without changing 
their in-memory or serialized representation.

> Incorrect or unstable implementations of `ValueIndexer` may lead to panics
> and/or undefined query behavior.

### Usage Notes

- `IndexingValue()` must return a stable and deterministic value.
- Different values that should compare equal in queries must return the same
  indexing value.
- The returned value is treated as an opaque index key and is compared using
  the same semantics as built-in scalar types.
- Nil handling is the responsibility of the implementation.
  If nil values are expected, `IndexingValue()` must handle them explicitly.

This mechanism is intended for advanced use cases where built-in scalar
indexing is insufficient.

## Patch Resolution Rules

`Patch` accepts string field identifiers and resolves them in the following order:

1. **Struct field name**
2. **`db` tag**
3. **`json` tag**

This allows JSON payloads to be applied directly without additional mapping.

```go
type User struct {
    // Indexed as "UserName". Patchable via "UserName"
    UserName string
    
    // Indexed as "email". Patchable via "Email", "email", or "mail".
    Email string `db:"email" json:"mail"`
    
    // Indexed as "Password". Patchable via "Password" or "pass".
    Password string `db:"-" json:"pass"`
    
    // Not indexed. Patchable via "Meta".
    Meta string `rbi:"-"`
}
```

## Index Persistence and Recovery

Indexes are persisted only on `Close()`.

On startup, an `.rbo` marker file is created. If the marker is present during
the next open, it indicates an unclean shutdown and automatically triggers a
full index rebuild from the stored data.

## Memory Usage

All secondary indexes are stored fully in memory.\
Memory usage is roughly proportional to:
* number of indexed fields,
* number of records,
* cardinality and distribution of indexed values.

Careful selection of indexed fields is strongly recommended for large datasets.

## Multiple Instances

Multiple instances of `DB` can safely work on top of a single bbolt database.
Each instance maintains its own in-memory index. Whether this is desirable
depends on workload and memory constraints and should be benchmarked for the
target use case.

## Encoding and Schema Evolution

Values are encoded using **msgpack**.

This provides:
* good performance,
* compact representation,
* stable field layout.

Schema changes are generally safe:
* adding fields – safe,
* removing fields – safe,
* renaming fields – requires index rebuild,
* changing field types – requires index rebuild.

Indexes are automatically rebuilt for affected fields when schema changes are
detected.

## Design Scope and Non-Goals

This package intentionally does **not** aim to be a relational database or a SQL engine.

* no projections (`SELECT field1, field2`),
* no joins,
* no aggregation functions,
* no query-time computed fields.

The focus is on fast selection of complete documents using secondary indexes.

## Performance Notes

The package is optimized for read-heavy workloads with complex filters.

Typical characteristics:
* microsecond-level query latency,
* logarithmic range queries,
* very fast `AND` / `OR` filtering via roaring bitmaps,
* batch writes strongly preferred over single inserts.

There is still room for optimization, but the current performance is already
suitable for many workloads.

## Contributing

Pull requests are welcome.

For major changes, please open an issue first to discuss design and compatibility.
