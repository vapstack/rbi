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
* **Index-driven execution** – filters and ordering use index structures and bitmap algebra.
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

* `Set(id, value)` – insert or replace a record and update affected indexes.
* `SetMany(ids, values)` – batch variant of `Set`, significantly faster for bulk inserts.
* `Patch(id, fields)` – apply partial updates and update only changed indexes.
* `PatchMany(ids, fields)` – patch multiple records with the same set of values.
* `Delete(id)` – remove a record and its index entries.
* `DeleteMany(ids)` – batch variant of `Delete`.

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

* `QueryItems(q)` – return matching records
* `QueryKeys(q)` – return matching IDs
* `QueryBitmap(expr)` – return a bitmap of matching IDs
* `Count(q)` – return result cardinality

## Query Execution Model

Queries are evaluated exclusively through secondary indexes and bitmap algebra.

1. **Expression evaluation**\
   Each leaf condition (e.g. `age > 30`, `tags HAS "go"`) is resolved into a
   bitmap of matching record IDs using the corresponding index.

2. **Bitmap composition**\
   Logical operators (`AND`, `OR`, `NOT`) are applied using Roaring bitmap
   operations. Intermediate results may be represented as negative sets
   (universe minus exclusions) to keep large result sets efficient.

3. **Ordering and limiting**\
   When ordering is requested, the index of the ordered field is traversed and
   intersected with the filtered bitmap while applying offset and limit.

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
level.

## Struct Tags and Indexing

By default, **all exported struct fields** are indexed using the Go field name.

To exclude a field from indexing, use one of:
- `db:"-"`
-`dbi:"-"`
-`rbi:"-"`

> Excluding large or opaque fields (blobs, binary data)
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

### Requirements:
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

Careful index selection is recommended for large datasets.

## Multiple Instances

Multiple `DB` instances may safely operate on the same bbolt database.\
Each instance maintains its own in-memory index.

## Encoding and Schema Evolution

Values are encoded using [msgpack](https://github.com/vmihailenco/msgpack).

Msgpack provides good performance, compact binary representation, and a
flatten encoding model similar to JSON. This makes it tolerant to many
schema changes, including field reordering and movement between embedded
and top-level structs.

Unlike `gob`, field decoding does not depend on the exact structural layout of
the type, which allows values to be safely decoded even if fields are moved
between embedded structs or promoted to the parent struct.

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

## Optional LRU Cache

The package provides an optional cache wrapper that adds two layers of caching:

1. In-memory LRU cache for individual records.\
   Is stores decoded values and returns **pointers to the cached data**.

2. Invalidation-based cache for query results.\
   A cache that maps a caching key (`QX.Key`) to the resulting list of record **IDs**.

### Item Cache Limitations

Cached values are returned as **pointers to the cached data**.\
No copying is performed.

This means:
- Modifying a returned value mutates the cached entry.
- Concurrent reads and writes to cached values may lead to data races.
- Callers must treat cached values as read-only.

### Query Cache Limitations

- **Phantom Reads**\
  The query cache relies on reactive invalidation:
  a cache entry is cleared only if one of the keys already contained in that entry
  is modified or deleted. If a **new record is inserted** that matches the query
  criteria, the cached result will not be updated or invalidated.
  The `InvalidateQuery` and `InvalidateQueryPrefix` methods can be used
  for manual invalidation.

* **Memory Usage**\
  While the item cache is bounded by the specified capacity,
  the query cache is **unbounded**. Heavy usage of unique query keys may lead
  to memory growth. The `Reset` method clears the entire cache
  and releases memory back to the GC.

### Atomicity Notes

When wrapped by a cache instance, `QueryItems` resolves keys first and fetches
values afterward. If a concurrent update or delete occurs between these steps,
the returned slice may contain `nil` values for records that no longer exist.

## Design Scope and Non-Goals

This package does **not** aim to be a relational database or a SQL engine.

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
* fast bitmap-based filtering,
* batch writes are preferred over single inserts.

There is still room for optimization, but the current performance is already
suitable for many workloads.

## Contributing

Pull requests are welcome.\
For major changes, please open an issue first.
