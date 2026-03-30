This package contains code derived from the Go RoaringBitmap implementation:

- Upstream project: `github.com/RoaringBitmap/roaring/v2`
- Upstream license: [Apache License 2.0](./APACHE-LICENSE-2.0.txt)

The Roaring-derived parts were heavily reworked and integrated into this package.

---

This package is the core of RBI performance.  
It is hard to maintain and reason about because of its current memory model.  
Any work here should be triple-checked.

Memory management and ownership:

- `List` is a small value wrapper over one of four payload forms: empty, singleton, compact (small/mid), or large.
- empty and singleton values are inline. They do not allocate and are not pooled.
- `List.Borrow` returns a non-owning view for non-inline payloads.
  The borrowed state is encoded in `List` metadata, not in the payload itself.
- Borrowed `List` must not release the payload. `List.Release` is a no-op for borrowed values.
- Mutating a borrowed `List` detaches it first. For compact payloads this is a direct compact clone.
  For large payloads this starts as a shared clone and becomes unique on write.
- `largePosting` stores a `largeArray` of `*bitmap32`. `bitmap32` stores a `containerIndex` of `container16` implementations.
- `bitmap32`, `containerArray`, `containerBitmap`, and `containerRun` are refcounted.
  Shared instances are cloned lazily by `getWritableContainerAtIndex(...)` before mutation.
- `largePosting.Clone` is not a full deep clone of the entire large tree.
  It produces a copy-on-write clone by retaining nested `bitmap32` state.
- Pooling is used for compact postings, iterators, `largePosting`, `bitmap32`, `containerArray`, `containerRun`, `containerBitmap`, and temporary batch buffers.
- `acquire*` functions only obtain an object, ensure required capacity, and initialize runtime fields needed for the next use.
- `release*` functions restore canonical reusable state and return the object to its pool when its capacity remains within pool limits.
- Code that stores postings beyond the current local operation must not store borrowed views directly. Such values must be detached first.
