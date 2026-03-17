This directory contains a local, reduced fork of selected code from:

- Upstream: https://github.com/RoaringBitmap/roaring
- Module path: `github.com/RoaringBitmap/roaring/v2`

Supported `roaring64` surface in this fork is intentionally limited to the API
used by RBI, including constructors, serialization, core bitmap operations,
set algebra, iteration/export, and storage helpers.

Licensing:

- The original upstream project is Apache License 2.0.
- The copied and adapted code in this directory remains subject to that license.
- See `LICENSE-APACHE-2.0.txt` in this directory.
