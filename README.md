# rbi

Roaring Bolt Indexer

Secondary index layer for [bbolt](https://github.com/etcd-io/bbolt) written in Go.\
It works on top of an existing bolt database.\
All index data is stored separately in .rbi files.

Indexes are built over exported struct fields unless explicitly disabled.\
[Roaring64](https://github.com/RoaringBitmap/roaring) is used for index bitmaps.

