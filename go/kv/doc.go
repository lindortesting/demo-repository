// Package kv provides a persistent string key-value cache: a fixed-capacity
// in-memory LRU with O(1) Get/Put/Delete in front of an append-only
// write-ahead log (WAL) on disk.
//
// Every mutation is appended to the WAL before it touches the cache, so Open
// can rebuild the exact key-value state by replaying the log after a restart.
// A Store is safe for concurrent use. The in-memory lru type is the pure
// eviction policy with no I/O and no locking; Store adds both at the edges.
//
// Durability: writes are unbuffered, so an acknowledged Put or Delete
// survives a process crash. Surviving a power loss requires flushing the OS
// page cache; call Sync when that guarantee is needed.
//
// Recovery semantics: the key-value contents are recovered exactly. Recency
// order after recovery reflects the order of writes, because reads are never
// logged.
package kv
