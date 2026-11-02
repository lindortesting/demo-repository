// Package kv provides a persistent string key-value cache: a fixed-capacity
// in-memory LRU with O(1) Get/Put/Delete in front of an append-only
// write-ahead log (WAL) on disk.
//
// Every mutation is appended to the WAL before it touches the cache, so Open
// can rebuild the exact key-value state by replaying the log after a restart.
// A Store is safe for concurrent use, and a WAL admits exactly one Store at a
// time: Open holds an exclusive advisory lock (flock) until Close. The
// in-memory lru type is the pure eviction policy with no I/O and no locking;
// Store adds both at the edges.
//
// Durability: writes are unbuffered, so an acknowledged Put or Delete
// survives a process crash. Surviving a power loss requires flushing the OS
// page cache; call Sync when that guarantee is needed.
//
// Recovery semantics: replay restores the exact cache state — contents and
// recency — because every state change is appended to the WAL before it is
// applied, including the recency refresh from a Get hit. The cost is that a
// cache hit performs one WAL append; a miss does no I/O.
package kv
