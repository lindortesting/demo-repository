# kv

A persistent string key-value cache for Go: a fixed-capacity in-memory LRU
with O(1) `Get`/`Put`/`Delete` in front of an append-only write-ahead log
(WAL) on disk. Every mutation is appended to the WAL before it touches the
cache, so `Open` rebuilds the exact key-value state by replaying the log
after a restart. `Store` is safe for concurrent use.

## Layout

| File       | Contents                                                        |
|------------|-----------------------------------------------------------------|
| `lru.go`   | The eviction policy: a pure, unsynchronized `lru` (map + doubly-linked list). No I/O. |
| `store.go` | `Store`: the `lru` plus WAL persistence and a `sync.Mutex`.     |
| `doc.go`   | Package documentation.                                          |

## Usage

```go
s, err := kv.Open("/var/lib/myapp/kv.wal", 100_000) // path, capacity
if err != nil {
	log.Fatal(err)
}
defer s.Close()

err = s.Put("user:42", "ada")
v, ok := s.Get("user:42")
err = s.Delete("user:42")
err = s.Sync() // fsync the WAL: durability across power loss
n := s.Len()
```

## WAL format

One record per mutation, appended and never rewritten:

```
crc32(4) | op(1) | keyLen(4) | valLen(4) | key | value
```

The CRC32 (IEEE) covers everything after itself. Replay reads records in
order and re-applies them to a fresh LRU — including the capacity bound, so
recovery cannot grow the cache beyond what the live cache would have held. A
record that decodes short or fails its CRC is treated as a torn tail (a
crash mid-append): replay stops there and the file is truncated to the last
good record, so later appends never follow garbage.

## Semantics and limitations

- **Recovered exactly:** key-value contents. Deletes do not resurrect.
- **Recovered partially:** recency order. Reads are never logged, so after a
  restart the eviction order reflects write order, not read order.
- **Durability:** writes are unbuffered, so an acknowledged `Put`/`Delete`
  survives a process crash. Surviving a power loss requires `Sync`.
- **Growth:** the WAL is append-only and grows without bound; there is no
  compaction or snapshotting. Operationally, rotate by writing a fresh WAL.
- **Concurrency:** a single mutex serializes everything (`Get` mutates
  recency, so a read-write lock would not help).

## Running the checks

```sh
go test ./...            # 28 tests
go test -race ./...      # includes TestStoreConcurrent
go vet ./... && gofmt -l .
go test -bench=. -benchmem ./...
```

## Benchmarks

Measured on this repo's VM (Intel Xeon @ 2.80GHz, 4 vCPU, linux/amd64,
go1.23.4), `go test -bench=. -benchmem -count=1 ./...`:

```
goos: linux
goarch: amd64
pkg: github.com/lindortesting/demo-repository/go/kv
cpu: Intel(R) Xeon(R) CPU @ 2.80GHz
BenchmarkStorePut-4           	  595854	      2430 ns/op	     215 B/op	       4 allocs/op
BenchmarkStoreGet-4           	13831611	        86.18 ns/op	       3 B/op	       0 allocs/op
BenchmarkStorePutParallel-4   	  383734	      3514 ns/op	     194 B/op	       4 allocs/op
BenchmarkOpenRecovery-4       	       5	 210059809 ns/op	20079912 B/op	  603995 allocs/op
```

Read: a `Put` costs ~2.4µs, dominated by the unbuffered WAL write; a
cache-hit `Get` is ~86ns with no allocations; parallel `Put`s serialize on
the mutex at ~3.5µs each; recovery replays ~100k records in ~210ms
(~2.1µs/record). Numbers are machine- and disk-dependent; rerun the command
above to reproduce.
