# kv

A persistent string key-value cache for Go: a fixed-capacity in-memory LRU
with O(1) `Get`/`Put`/`Delete` in front of an append-only write-ahead log
(WAL) on disk. Every state change — including the recency refresh from a
`Get` hit — is appended to the WAL before it touches the cache, so `Open`
rebuilds the exact cache state, eviction decisions included, by replaying
the log after a restart. `Store` is safe for concurrent use.

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
v, ok, err := s.Get("user:42") // a hit appends a touch record and can fail
err = s.Delete("user:42")
err = s.Sync() // fsync the WAL: durability across power loss
n := s.Len()
```

## WAL format

One record per state change, appended and never rewritten:

```
crc32(4) | op(1) | keyLen(4) | valLen(4) | key | value
```

Ops: `1` = put, `2` = delete, `3` = touch (a `Get` hit's recency refresh).
The CRC32 (IEEE) covers everything after itself. Replay reads records in
order and re-applies them to a fresh LRU — including the capacity bound, so
recovery cannot grow the cache beyond what the live cache would have held. A
record that decodes short or fails its CRC is treated as a torn tail (a
crash mid-append): replay stops there and the file is truncated to the last
good record, so later appends never follow garbage.

Keys and values frame as `uint32`, so each may be at most 4 GiB−1 byte;
larger inputs are rejected before anything is allocated or written.

## Semantics and limitations

- **Recovered exactly:** key-value contents *and* recency. Replay applies
  the identical operation sequence, so the post-recovery cache — including
  which key the next insert would evict — matches the pre-restart cache.
- **Read cost:** a `Get` hit is a state change (recency), so it performs one
  WAL append and can report an error. A miss changes nothing and does no
  I/O.
- **Durability:** writes are unbuffered, so an acknowledged mutation
  survives a process crash. Surviving a power loss requires `Sync`.
- **Failure behavior:** if a WAL append fails, the partial record is rolled
  back. If the rollback itself fails, the store goes read-only: every later
  mutation is rejected with a sticky error until the store is closed and
  reopened (replay then truncates the partial record). Acknowledging writes
  that recovery would silently discard is worse than refusing them.
- **Growth:** the WAL is append-only and grows without bound — reads
  included. There is no compaction or snapshotting; operationally, rotate by
  writing a fresh WAL.
- **Concurrency:** a single mutex serializes everything (`Get` mutates
  recency, so a read-write lock would not help).

## Running the checks

```sh
go test ./...            # 31 tests
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
BenchmarkStorePut-4           	  607719	      2450 ns/op	     213 B/op	       4 allocs/op
BenchmarkStoreGet-4           	  685588	      1753 ns/op	      27 B/op	       1 allocs/op
BenchmarkStorePutParallel-4   	  353068	      3531 ns/op	     200 B/op	       4 allocs/op
BenchmarkOpenRecovery-4       	       5	 210032858 ns/op	20088356 B/op	  604032 allocs/op
```

Read: every mutation costs ~2µs, dominated by the unbuffered WAL write; a
`Get` hit costs ~1.8µs (its touch record is smaller than a put's); parallel
`Put`s serialize on the mutex at ~3.5µs each; recovery replays ~100k records
in ~210ms (~2.1µs/record). Numbers are machine- and disk-dependent; rerun
the command above to reproduce.
