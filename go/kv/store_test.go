package kv_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lindortesting/demo-repository/go/kv"
)

func openStore(t *testing.T, capacity int) (*kv.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal")
	s, err := kv.Open(path, capacity)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() }) // double Close after an explicit one is harmless
	return s, path
}

func mustPut(t *testing.T, s *kv.Store, key, value string) {
	t.Helper()
	if err := s.Put(key, value); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func mustGet(t *testing.T, s *kv.Store, key, want string) {
	t.Helper()
	v, ok, err := s.Get(key)
	if err != nil || !ok || v != want {
		t.Fatalf("Get(%q) = %q, %v, %v; want %q, true, nil", key, v, ok, err, want)
	}
}

func mustMiss(t *testing.T, s *kv.Store, key string) {
	t.Helper()
	if v, ok, err := s.Get(key); err != nil || ok {
		t.Fatalf("Get(%q) = %q, %v, %v; want miss with nil error", key, v, ok, err)
	}
}

// reopen closes s and opens a fresh Store on the same WAL, returning it.
func reopen(t *testing.T, s *kv.Store, path string, capacity int) *kv.Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := kv.Open(path, capacity)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	return s2
}

// Requirement: capacity is explicit configuration — a non-positive value is
// a bug, so Open fails fast instead of silently building a degenerate cache.
func TestOpenInvalidCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	for _, capacity := range []int{0, -3} {
		if _, err := kv.Open(path, capacity); err == nil {
			t.Fatalf("Open(capacity=%d) succeeded; want error", capacity)
		}
	}
}

// Requirement: Open on a nonexistent path creates an empty store, and an
// empty WAL replays to an empty cache. Boundary case: nothing to recover.
func TestStoreOpenEmpty(t *testing.T) {
	s, path := openStore(t, 4)
	if s.Len() != 0 {
		t.Fatalf("Len = %d; want 0", s.Len())
	}
	s = reopen(t, s, path, 4)
	if s.Len() != 0 {
		t.Fatalf("Len after reopen = %d; want 0", s.Len())
	}
}

// Requirement: basic round trip through the public API.
func TestStorePutGetAndLen(t *testing.T) {
	s, _ := openStore(t, 4)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	mustGet(t, s, "a", "1")
	mustGet(t, s, "b", "2")
	if s.Len() != 2 {
		t.Fatalf("Len = %d; want 2", s.Len())
	}
}

// Requirement: Get on an absent key reports a miss, not an error.
func TestStoreGetMiss(t *testing.T) {
	s, _ := openStore(t, 4)
	mustMiss(t, s, "ghost")
}

// Requirement: the key-value state survives a process restart. This is the
// WAL's reason to exist.
func TestStoreRecovery(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	mustPut(t, s, "b", "2b") // overwrite before restart
	mustPut(t, s, "c", "3")

	s = reopen(t, s, path, 8)
	mustGet(t, s, "a", "1")
	mustGet(t, s, "b", "2b")
	mustGet(t, s, "c", "3")
	if s.Len() != 3 {
		t.Fatalf("Len after recovery = %d; want 3", s.Len())
	}
}

// Requirement: deletes survive a restart too — a deleted key must not
// resurrect from its earlier Put record.
func TestStoreRecoveryAfterDelete(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	if err := s.Delete("a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	s = reopen(t, s, path, 8)
	mustMiss(t, s, "a")
	mustGet(t, s, "b", "2")
	if s.Len() != 1 {
		t.Fatalf("Len = %d; want 1", s.Len())
	}
}

// Requirement: replay re-applies the capacity bound, so recovery cannot grow
// the cache beyond what the live cache would have held.
func TestStoreRecoveryReplaysEviction(t *testing.T) {
	s, path := openStore(t, 2)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	mustPut(t, s, "c", "3") // evicts "a"

	s = reopen(t, s, path, 2)
	mustMiss(t, s, "a")
	mustGet(t, s, "b", "2")
	mustGet(t, s, "c", "3")
}

// Requirement: replay restores recency exactly, because Get hits are logged
// as touch records. This is the reviewer's scenario: live, the Get protects
// "a" from eviction; after a restart it must still be protected, and the
// same key must be evicted as would have been without the restart.
func TestStoreRecencySurvivesRecovery(t *testing.T) {
	s, path := openStore(t, 2)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	mustGet(t, s, "a", "1") // touch: "a" becomes most recently used, and the WAL knows

	s = reopen(t, s, path, 2)
	mustPut(t, s, "c", "3") // must evict "b", exactly as the live cache would
	mustGet(t, s, "a", "1")
	mustMiss(t, s, "b")
	mustGet(t, s, "c", "3")
}

// Requirement: deleting an absent key is a no-op and must not grow the WAL —
// otherwise missing-key deletes would bloat the log with useless records.
func TestStoreDeleteMissingIsNoop(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := s.Delete("ghost"); err != nil {
		t.Fatalf("Delete of absent key: %v", err)
	}
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi2.Size() != fi.Size() {
		t.Fatalf("WAL grew on no-op delete: %d -> %d", fi.Size(), fi2.Size())
	}
	mustGet(t, s, "a", "1")
}

// Requirement: a torn tail (crash mid-append) must not lose earlier records
// or poison later ones. Replay truncates the garbage and recovery continues.
func TestStoreTornTailTruncated(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x42}); err != nil {
		t.Fatalf("append garbage: %v", err)
	}
	f.Close()

	s2, err := kv.Open(path, 8)
	if err != nil {
		t.Fatalf("Open with torn tail: %v", err)
	}
	t.Cleanup(func() { s2.Close() })

	// Stat before any Get: hits append touch records and grow the file.
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi2.Size() != fi.Size() {
		t.Fatalf("torn tail not truncated: size %d; want %d", fi2.Size(), fi.Size())
	}
	mustGet(t, s2, "a", "1")
	mustGet(t, s2, "b", "2")

	// Appends after recovery must follow the last good record, not the garbage.
	mustPut(t, s2, "c", "3")
	s2 = reopen(t, s2, path, 8)
	mustGet(t, s2, "a", "1")
	mustGet(t, s2, "b", "2")
	mustGet(t, s2, "c", "3")
}

// Requirement: a record whose CRC fails (bit rot) ends recovery there: all
// earlier records are kept, that record and everything after it are dropped.
// Keys and values are one byte each, so every record is exactly 15 bytes and
// record 2's key byte sits at offset 15+13.
func TestStoreCorruptMiddleRecordDropsTail(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	mustPut(t, s, "c", "3")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte{'X'}, 28); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	f.Close()

	s2, err := kv.Open(path, 8)
	if err != nil {
		t.Fatalf("Open with corrupt record: %v", err)
	}
	t.Cleanup(func() { s2.Close() })

	// Stat before any Get: hits append touch records and grow the file.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != 15 {
		t.Fatalf("size after truncation = %d; want 15 (one record)", fi.Size())
	}
	mustGet(t, s2, "a", "1")
	mustMiss(t, s2, "b")
	mustMiss(t, s2, "c")
}

// Requirement: after Close, mutations fail loudly instead of pretending to
// persist. Get hits fail too (the recency touch cannot be logged); Get
// misses still report absence.
func TestStorePutAfterClose(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Put("b", "2"); err == nil {
		t.Fatal("Put after Close succeeded; want error")
	}
	if err := s.Delete("a"); err == nil {
		t.Fatal("Delete after Close succeeded; want error")
	}
	if _, ok, err := s.Get("a"); err == nil || ok {
		t.Fatalf("Get hit after Close = _, %v, %v; want miss with error", ok, err)
	}
	mustMiss(t, s, "ghost")

	// The failed mutations must not have reached the log.
	s2, err := kv.Open(path, 8)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	mustGet(t, s2, "a", "1")
	mustMiss(t, s2, "b")
}

// Requirement: the empty string is a valid key and survives recovery.
// Boundary case: zero-length fields in the WAL framing.
func TestStoreEmptyKeyRoundTrip(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "", "empty")
	s = reopen(t, s, path, 8)
	mustGet(t, s, "", "empty")
}

// Requirement: large values frame and recover correctly — the 4-byte length
// prefix must handle payloads far beyond one byte or one page.
func TestStoreLargeValueRoundTrip(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	s, path := openStore(t, 4)
	mustPut(t, s, "big", big)
	s = reopen(t, s, path, 4)
	mustGet(t, s, "big", big)
}

// Requirement: a Get miss changes nothing and must not append to the WAL —
// otherwise read-heavy workloads on absent keys would bloat the log. Only
// hits pay for a touch record.
func TestStoreGetMissWritesNothing(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	mustMiss(t, s, "ghost")
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi2.Size() != fi.Size() {
		t.Fatalf("WAL grew on a Get miss: %d -> %d", fi.Size(), fi2.Size())
	}
}

// Requirement: a Get hit logs a touch record, so the WAL grows — the price
// of exact recency recovery. This pins the contract so the cost is
// deliberate, not accidental.
func TestStoreGetHitAppendsTouch(t *testing.T) {
	s, path := openStore(t, 8)
	mustPut(t, s, "a", "1")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	mustGet(t, s, "a", "1")
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi2.Size() <= fi.Size() {
		t.Fatalf("WAL did not grow on a Get hit: %d -> %d", fi.Size(), fi2.Size())
	}
}

// Requirement: Sync flushes the WAL and reports no error in normal operation.
func TestStoreSync(t *testing.T) {
	s, _ := openStore(t, 4)
	mustPut(t, s, "a", "1")
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// Requirement: Store is safe for concurrent use (run with -race), and the
// WAL faithfully records interleaved mutations — after a restart every key
// holds exactly the value its owner last wrote. Each goroutine owns 50
// disjoint keys so the final state is deterministic; all goroutines also
// hammer one shared "hot" key to force same-key contention.
func TestStoreConcurrent(t *testing.T) {
	const workers, keysPerWorker = 8, 50
	s, path := openStore(t, workers*keysPerWorker+1) // +1 for "hot"; nothing evicted

	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				key := fmt.Sprintf("w%d-k%d", g, i%keysPerWorker)
				switch i % 4 {
				case 0:
					s.Get(key)
				case 1:
					s.Delete(key)
				case 2:
					s.Put("hot", strconv.Itoa(g))
				default:
					s.Put(key, strconv.Itoa(i))
				}
			}
			// Deterministic final state for this worker's keys.
			for i := 0; i < keysPerWorker; i++ {
				s.Put(fmt.Sprintf("w%d-k%d", g, i), strconv.Itoa(g))
			}
		}(g)
	}
	wg.Wait()

	assertFinal := func(s *kv.Store) {
		t.Helper()
		for g := 0; g < workers; g++ {
			for i := 0; i < keysPerWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", g, i)
				if v, ok, err := s.Get(key); err != nil || !ok || v != strconv.Itoa(g) {
					t.Fatalf("Get(%q) = %q, %v, %v; want %q, true, nil", key, v, ok, err, strconv.Itoa(g))
				}
			}
		}
	}
	assertFinal(s)

	s = reopen(t, s, path, workers*keysPerWorker+1)
	assertFinal(s)
}

func newBenchStore(b *testing.B, capacity int) *kv.Store {
	b.Helper()
	s, err := kv.Open(filepath.Join(b.TempDir(), "wal"), capacity)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// Benchmark: sequential Put cost — one WAL append plus one LRU insert.
func BenchmarkStorePut(b *testing.B) {
	s := newBenchStore(b, 1<<20)
	for i := 0; i < b.N; i++ {
		if err := s.Put(strconv.Itoa(i), "value"); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: cache-hit Get cost on a preloaded store — one WAL touch append
// plus one LRU lookup, the price of exact recency recovery.
func BenchmarkStoreGet(b *testing.B) {
	const n = 10000
	s := newBenchStore(b, n)
	for i := 0; i < n; i++ {
		if err := s.Put(strconv.Itoa(i), "value"); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := s.Get(strconv.Itoa(i % n)); err != nil || !ok {
			b.Fatalf("Get: ok=%v err=%v", ok, err)
		}
	}
}

// Benchmark: Put under concurrent contention — the mutex serializes
// appends, so this measures lock handoff more than raw throughput.
func BenchmarkStorePutParallel(b *testing.B) {
	s := newBenchStore(b, 1<<20)
	var n atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Put(strconv.FormatInt(n.Add(1), 10), "value"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark: recovery cost — Open replays a WAL of 100k records.
func BenchmarkOpenRecovery(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal")
	const records = 100000
	s, err := kv.Open(path, records)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	for i := 0; i < records; i++ {
		if err := s.Put(strconv.Itoa(i), "value"); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := kv.Open(path, records)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		if err := s.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}
