package kv

import (
	"errors"
	"path/filepath"
	"testing"
)

var errBoom = errors.New("kv test: injected fault")

// Requirement (regression for a review): a WAL has exactly one writer at a
// time. A second Open of the same path must fail while the first store is
// open and succeed after Close — two stores sharing one file diverge
// silently (stale size offsets, unlogged deletes, cross-store truncation).
// Skipped where the platform cannot enforce the lock.
func TestStoreExclusiveWALOwnership(t *testing.T) {
	if !ownershipEnforced {
		t.Skip("exclusive WAL ownership is not enforced on this platform")
	}
	path := filepath.Join(t.TempDir(), "wal")
	s, err := Open(path, 8)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := Open(path, 8); err == nil {
		t.Fatal("second Open of the same WAL succeeded; want exclusive-ownership error")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(path, 8)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	if err := s2.Put("k", "v"); err != nil {
		t.Fatalf("Put after reacquiring the WAL: %v", err)
	}
}

// faultyFile injects failures into WAL operations to exercise error paths
// that a real file cannot produce on demand.
type faultyFile struct {
	walFile
	failWrite, failTruncate bool
}

func (f *faultyFile) Write(p []byte) (int, error) {
	if f.failWrite {
		return 0, errBoom
	}
	return f.walFile.Write(p)
}

func (f *faultyFile) Truncate(size int64) error {
	if f.failTruncate {
		return errBoom
	}
	return f.walFile.Truncate(size)
}

// Requirement (regression for a critical review): if a WAL write fails AND
// the rollback truncate fails, the store must go read-only instead of
// acknowledging later writes that recovery would silently discard at the
// torn tail. The sticky error persists for the store's lifetime — even after
// the fault clears — and repair is close+reopen, where replay truncates the
// partial record.
func TestStoreBrokenAfterFailedRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s, err := Open(path, 8)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Put("a", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ff := &faultyFile{walFile: s.wal, failWrite: true, failTruncate: true}
	s.wal = ff

	// The faulted mutation itself reports the write error and does not
	// touch the cache.
	if err := s.Put("b", "2"); !errors.Is(err, errBoom) {
		t.Fatalf("Put with injected failure = %v; want %v", err, errBoom)
	}
	// Every later mutation is rejected with the sticky error, even reads
	// that would need to log a touch.
	ops := []struct {
		name string
		op   func() error
	}{
		{"Put", func() error { return s.Put("c", "3") }},
		{"Delete", func() error { return s.Delete("a") }},
		{"Get hit", func() error { _, _, err := s.Get("a"); return err }},
	}
	for _, tc := range ops {
		if err := tc.op(); !errors.Is(err, errBoom) {
			t.Errorf("%s on broken store = %v; want sticky error", tc.name, err)
		}
	}
	// Reads that need no WAL stay available.
	if v, ok, err := s.Get("ghost"); err != nil || ok {
		t.Fatalf("Get miss on broken store = %q, %v, %v; want clean miss", v, ok, err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len on broken store = %d; want 1", s.Len())
	}
	// Clearing the fault does not clear the stickiness.
	ff.failWrite, ff.failTruncate = false, false
	if err := s.Put("d", "4"); !errors.Is(err, errBoom) {
		t.Fatalf("Put after fault cleared = %v; want sticky error", err)
	}

	// Repair: close and reopen. Replay truncates whatever partial record
	// the failed write left; only the acknowledged write ("a") survives.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(path, 8)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	if v, ok, err := s2.Get("a"); err != nil || !ok || v != "1" {
		t.Fatalf("Get(a) after reopen = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok, _ := s2.Get(k); ok {
			t.Fatalf("key %q present after reopen; want discarded", k)
		}
	}
	if err := s2.Put("e", "5"); err != nil {
		t.Fatalf("Put after reopen = %v; want nil (store repaired)", err)
	}
}
