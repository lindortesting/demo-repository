package kv

import "testing"

// Requirement: Get on a key that was never Put reports absence, including on
// an empty cache. Boundary case: the zero-content collection.
func TestLRUGetMiss(t *testing.T) {
	c := newLRU(2)
	if v, ok := c.get("nope"); ok || v != "" {
		t.Fatalf("get on empty cache = %q, %v; want \"\", false", v, ok)
	}
}

// Requirement: a Put value is returned by Get. This is the cache's basic
// contract.
func TestLRUPutGet(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	if v, ok := c.get("a"); !ok || v != "1" {
		t.Fatalf("get = %q, %v; want \"1\", true", v, ok)
	}
}

// Requirement: re-Putting an existing key replaces the value without growing
// the cache or creating a duplicate entry.
func TestLRUPutOverwrite(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	c.put("a", "2")
	if c.len() != 1 {
		t.Fatalf("len = %d; want 1", c.len())
	}
	if v, _ := c.get("a"); v != "2" {
		t.Fatalf("get = %q; want \"2\"", v)
	}
}

// Requirement: len tracks distinct live keys, not Put calls.
func TestLRULen(t *testing.T) {
	c := newLRU(4)
	c.put("a", "1")
	c.put("a", "2")
	c.put("b", "3")
	if c.len() != 2 {
		t.Fatalf("len = %d; want 2", c.len())
	}
	c.delete("b")
	if c.len() != 1 {
		t.Fatalf("len after delete = %d; want 1", c.len())
	}
}

// Requirement: when a Put exceeds capacity, the least recently used entry is
// evicted. This is the defining LRU property.
func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	c.put("b", "2")
	c.put("c", "3") // evicts "a"
	if _, ok := c.get("a"); ok {
		t.Fatal("oldest key was not evicted")
	}
	for _, k := range []string{"b", "c"} {
		if _, ok := c.get(k); !ok {
			t.Fatalf("key %q wrongly evicted", k)
		}
	}
}

// Requirement: Get refreshes recency, so a recently read key survives an
// eviction that claims an older one.
func TestLRUGetRefreshesRecency(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	c.put("b", "2")
	c.get("a")      // "b" is now least recently used
	c.put("c", "3") // evicts "b"
	if _, ok := c.get("b"); ok {
		t.Fatal("Get did not refresh recency: \"a\" kept but \"b\" should have been evicted")
	}
	if _, ok := c.get("a"); !ok {
		t.Fatal("recently read key \"a\" was evicted")
	}
}

// Requirement: re-Putting an existing key also refreshes its recency.
func TestLRUPutRefreshesRecency(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	c.put("b", "2")
	c.put("a", "1b") // refresh "a"; "b" becomes oldest
	c.put("c", "3")  // evicts "b"
	if _, ok := c.get("b"); ok {
		t.Fatal("Put did not refresh recency: \"b\" should have been evicted")
	}
	if v, ok := c.get("a"); !ok || v != "1b" {
		t.Fatalf("get(\"a\") = %q, %v; want \"1b\", true", v, ok)
	}
}

// Requirement: Delete removes a key and reports that it was present.
func TestLRUDelete(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	if !c.delete("a") {
		t.Fatal("delete of present key returned false")
	}
	if _, ok := c.get("a"); ok {
		t.Fatal("deleted key still visible")
	}
	if c.len() != 0 {
		t.Fatalf("len = %d; want 0", c.len())
	}
}

// Requirement: Delete of an absent key is a harmless no-op reporting false.
// Callers must be able to delete without checking existence first.
func TestLRUDeleteMissing(t *testing.T) {
	c := newLRU(2)
	if c.delete("nope") {
		t.Fatal("delete of absent key returned true")
	}
}

// Requirement: capacity 1 works — the smallest non-degenerate cache. Each
// new Put evicts the previous entry.
func TestLRUCapacityOne(t *testing.T) {
	c := newLRU(1)
	c.put("a", "1")
	c.put("b", "2")
	if _, ok := c.get("a"); ok {
		t.Fatal("capacity-1 cache kept two keys")
	}
	if v, ok := c.get("b"); !ok || v != "2" {
		t.Fatalf("get(\"b\") = %q, %v; want \"2\", true", v, ok)
	}
}

// Requirement: empty keys and empty values are valid data, not sentinel
// errors. The zero-length boundary must round-trip like any other string.
func TestLRUEmptyKeyAndValue(t *testing.T) {
	c := newLRU(2)
	c.put("", "")
	if v, ok := c.get(""); !ok || v != "" {
		t.Fatalf("get(\"\") = %q, %v; want \"\", true", v, ok)
	}
}

// Requirement: has reports presence without changing eviction order. Delete
// relies on this to decide whether a WAL record is needed, and a recency
// side effect there would silently corrupt eviction order.
func TestLRUHasDoesNotRefreshRecency(t *testing.T) {
	c := newLRU(2)
	c.put("a", "1")
	c.put("b", "2")
	if !c.has("a") {
		t.Fatal("has returned false for present key")
	}
	c.put("c", "3") // must still evict "a", proving has() did not refresh it
	if _, ok := c.get("a"); ok {
		t.Fatal("has() refreshed recency: \"a\" should have been evicted")
	}
}
