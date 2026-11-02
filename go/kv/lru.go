package kv

import "container/list"

// lru is a fixed-capacity least-recently-used string cache with O(1)
// operations. It is pure bookkeeping — no I/O, no locking — and is not safe
// for concurrent use; Store synchronizes it.
type lru struct {
	capacity int
	ll       *list.List // front is most recently used; values are lruEntry
	items    map[string]*list.Element
}

type lruEntry struct {
	key, value string
}

func newLRU(capacity int) *lru {
	return &lru{capacity: capacity, ll: list.New(), items: make(map[string]*list.Element)}
}

// get returns the value for key and marks it most recently used.
func (c *lru) get(key string) (string, bool) {
	e, ok := c.items[key]
	if !ok {
		return "", false
	}
	c.ll.MoveToFront(e)
	return e.Value.(lruEntry).value, true
}

// put inserts or updates key, marks it most recently used, and evicts the
// least recently used entry when the cache exceeds its capacity.
func (c *lru) put(key, value string) {
	if e, ok := c.items[key]; ok {
		e.Value = lruEntry{key, value}
		c.ll.MoveToFront(e)
		return
	}
	c.items[key] = c.ll.PushFront(lruEntry{key, value})
	if c.ll.Len() > c.capacity {
		back := c.ll.Back()
		delete(c.items, back.Value.(lruEntry).key)
		c.ll.Remove(back)
	}
}

// delete removes key, reporting whether it was present.
func (c *lru) delete(key string) bool {
	e, ok := c.items[key]
	if !ok {
		return false
	}
	c.ll.Remove(e)
	delete(c.items, key)
	return true
}

// has reports whether key is present without affecting recency.
func (c *lru) has(key string) bool {
	_, ok := c.items[key]
	return ok
}

func (c *lru) len() int { return len(c.items) }
