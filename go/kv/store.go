package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL record layout:
//
//	crc32(4) | op(1) | keyLen(4) | valLen(4) | key | value
//
// The CRC covers everything after itself. A record that fails to decode or
// whose CRC mismatches is treated as a torn tail: replay stops there and the
// file is truncated to the last good record, so later appends never follow
// garbage.
const (
	opPut    byte = 1
	opDelete byte = 2

	headerSize = 4 + 1 + 4 + 4
)

// errTrunc marks a torn or corrupt WAL tail; replay truncates at the last
// good record instead of failing.
var errTrunc = errors.New("kv: wal torn or corrupt tail")

// Store is a thread-safe string key-value cache: a fixed-capacity LRU in
// front of an append-only write-ahead log. Call Open to create one.
type Store struct {
	mu   sync.Mutex
	lru  *lru
	wal  *os.File
	size int64 // end of the last good WAL record
}

// Open opens (creating if necessary) the WAL at path, replays it into a
// fresh cache of the given capacity, and returns the ready Store.
func Open(path string, capacity int) (*Store, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("kv: capacity must be positive, got %d", capacity)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, err
	}
	s := &Store{lru: newLRU(capacity), wal: f}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// Get returns the value for key, or ok=false, and marks it most recently
// used. The recency change is in-memory only; it is not recovered.
func (s *Store) Get(key string) (value string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.get(key)
}

// Put inserts or updates key. The write is appended to the WAL before the
// cache is touched, and the cache is left unchanged if the append fails.
func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.append(opPut, key, value); err != nil {
		return err
	}
	s.lru.put(key, value)
	return nil
}

// Delete removes key. Deleting an absent key is a no-op that touches neither
// the log nor the cache.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.lru.has(key) {
		return nil
	}
	if err := s.append(opDelete, key, ""); err != nil {
		return err
	}
	s.lru.delete(key)
	return nil
}

// Len returns the number of cached entries.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.len()
}

// Sync flushes the WAL to stable storage.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Sync()
}

// Close closes the WAL. The in-memory cache stays readable via Get; Put and
// Delete fail afterwards.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}

// append writes one record to the end of the WAL. On failure the file is
// truncated back to the last good record so no partial record survives.
func (s *Store) append(op byte, key, value string) error {
	buf := make([]byte, headerSize+len(key)+len(value))
	buf[4] = op
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[9:], uint32(len(value)))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)
	binary.LittleEndian.PutUint32(buf, crc32.ChecksumIEEE(buf[4:]))
	if _, err := s.wal.Write(buf); err != nil {
		s.wal.Truncate(s.size) // drop a possibly partial record
		return err
	}
	s.size += int64(len(buf))
	return nil
}

// replay rebuilds the cache from the WAL and truncates any torn or corrupt
// tail, so future appends follow only complete records.
func (s *Store) replay() error {
	fi, err := s.wal.Stat()
	if err != nil {
		return err
	}
	hdr := make([]byte, headerSize)
	for s.size < fi.Size() {
		n, err := s.replayOne(hdr, fi.Size())
		if errors.Is(err, errTrunc) {
			break
		}
		if err != nil {
			return err
		}
		s.size += n
	}
	if s.size < fi.Size() {
		return s.wal.Truncate(s.size)
	}
	return nil
}

// replayOne validates and applies the record at s.size, returning its length.
// Validation failures and end-of-file map to errTrunc; other read errors
// propagate.
func (s *Store) replayOne(hdr []byte, fileSize int64) (int64, error) {
	if _, err := s.wal.ReadAt(hdr, s.size); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, errTrunc // short header at end of file
		}
		return 0, err
	}
	klen := binary.LittleEndian.Uint32(hdr[5:])
	vlen := binary.LittleEndian.Uint32(hdr[9:])
	n := int64(headerSize) + int64(klen) + int64(vlen)
	if n > fileSize-s.size {
		return 0, errTrunc // header claims more than the file holds
	}
	payload := make([]byte, int(klen)+int(vlen))
	if _, err := s.wal.ReadAt(payload, s.size+headerSize); err != nil {
		return 0, err
	}
	crc := crc32.NewIEEE()
	crc.Write(hdr[4:])
	crc.Write(payload)
	if binary.LittleEndian.Uint32(hdr) != crc.Sum32() {
		return 0, errTrunc
	}
	switch key := string(payload[:klen]); hdr[4] {
	case opPut:
		s.lru.put(key, string(payload[klen:]))
	case opDelete:
		s.lru.delete(key)
	default:
		return 0, errTrunc
	}
	return n, nil
}
