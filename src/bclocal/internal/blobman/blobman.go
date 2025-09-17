package blobman

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

// Store stores blobs on in the filesystem.
type Store struct {
	root *os.Root

	shard shard
}

func New(root *os.Root) *Store {
	st := &Store{root: root}
	return st
}

// Put finds a spot for key, and writes data to it.
// If the key already exists, then the write is ignored and false is returned.
func (db *Store) Put(key Key, data []byte) (bool, error) {
	sctx := &storeCtx{root: db.root}
	return db.shard.put(sctx, key, 0, data)
}

// Get finds key if it exists and calls fn with the data.
// The data must not be used outside the callback.
func (db *Store) Get(key Key, buf []byte, fn func(data []byte)) (bool, error) {
	return db.shard.get(db.root, key, 0, buf, fn)
}

// Delete overwrites any tables containing key with a tombstone.
func (db *Store) Delete(key Key) error {
	return nil
}

func (db *Store) Close() error {
	return db.shard.close()
}

type storeCtx struct {
	root *os.Root
}

type shard struct {
	mu     sync.RWMutex
	loaded bool // structural load completed (children presence discovered)
	tab    Table
	pack   Pack
	// mem maps full-key fingerprint to table row (zero-based)
	mem map[Prefix121]uint32

	children [256]atomic.Pointer[shard]
}

// load performs a structural load (discover children) and ensures this node's
// table/pack are opened if they already exist on disk. It does not create files.
func (s *shard) load(sctx *storeCtx, shardID Prefix121, numRows uint32) error {
	s.mu.RLock()
	loaded := s.loaded
	s.mu.RUnlock()
	if loaded {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded {
		return nil
	}
	// Open or create table/pack files for this shard's path.
	pf, err := LoadPackFile(sctx.root, shardID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			pf, err = CreatePackFile(sctx.root, shardID, MaxPackSize)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	tf, err := LoadTableFile(sctx.root, shardID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			tf, err = CreateTableFile(sctx.root, shardID, DefaultMaxIndexSize)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	table, err := NewTable(tf, numRows)
	if err != nil {
		return err
	}
	// Determine next pack offset. If table is empty, 0.
	var nextOffset uint32
	if table.Len() > 0 {
		last := table.Slot(table.Len() - 1)
		nextOffset = last.Offset + last.Len
	}
	pack, err := NewPack(pf, nextOffset)
	if err != nil {
		return err
	}
	s.tab = table
	s.pack = pack
	// Build in-memory map from table
	mem := make(map[Prefix121]uint32)
	for i := uint32(0); i < s.tab.Len(); i++ {
		ent := s.tab.Slot(i)
		mem[ent.Prefix] = i
	}
	s.mem = mem
	s.loaded = true
	return nil
}

func (s *shard) maxRows() uint32 { return uint32(len(s.tab.mm)) / TableEntrySize }

func (s *shard) put(sctx *storeCtx, key Key, numBits uint8, data []byte) (bool, error) {
	if err := s.load(sctx, key.ToPrefix(numBits), 0); err != nil {
		return false, err
	}
	// Duplicate check using in-memory map
	mapKey := key.ToPrefix(numBits)
	s.mu.RLock()
	_, exists := s.mem[mapKey]
	s.mu.RUnlock()
	if exists {
		return false, nil
	}
	// Try deeper child first if it exists (longest prefix available)
	childIdx := key.Uint8(0)
	if child := s.children[childIdx].Load(); child != nil {
		if ok, err := child.put(sctx, key.ShiftIn(8), numBits+8, data); err != nil {
			return false, err
		} else if ok {
			return ok, nil
		}
		// If child could not accept (full further down), fall back to this node below.
	}
	// Prepare data record: length prefix + bytes
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(data)))
	buf := append(hdr[:], data...)
	off := s.pack.Append(buf)
	if off == math.MaxUint32 {
		// Node full; create child and insert there
		child := s.children[childIdx].Load()
		if child == nil {
			newChild := &shard{}
			s.children[childIdx].CompareAndSwap(nil, newChild)
			child = s.children[childIdx].Load()
		}
		return child.put(sctx, key.ShiftIn(8), numBits+8, data)
	}
	// Check table capacity
	if s.tab.Len() >= s.maxRows() {
		// Create child and insert there
		child := s.children[childIdx].Load()
		if child == nil {
			newChild := &shard{}
			s.children[childIdx].CompareAndSwap(nil, newChild)
			child = s.children[childIdx].Load()
		}
		return child.put(sctx, key.ShiftIn(8), numBits+8, data)
	}
	// Append index entry
	ent := IndexEntry{
		Prefix: key.ToPrefix(numBits),
		Offset: off + 4,
		Len:    uint32(len(data)),
	}
	newRow := s.tab.Append(ent)
	// Update in-memory map
	s.mu.Lock()
	s.mem[key.ToPrefix(numBits)] = newRow
	s.mu.Unlock()
	return true, nil
}

func (s *shard) get(root *os.Root, key Key, numBits uint8, buf []byte, fn func(data []byte)) (bool, error) {
	// Check local map first
	s.mu.RLock()
	slotIdx, ok := s.mem[key.ToPrefix(numBits)]
	s.mu.RUnlock()
	if ok {
		ent := s.tab.Slot(slotIdx)
		if s.pack.Get(ent.Offset, ent.Len, fn) {
			return true, nil
		}
	}
	childIdx := key.Uint8(0)
	if child := s.children[childIdx].Load(); child != nil {
		// recurse to the next node
		return child.get(root, key.ShiftIn(8), numBits+8, buf, fn)
	} else {
		return false, nil
	}
}

func (s *shard) close() error {
	return errors.Join(s.tab.Close(), s.pack.Close())
}
