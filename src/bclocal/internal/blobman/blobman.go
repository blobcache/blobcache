package blobman

import (
	"errors"
	"math"
	"os"
	"sync"
)

// Store stores blobs on in the filesystem.
type Store struct {
	root *os.Root
	// maxTableLen is the maximum number of rows in a table.
	maxTableLen uint32
	// maxPackSize is the maximum size of a pack in bytes.
	maxPackSize uint32

	shard shard
}

func New(root *os.Root) *Store {
	st := &Store{
		root:        root,
		maxTableLen: DefaultMaxTableLen,
		maxPackSize: DefaultMaxPackSize,
	}
	return st
}

// Put finds a spot for key, and writes data to it.
// If the key already exists, then the write is ignored and false is returned.
func (db *Store) Put(key Key, data []byte) (bool, error) {
	ok, err := db.put(&db.shard, key, 0, data)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Get finds key if it exists and calls fn with the data.
// The data must not be used outside the callback.
func (db *Store) Get(key Key, buf []byte, fn func(data []byte)) (bool, error) {
	return db.get(&db.shard, key, 0, buf, fn)
}

// Delete overwrites any tables containing key with a tombstone.
func (db *Store) Delete(key Key) error {
	return nil
}

func (db *Store) Flush() error {
	return db.flushShard(&db.shard)
}

func (db *Store) flushShard(sh *shard) error {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if !sh.loaded {
		return nil
	}
	if err := errors.Join(sh.tab.Flush(), sh.pack.Flush()); err != nil {
		return err
	}
	for _, child := range sh.children {
		if err := db.flushShard(child); err != nil {
			return err
		}
	}
	return nil
}

func (db *Store) Close() error {
	return db.shard.close()
}

func (db *Store) maxTableSize() uint32 {
	return TableHeaderSize + db.maxTableLen*TableEntrySize
}

// loadShard ensures that the shard is loaded and ready to use.
func (db *Store) loadShard(dst *shard, shardID Prefix120) error {
	// quick check with the read lock
	dst.mu.RLock()
	loaded := dst.loaded
	dst.mu.RUnlock()
	if loaded {
		return nil
	}
	// now get the write lock
	dst.mu.Lock()
	defer dst.mu.Unlock()
	if dst.loaded {
		// check one more time
		return nil
	}

	var createdTable bool
	tf, err := LoadTableFile(db.root, shardID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			tf, err = CreateTableFile(db.root, shardID, db.maxTableSize())
			if err != nil {
				return err
			}
			createdTable = true
		} else {
			return err
		}
	}
	table, err := NewTable(tf)
	if err != nil {
		return err
	}
	var packFile *os.File
	if createdTable {
		packFile, err = CreatePackFile(db.root, shardID, db.maxPackSize)
		if err != nil {
			return err
		}
	} else {
		packFile, err = LoadPackFile(db.root, shardID)
		if err != nil {
			return err
		}
	}
	// Determine next pack offset. If table is empty, 0.
	var nextOffset uint32
	if table.Len() > 0 {
		last := table.Slot(table.Len() - 1)
		nextOffset = last.Offset + last.Len
	}
	pack, err := NewPack(packFile, nextOffset)
	if err != nil {
		return err
	}
	dst.tab = table
	dst.pack = pack
	dst.loaded = true
	return nil
}

// put recursively traverses the trie, and inserts key and data into the appropriate shard.
func (db *Store) put(sh *shard, key Key, numBits uint8, data []byte) (bool, error) {
	if err := db.loadShard(sh, key.ToPrefix(numBits)); err != nil {
		return false, err
	}

	childIdx := key.Uint8(int(numBits / 8))
	var child *shard
	if done, changed := func() (changed bool, done bool) {
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		if found := sh.localExists(key); found {
			// alread exists, nothing to do.
			return true, false
		}
		// check if there is the child exists
		child = sh.children[childIdx]
		if child == nil {
			// doesn't exist, so append to the local table.
			if ok := sh.localAppend(key, data); ok {
				return true, true
			}
		}
		// was not able to append locally, so return false.
		return false, false
	}(); done {
		return changed, nil
	}
	if child != nil {
		return db.put(child, key, numBits+8, data)
	}

	sh.mu.Lock()
	child = sh.getOrCreateChild(childIdx)
	sh.mu.Unlock()
	return db.put(child, key, numBits+8, data)
}

func (db *Store) get(sh *shard, key Key, numBits uint8, buf []byte, fn func(data []byte)) (bool, error) {
	if err := db.loadShard(sh, key.ToPrefix(numBits)); err != nil {
		return false, err
	}

	if found := func() bool {
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		ent, found := sh.localScan(key)
		if found {
			return sh.pack.Get(ent.Offset, ent.Len, fn)
		}
		return false
	}(); found {
		return true, nil
	}

	childIdx := key.Uint8(int(numBits / 8))
	sh.mu.RLock()
	child := sh.children[childIdx]
	sh.mu.RUnlock()
	if child == nil {
		return false, nil
	}
	return db.get(child, key, numBits+8, buf, fn)
}

type shard struct {
	mu     sync.RWMutex
	loaded bool // structural load completed (children presence discovered)
	tab    Table
	pack   Pack

	children [256]*shard
}

// localExists checks if the key is in this shard.
func (s *shard) localExists(key Key) bool {
	_, found := s.localScan(key)
	return found
}

func (s *shard) localScan(key Key) (TableEntry, bool) {
	for i := uint32(0); i < s.tab.Len(); i++ {
		ent := s.tab.Slot(i)
		if ent.Key == key {
			return ent, true
		}
	}
	return TableEntry{}, false
}

// localAppend appends the key and data to the table.
// this function does not check if the key already exists, the caller must do that.
// It returns false if the table is full or the pack is full.
func (s *shard) localAppend(key Key, data []byte) bool {
	off := s.pack.Append(data)
	if off == math.MaxUint32 {
		return false
	}
	ent := TableEntry{
		Key:    key,
		Offset: off,
		Len:    uint32(len(data)),
	}
	slotIdx := s.tab.Append(ent)
	return slotIdx != math.MaxUint32
}

// getOrCreateChild gets the child if it exists, otherwise creates it.
// it does not lock the shard.
func (s *shard) getOrCreateChild(childIdx uint8) *shard {
	if child := s.children[childIdx]; child != nil {
		return child
	}
	child := &shard{}
	s.children[childIdx] = child
	return child
}

func (s *shard) close() error {
	return errors.Join(s.tab.Close(), s.pack.Close())
}
