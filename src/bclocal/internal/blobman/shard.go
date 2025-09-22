package blobman

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"sync"
)

// CreateShard creates a new shard in the filesystem.
// If the shard already exists, than os.ErrExist is returned.
// The root should be the global root.
func CreateShard(root *os.Root, prefix ShardID) (*Shard, error) {
	p, err := prefix.Path()
	if err != nil {
		return nil, err
	}
	if err := root.Mkdir(p, 0o755); err != nil {
		return nil, err
	}
	root2, err := root.OpenRoot(p)
	if err != nil {
		return nil, err
	}
	return newShard(root2), nil
}

// OpenShard opens a shard that already exists in the filesystem.
// The root should be the global root.
func OpenShard(root *os.Root, prefix ShardID) (*Shard, error) {
	p, err := prefix.Path()
	if err != nil {
		return nil, err
	}
	root2, err := root.OpenRoot(p)
	if err != nil {
		return nil, err
	}
	return newShard(root2), nil
}

// Shard is a directory on disk containing table and pack files, potentially across multiple generations.
// Each shard is independent of every other shard, and there are no consistency guarantees between shards.
// The Shard has no information about where it is in the trie.
type Shard struct {
	// rootDir is the directory for the shard
	// this is the only field which must not be zero.
	// everything else will be set during load.
	rootDir *os.Root

	mu     sync.RWMutex
	gen    uint64
	loaded bool // structural load completed (children presence discovered)
	tab    Table
	pack   Pack
	bfs    []bloom2048

	children [256]*Shard
}

func newShard(rootDir *os.Root) *Shard {
	return &Shard{rootDir: rootDir}
}

func (s *Shard) Close() error {
	if err := s.pack.Close(); err != nil {
		return err
	}
	if err := s.tab.Close(); err != nil {
		return err
	}
	return s.rootDir.Close()
}

func (dst *Shard) load(maxTableSize, maxPackSize uint32) error {
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
	tf, err := LoadTableFile(dst.rootDir, dst.gen)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			tf, err = CreateTableFile(dst.rootDir, dst.gen, maxTableSize)
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
		packFile, err = CreatePackFile(dst.rootDir, dst.gen, maxPackSize)
		if err != nil {
			return err
		}
	} else {
		packFile, err = LoadPackFile(dst.rootDir, dst.gen)
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
	// need to add all the entries to the bloom filters
	for i := uint32(0); i < table.Len(); i++ {
		ent := table.Slot(i)
		bfIdx := filterIndex(i)
		if len(dst.bfs) <= bfIdx {
			dst.bfs = append(dst.bfs, bloom2048{})
		}
		dst.bfs[bfIdx].add(ent.Key)
	}
	// load the children
	children, err := dst.findChildren()
	if err != nil {
		return err
	}
	dst.children = children

	dst.loaded = true
	return nil
}

// findChildren looks for child shards in the filesystem.
func (dst *Shard) findChildren() ([256]*Shard, error) {
	var children [256]*Shard
	entries, err := fs.ReadDir(dst.rootDir.FS(), ".")
	if err != nil {
		return children, err
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		childIdx, err := func() (uint8, error) {
			data, err := hex.DecodeString(ent.Name())
			if err != nil {
				return 0, err
			}
			if len(data) != 1 {
				return 0, fmt.Errorf("child shard must be 1 byte decoded")
			}
			return uint8(data[0]), nil
		}()
		if err != nil {
			return children, fmt.Errorf("invalid shard filename %s: %w", ent.Name(), err)
		}
		childRoot, err := dst.rootDir.OpenRoot(ent.Name())
		if err != nil {
			return children, err
		}
		children[childIdx] = &Shard{rootDir: childRoot}
	}
	return children, nil
}

// localExists checks if the key is in this shard.
// localExists must be called with the shard read lock held.
func (s *Shard) localExists(key Key) bool {
	_, found := s.localScan(key)
	return found
}

// localScan must be called with the shard read lock held.
// localScan iterates through each bloom filter, if it gets a hit, then it iterates through the corresponding
// range in the table.
func (s *Shard) localScan(key Key) (TableEntry, bool) {
	for filterIdx := range s.bfs {
		if !s.bfs[filterIdx].contains(key) {
			continue
		}
		beg := slotBeg(filterIdx)
		end := slotEnd(filterIdx)
		for slot := beg; slot < end && slot < s.tab.Len(); slot++ {
			ent := s.tab.Slot(slot)
			if ent.IsTombstone() {
				continue
			}
			if ent.Key == key {
				return ent, true
			}
		}
	}
	return TableEntry{}, false
}

// localAppend appends the key and data to the table.
// this function does not check if the key already exists, the caller must do that.
// It returns false if the table is full or the pack is full.
// localAppend must be called with the shard write lock held.
func (s *Shard) localAppend(key Key, data []byte) bool {
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
	if slotIdx == math.MaxUint32 {
		return false
	}
	bfIdx := filterIndex(slotIdx)
	if len(s.bfs) <= bfIdx {
		s.bfs = append(s.bfs, bloom2048{})
	}
	s.bfs[bfIdx].add(key)
	return true
}

func (s *Shard) localDelete(key Key) bool {
	for i := uint32(0); i < s.tab.Len(); i++ {
		ent := s.tab.Slot(i)
		if ent.Key == key {
			s.tab.Tombstone(i)
			return true
		}
	}
	return false
}

func (s *Shard) createChild(childIdx uint8) (*Shard, error) {
	p := hex.EncodeToString([]byte{childIdx})
	if err := s.rootDir.Mkdir(p, 0o755); err != nil {
		return nil, err
	}
	childRoot, err := s.rootDir.OpenRoot(p)
	if err != nil {
		return nil, err
	}
	return newShard(childRoot), nil
}

// getOrCreateChild gets the child if it exists, otherwise creates it.
// it does not lock the shard.
func (s *Shard) getOrCreateChild(childIdx uint8) (*Shard, error) {
	if child := s.children[childIdx]; child != nil {
		return child, nil
	}
	child, err := s.createChild(childIdx)
	if err != nil {
		return nil, err
	}
	s.children[childIdx] = child
	return child, nil
}
