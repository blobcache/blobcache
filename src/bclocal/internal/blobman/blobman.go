package blobman

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Store stores blobs on in the filesystem.
type Store struct {
	root *os.Root
	// maxTableLen is the maximum number of rows in a table.
	maxTableLen uint32
	// maxPackSize is the maximum size of a pack in bytes.
	maxPackSize uint32

	trie trie
}

func New(root *os.Root) *Store {
	st := &Store{
		root:        root,
		maxTableLen: DefaultMaxTableLen,
		maxPackSize: DefaultMaxPackSize,
		trie:        trie{shard: newShard(root)},
	}
	return st
}

// Put finds a spot for key, and writes data to it.
// If the key already exists, then the write is ignored and false is returned.
// If some data with key does not exist in the system after this call, then an error is returned.
func (db *Store) Put(key Key, data []byte) (bool, error) {
	if key == (Key{}) {
		return false, fmt.Errorf("blobman.Put: cannot insert the zero key")
	}
	if len(data) > int(db.maxPackSize) {
		return false, fmt.Errorf("data is too large to fit in a pack maxPackSize=%d len(data)=%d", db.maxPackSize, len(data))
	}
	return db.putLoop(&db.trie, key, 0, data)
}

func (db *Store) putLoop(tr *trie, key Key, depth int, data []byte) (bool, error) {
	for range 128 {
		inserted, next, err := db.put(tr, key, depth, data)
		if err != nil {
			return false, err
		}
		if next == nil {
			return inserted, nil
		}
		// If we are moving to a different shard, it will be a child, increment the depth.
		if next != tr {
			depth++
		}
		tr = next
	}
	return false, fmt.Errorf("blobman.Put: the trie iteration limit was reached.  This is a bug, and this error is prefferable to spinning forever")
}

// Get finds key if it exists and calls fn with the data.
// The data must not be used outside the callback.
func (db *Store) Get(key Key, fn func(data []byte)) (bool, error) {
	return db.get(&db.trie, key, 0, fn)
}

// Delete overwrites any tables containing key with a tombstone.
func (db *Store) Delete(key Key) error {
	for tr := &db.trie; tr != nil; {
		next, err := db.delete(tr, key, 0)
		if err != nil {
			return err
		}
		tr = next
	}
	return nil
}

func (db *Store) Flush() error {
	eg := errgroup.Group{}
	eg.SetLimit(runtime.GOMAXPROCS(0))
	db.trie.walk(func(shard *Shard) {
		eg.Go(func() error {
			return shard.Flush()
		})
	})
	return eg.Wait()
}

// Maintain performs background maintenance tasks on the trie.
func (db *Store) Maintain() error {
	eg := errgroup.Group{}
	eg.SetLimit(1)
	db.trie.walk(func(shard *Shard) {
		eg.Go(func() error {
			return shard.load(db.maxTableSize(), db.maxPackSize)
		})
	})
	return eg.Wait()
}

func (db *Store) Close() error {
	if err := db.Flush(); err != nil {
		return err
	}
	db.trie.walk(func(shard *Shard) {
		shard.Close()
	})
	return nil
}

func (db *Store) maxTableSize() uint32 {
	return db.maxTableLen * TableEntrySize
}

// trieChild loads a child from the trie.
// tr is the parent trie to look in.
func (db *Store) trieChild(tr *trie, parentID ShardID, childIdx uint8, create bool) (*trie, error) {
	child := tr.children[childIdx].Load()
	if child != nil {
		return child, nil
	}
	if !create && tr.isChecked(childIdx) {
		// we have checked, it doesn't exist, and we aren't creating it, so return nil.
		return nil, nil
	}
	childID := parentID.Child(childIdx)
	childShard, err := OpenShard(db.root, childID, db.maxTableSize(), db.maxPackSize)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if !create {
				tr.markChecked(childIdx)
				return nil, nil
			} else {
				childShard, err = CreateShard(db.root, childID, db.maxTableSize(), db.maxPackSize)
				if err != nil {
					return nil, err
				}
			}
		} else {
			return nil, err
		}
	}
	child = newTrie(childShard)
	if tr.children[childIdx].CompareAndSwap(nil, child) {
		return child, nil
	} else {
		// we lost the race, close the shard.
		child.shard.Close()
		return tr.children[childIdx].Load(), nil
	}
}

// put recursively traverses the trie, and inserts key and data into the appropriate shard.
// the next shard to visit is returned.
func (db *Store) put(tr *trie, key Key, depth int, data []byte) (changed bool, next *trie, _ error) {
	sh := tr.shard
	if err := sh.load(db.maxTableSize(), db.maxPackSize); err != nil {
		return false, nil, err
	}
	shardID := key.ShardID(depth * 8)

	// first check if the key already exists in this shard.
	if sh.LocalExists(key) {
		// already exists, nothing to do, return false and nil.
		return false, nil, nil
	}
	// then check if the child exists
	childIdx := key.Uint8(depth)
	child, err := db.trieChild(tr, shardID, childIdx, false)
	if err != nil {
		return false, nil, err
	}
	if child != nil {
		// found a child, continue
		return false, child, nil
	}

	// No child, so this is the best shard.
	// See if this shard has space.
	// if the pack or table is full, then we need to go to a new child.
	if sh.HasSpace(len(data)) {
		// The shard does have space, try appending to it.
		ok, err := sh.LocalAppend(key, data)
		if err != nil {
			if IsErrShardFull(err) {
				// we lost a race, and the shard became full.
				// rerun this function on this shard.
				return false, tr, nil
			}
			return false, nil, err
		}
		return ok, nil, nil
	}
	// the shard does not have space even though it was the best.
	// Now we need to create an even better shard.
	child, err = db.trieChild(tr, shardID, childIdx, true)
	if err != nil {
		return false, nil, err
	}
	return false, child, nil
}

func (db *Store) get(tr *trie, key Key, depth int, fn func(data []byte)) (bool, error) {
	sh := tr.shard
	if err := sh.load(db.maxTableSize(), db.maxPackSize); err != nil {
		return false, err
	}
	shardID := key.ShardID(depth * 8)

	if found, err := sh.LocalGet(key, fn); err != nil {
		return false, err
	} else if found {
		return true, nil
	}

	childIdx := key.Uint8(depth)
	child, err := db.trieChild(tr, shardID, childIdx, false)
	if err != nil {
		return false, err
	}
	if child == nil {
		return false, nil
	}
	return db.get(child, key, depth+1, fn)
}

func (db *Store) delete(tr *trie, key Key, depth int) (*trie, error) {
	sh := tr.shard
	if err := sh.load(db.maxTableSize(), db.maxPackSize); err != nil {
		return nil, err
	}
	shardID := key.ShardID(depth * 8)
	if _, err := sh.LocalDelete(key); err != nil {
		return nil, err
	}
	childIdx := key.Uint8(int(depth))
	child, err := db.trieChild(tr, shardID, childIdx, false)
	if err != nil {
		return nil, err
	}
	return child, nil
}

// trie is used to index the shards
type trie struct {
	children [256]atomic.Pointer[trie]
	// checked is a bitmap for when children have been checked in the filesystem.
	checked [4]atomic.Uint64

	shard *Shard
}

func newTrie(shard *Shard) *trie {
	return &trie{shard: shard}
}

func (t *trie) isChecked(childIdx uint8) bool {
	return t.checked[childIdx/64].Load()&(1<<(childIdx%64)) != 0
}

func (t *trie) markChecked(childIdx uint8) {
	for {
		old := t.checked[childIdx/64].Load()
		if t.checked[childIdx/64].CompareAndSwap(old, old|(1<<(childIdx%64))) {
			break
		}
	}
}

func (t *trie) walk(fn func(shard *Shard)) {
	shard := t.shard
	if shard != nil {
		fn(shard)
	}
	for i := range 256 {
		child := t.children[i].Load()
		if child != nil {
			child.walk(fn)
		}
	}
}

func ensureDir(root *os.Root, path string) error {
	if err := root.Mkdir(path, 0o755); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}
	return nil
}
