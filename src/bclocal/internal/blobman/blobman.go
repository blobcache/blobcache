package blobman

import (
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"sync"
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
		trie:        trie{},
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

func (db *Store) putLoop(tr *trie, key Key, depth uint8, data []byte) (bool, error) {
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

// loadTrie must be called before all operations
// it searches the filesystem for children and creates shards for them.
func (db *Store) loadTrie(dst *trie, shardID ShardID) error {
	shard := dst.shard
	if shard != nil {
		return nil
	}
	children, err := db.findChildren(shardID)
	if err != nil {
		return err
	}
	for i, childShard := range children {
		childTrie := newTrie(childShard)
		dst.children[i].Store(childTrie)
	}
	return nil
}

// findChildren looks for child shards in the filesystem.
func (db *Store) findChildren(shardID ShardID) ([256]*Shard, error) {
	var children [256]*Shard
	p := shardID.Path()
	entries, err := fs.ReadDir(db.root.FS(), p)
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
		childRoot, err := db.root.OpenRoot(ent.Name())
		if err != nil {
			return children, err
		}
		children[childIdx] = &Shard{rootDir: childRoot}
	}
	return children, nil
}

func (db *Store) createShard(shardID ShardID) (*Shard, error) {
	return CreateShard(db.root, shardID)
}

// put recursively traverses the trie, and inserts key and data into the appropriate shard.
// the next shard to visit is returned.
func (db *Store) put(tr *trie, key Key, depth uint8, data []byte) (changed bool, next *trie, _ error) {
	if err := db.loadTrie(tr, key.ShardID(depth*8)); err != nil {
		return false, nil, err
	}
	sh := tr.shard

	// first check if the key already exists in this shard.
	if sh.LocalExists(key) {
		// already exists, nothing to do, return false and nil.
		return false, nil, nil
	}
	// then check if the child exists
	child := tr.children[key.Uint8(int(depth))].Load()
	if child != nil {
		// found a child, continue
		return false, child, nil
	}
	// at this point, we might need to append to this shard, check if that is possible.
	// if the pack or table is full, then we need to go to a new child.
	if sh.HasSpace() {
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
	// at this point, the shard does not have space, and there wasn't a child.
	// so we need to create a new child, and continue to it.
	childIdx := key.Uint8(int(depth))
	for range 128 {
		child = tr.children[childIdx].Load()
		if child != nil {
			// continue to the child.
			return false, child, nil
		}
		// create a shard.
		sh, err := db.createShard(key.ShardID((depth + 1) * 8))
		if err != nil {
			return false, nil, err
		}
		if !tr.children[childIdx].CompareAndSwap(nil, newTrie(sh)) {
			// we lost the race, close the shard.
			sh.Close()
		}
	}
	return false, nil, fmt.Errorf("spun too long trying to create a shard")
}

func (db *Store) get(tr *trie, key Key, depth uint8, fn func(data []byte)) (bool, error) {
	sh := tr.shard

	if found, err := sh.LocalGet(key, fn); err != nil {
		return false, err
	} else if found {
		return true, nil
	}

	child := tr.children[key.Uint8(int(depth))].Load()
	if child == nil {
		return false, nil
	}
	return db.get(child, key, depth+1, fn)
}

func (db *Store) delete(tr *trie, key Key, depth int) (*trie, error) {
	sh := tr.shard
	if _, err := sh.LocalDelete(key); err != nil {
		return nil, err
	}
	child := tr.children[key.Uint8(depth)].Load()
	return child, nil
}

// filterIndex returns the index of the filter that contains the slot.
// The first slot is taken up by the header, so [0, 126] is the first filter.
func filterIndex(slotIdx uint32) int {
	return int((slotIdx + 1) / 128)
}

func slotBeg(filterIdx int) uint32 {
	if filterIdx == 0 {
		return 0
	}
	return uint32(filterIdx*128 - 1)
}

func slotEnd(filterIdx int) uint32 {
	return uint32(filterIdx*128 + 127)
}

// trie is used to index the shards
type trie struct {
	children [256]atomic.Pointer[trie]
	shard    *Shard
	loadMu   sync.Mutex
}

func newTrie(shard *Shard) *trie {
	return &trie{shard: shard}
}

// find finds the trie node closest to the key.
func (t *trie) find(key Key, depth uint8) *trie {
	child := t.children[key.Uint8(int(depth))].Load()
	if child == nil {
		return t
	}
	return child.find(key, depth+1)
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
