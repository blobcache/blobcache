package blobman

import (
	"fmt"
	"math"
	"os"
	"slices"
)

// Store stores blobs on in the filesystem.
type Store struct {
	// maxTableLen is the maximum number of rows in a table.
	maxTableLen uint32
	// maxPackSize is the maximum size of a pack in bytes.
	maxPackSize uint32

	shard Shard
}

func New(root *os.Root) *Store {
	st := &Store{
		maxTableLen: DefaultMaxTableLen,
		maxPackSize: DefaultMaxPackSize,
		shard:       Shard{rootDir: root},
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
	sh := &db.shard
	return db.putLoop(sh, key, 0, data)
}

func (db *Store) putLoop(sh *Shard, key Key, depth uint8, data []byte) (bool, error) {
	for range 128 {
		inserted, next, err := db.put(sh, key, depth, data)
		if err != nil {
			return false, err
		}
		if next == nil {
			return inserted, nil
		}
		// If we are moving to a different shard, it will be a child, increment the depth.
		if next != sh {
			depth++
		}
		sh = next
	}
	return false, fmt.Errorf("blobman.Put: the trie iteration limit was reached.  This is a bug, and this error is prefferable to spinning forever")
}

// Get finds key if it exists and calls fn with the data.
// The data must not be used outside the callback.
func (db *Store) Get(key Key, fn func(data []byte)) (bool, error) {
	return db.get(&db.shard, key, 0, fn)
}

// Delete overwrites any tables containing key with a tombstone.
func (db *Store) Delete(key Key) error {
	return db.delete(&db.shard, key, 0)
}

func (db *Store) Flush() error {
	return db.flushShard(&db.shard)
}

// Maintain performs background maintenance tasks on the trie.
func (db *Store) Maintain() error {
	return db.maintainShard(&db.shard)
}

func (db *Store) maintainShard(sh *Shard) error {
	moveOutTableThreshold := uint32(float64(db.maxTableLen) * 0.85)
	moveOutPackThreshold := uint32(float64(db.maxPackSize) * 0.85)
	if sh.tab.Len() > moveOutTableThreshold || sh.pack.FreeSpace() > moveOutPackThreshold {
		return db.moveOutward(sh, 0)
	}
	sh.mu.RLock()
	children := sh.children
	sh.mu.RUnlock()
	for _, child := range children {
		if err := db.maintainShard(child); err != nil {
			return err
		}
	}
	return nil
}

func (db *Store) flushShard(sh *Shard) error {
	if sh == nil {
		return nil
	}
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if !sh.loaded {
		return nil
	}
	if err := sh.pack.Flush(); err != nil {
		return err
	}
	if err := sh.tab.Flush(); err != nil {
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
	return db.flushShard(&db.shard)
}

func (db *Store) maxTableSize() uint32 {
	return TableHeaderSize + db.maxTableLen*TableEntrySize
}

// put recursively traverses the trie, and inserts key and data into the appropriate shard.
// the next shard to visit is returned.
func (db *Store) put(sh *Shard, key Key, depth uint8, data []byte) (changed bool, next *Shard, _ error) {
	if err := sh.load(db.maxTableSize(), db.maxPackSize); err != nil {
		return false, nil, err
	}
	childIdx := key.Uint8(int(depth))

	// check if the key already exists in this shard.
	sh.mu.RLock()
	found := sh.localExists(key)
	child := sh.children[childIdx]
	sh.mu.RUnlock()
	if found {
		// already exists, nothing to do, return false and nil.
		return false, nil, nil
	}
	if child != nil {
		// found a child, continue
		return false, child, nil
	}

	// at this point, we might need to append to this shard, check if that is possible.
	// if the pack or table is full, then we need to go to a new child.
	if !sh.pack.CanAppend(uint32(len(data))) || !sh.tab.CanAppend() {
		// first check if the child exists
		sh.mu.RLock()
		child := sh.children[childIdx]
		sh.mu.RUnlock()
		if child != nil {
			// found a child, continue
			return false, child, nil
		}
		// if it doesn't exist, then create it, but get the write lock first.
		sh.mu.Lock()
		defer sh.mu.Unlock()
		child, err := sh.getOrCreateChild(childIdx)
		if err != nil {
			return false, nil, err
		}
		// continue to the child.
		return false, child, nil
	}

	// at this point, we can *probably* append to the shard.
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if found := sh.localExists(key); found {
		// already exists, nothing to do.
		return false, nil, nil
	}
	if child := sh.children[childIdx]; child != nil {
		// found a child, better place to insert key, continue on.
		return false, child, nil
	}
	if ok := sh.localAppend(key, data); ok {
		// data was appended successfully, return true and nil.
		return true, nil, nil
	} else {
		// we lost a race, and the shard became full.
		// rerun this function on this shard.
		return false, sh, nil
	}
}

func (db *Store) get(sh *Shard, key Key, depth uint8, fn func(data []byte)) (bool, error) {
	if err := sh.load(db.maxTableSize(), db.maxPackSize); err != nil {
		return false, err
	}

	if found, err := func() (bool, error) {
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		ent, found := sh.localScan(key)
		if found {
			return true, sh.pack.Get(ent.Offset, ent.Len, fn)
		}
		return false, nil
	}(); err != nil {
		return false, err
	} else if found {
		return true, nil
	}

	childIdx := key.Uint8(int(depth))
	sh.mu.RLock()
	child := sh.children[childIdx]
	sh.mu.RUnlock()
	if child == nil {
		return false, nil
	}
	return db.get(child, key, depth+1, fn)
}

func (db *Store) delete(sh *Shard, key Key, numBits uint8) error {
	if err := sh.load(db.maxTableSize(), db.maxPackSize); err != nil {
		return err
	}

	if done := func() bool {
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		return sh.localDelete(key)
	}(); done {
		return nil
	}

	childIdx := key.Uint8(int(numBits / 8))
	sh.mu.RLock()
	child := sh.children[childIdx]
	sh.mu.RUnlock()
	if child == nil {
		return nil
	}
	return db.delete(child, key, numBits+8)
}

func (db *Store) moveOutward(sh *Shard, depth uint8) error {
	// for all the entries in this shard,
	// move them to a child shard, and then tombstone the entry in this shard.
	sh.mu.Lock()
	children := make([]*Shard, 256)
	for i := range 256 {
		var err error
		children[i], err = sh.getOrCreateChild(uint8(i))
		if err != nil {
			return err
		}
	}
	sh.mu.Unlock()

	// lock the shard for reading until we are done
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	for i := uint32(0); i < sh.tab.Len(); i++ {
		ent := sh.tab.Slot(i)
		if ent.IsTombstone() {
			continue
		}
		childIdx := ent.Key.Uint8(int(depth + 1))
		child := children[childIdx]
		if err := func() error {
			var putErr error
			if err := sh.pack.Get(ent.Offset, ent.Len, func(data []byte) {
				_, putErr = db.putLoop(child, ent.Key, depth+1, data)
			}); err != nil {
				return err
			}
			return putErr
		}(); err != nil {
			return err
		}
		// after a successful move, tombstone the entry in the source shard.
		sh.tab.Tombstone(i)
	}
	return nil
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

// Copy copies entries from src{Tab,Pack} to dst{Tab,Pack}.
// If there is not enough space available in either the table or the pack, then an error is returned.
// Copy will not copy tombstones, and will sort all the entries by key before copying.
func Copy(srcTab Table, srcPack Pack, dstTab Table, dstPack Pack) (int, error) {
	// todo are the slots that need to be copied, in sorted order by key.
	var todo []uint32
	var cumSize uint32
	for i := uint32(0); i < srcTab.Len(); i++ {
		ent := srcTab.Slot(i)
		if ent.IsTombstone() {
			continue
		}
		todo = append(todo, i)
		cumSize += ent.Len
	}
	if len(todo) > int(dstTab.SlotsLeft()) {
		return 0, fmt.Errorf("dstTab has %d slots, but %d are needed", dstTab.SlotsLeft(), len(todo))
	}
	if cumSize > dstPack.FreeSpace() {
		return 0, fmt.Errorf("dstPack has %d free space, but %d is needed", dstPack.FreeSpace(), cumSize)
	}
	slices.SortFunc(todo, func(a, b uint32) int {
		return KeyCompare(srcTab.Slot(a).Key, srcTab.Slot(b).Key)
	})
	for _, slot := range todo {
		ent := srcTab.Slot(slot)
		var newOffset uint32
		if err := srcPack.Get(ent.Offset, ent.Len, func(data []byte) {
			newOffset = dstPack.Append(data)
		}); err != nil {
			return 0, fmt.Errorf("while copying: %w", err)
		}
		if newOffset == math.MaxUint32 {
			return 0, fmt.Errorf("dstPack is full")
		}
		newSlot := dstTab.Append(TableEntry{
			Key:    ent.Key,
			Offset: newOffset,
			Len:    ent.Len,
		})
		if newSlot == math.MaxUint32 {
			return 0, fmt.Errorf("dstTab is full")
		}
	}
	return len(todo), nil
}

func KeyCompare(a, b Key) int {
	dataA := a.Data()
	dataB := b.Data()
	return slices.Compare(dataA[:], dataB[:])
}
