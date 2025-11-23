// Package shard implements a single Shard in the database
// A Shard is a single directory on disk, called the Shard's "root".
// All of the data for a Shard is stored as files, which are the immediate children
// of the directory.
// Shards may have subdirectories, which are entirely ignored by the Shard.
// In practice, the organizing layer above will place child Shard directories beneath the
// the parent in the filesystem.
package shard

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Shard is a directory on disk containing table and pack files, potentially across multiple generations.
// Each shard is independent of every other shard, and there are no consistency guarantees between shards.
// The Shard has no information about where it is in the trie.
// Shards only deal with files in their immediate directory, subdirectories (which correspond to children in the trie)
// are ignored by the Shard.
type Shard struct {
	// rootDir is the directory for the shard
	// this is the only field which must not be zero.
	// everything else will be set during load.
	rootDir *os.Root

	// accessMu mediates general access to the shard.
	// the write lock is used when setting up the shard.
	// All operations on the shard must be holding the read lock.
	accessMu sync.RWMutex
	mf       Manifest
	tab      Table
	pack     Pack
	// bfMu guards access to the bloom filters slice.
	// The bloom filters themselves are safe for concurrent use.
	bfMu sync.RWMutex
	bfs  []bloom2048
}

func New(rootDir *os.Root) *Shard {
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

func (sh *Shard) Hydrate(maxTableSize, maxPackSize uint32) error {
	return sh.load(maxTableSize, maxPackSize)
}

func (sh *Shard) isLoaded() bool {
	return sh.mf.Nonce > 0
}

func (dst *Shard) load(maxTableSize, maxPackSize uint32) error {
	// quick check with the read lock
	dst.accessMu.RLock()
	loaded := dst.isLoaded()
	dst.accessMu.RUnlock()
	if loaded {
		return nil
	}
	// now get the write lock
	dst.accessMu.Lock()
	defer dst.accessMu.Unlock()
	if dst.isLoaded() {
		// check one more time
		return nil
	}

	mf, err := LoadManifest(dst.rootDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// initialize a fresh manifest
			mf = Manifest{Nonce: 0, Gen: 0, TableLen: 0}
		} else {
			return err
		}
	}
	mf.Nonce++

	tf, err := LoadTableFile(dst.rootDir, mf.Gen)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			tf, err = CreateTableFile(dst.rootDir, mf.Gen, maxTableSize)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	table, err := NewTable(tf)
	if err != nil {
		return err
	}
	packFile, err := LoadPackFile(dst.rootDir, mf.Gen)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			packFile, err = CreatePackFile(dst.rootDir, mf.Gen, maxPackSize)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// Determine next pack offset
	// Maximum of all Offset + Len in the table.
	var offset uint32
	for i := uint32(0); i < mf.TableLen; i++ {
		ent := table.Slot(i)
		offset = max(offset, ent.Offset+ent.Len)
	}
	pack, err := NewPack(packFile, offset)
	if err != nil {
		return err
	}
	dst.tab = table
	dst.pack = pack
	// need to add all the entries to the bloom filters
	for i := uint32(0); i < mf.TableLen; i++ {
		ent := table.Slot(i)
		bfIdx := filterIndex(i)
		if len(dst.bfs) <= bfIdx {
			dst.bfs = append(dst.bfs, bloom2048{})
		}
		dst.bfs[bfIdx].add(ent.Key)
	}

	// this causes the shard to be considered loaded
	dst.mf = mf
	return nil
}

func (s *Shard) HasSpace(dataLen int) bool {
	tableLen := atomic.LoadUint32(&s.mf.TableLen)
	return s.pack.FreeSpace() > uint32(dataLen) && tableLen < s.tab.Capacity()
}

// LocalExists checks if the key exists in this shards local data.
// Local means that the children and grandchildren are not checked.
func (sh *Shard) LocalExists(key Key) bool {
	sh.accessMu.RLock()
	defer sh.accessMu.RUnlock()
	_, found := sh.localScan(key)
	return found
}

func (sh *Shard) LocalGet(key Key, fn func(data []byte)) (bool, error) {
	sh.accessMu.RLock()
	defer sh.accessMu.RUnlock()
	ent, found := sh.localScan(key)
	if found {
		return true, sh.pack.Get(ent.Offset, ent.Len, fn)
	}
	return false, nil
}

// LocalAppend appends the key and data to the table.
// It returns an error if the table is full or the pack is full.
// It returns (false, nil) if the data already exists.
// It returns (true, nil) if the data was appended successfully.
func (s *Shard) LocalAppend(key Key, data []byte) (bool, error) {
	s.accessMu.RLock()
	defer s.accessMu.RUnlock()
	_, found := s.localScan(key)
	if found {
		return false, nil
	}
	if !s.localAppend(key, data) {
		return false, ErrShardFull{}
	}
	return true, nil
}

// LocalDelete deletes the key from the table.
// It returns (true, nil) if the key was deleted successfully.
func (s *Shard) LocalDelete(key Key) (bool, error) {
	s.accessMu.RLock()
	defer s.accessMu.RUnlock()
	return s.localDelete(key), nil
}

// Flush causes the Shard's current state to be written to disk.
// First the pack and table are flushed concurrently.
// Then once both of them have flushed successfully, the manifest is saved.
func (sh *Shard) Flush() error {
	sh.accessMu.Lock()
	defer sh.accessMu.Unlock()
	if !sh.isLoaded() {
		return nil
	}
	mf := sh.mf
	sh.mf.Nonce++

	var eg errgroup.Group
	eg.Go(func() error {
		return sh.pack.Flush()
	})
	eg.Go(func() error {
		return sh.tab.Flush()
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	if err := SaveManifest(sh.rootDir, mf); err != nil {
		return err
	}
	return nil
}

func (sh *Shard) TableLen() uint32 {
	return sh.mf.TableLen
}

// localScan iterates through each bloom filter, if it gets a hit, then it iterates through the corresponding
// range in the table.
// localScan acquires the bloom filter read lock.
func (s *Shard) localScan(key Key) (Entry, bool) {
	s.bfMu.RLock()
	defer s.bfMu.RUnlock()
	for filterIdx := range s.bfs {
		if !s.bfs[filterIdx].contains(key) {
			continue
		}
		beg := slotBeg(filterIdx)
		end := slotEnd(filterIdx)
		for slot := beg; slot < end && slot < s.mf.TableLen; slot++ {
			ent := s.tab.Slot(slot)
			if ent.IsTombstone() {
				continue
			}
			if ent.Key == key {
				return ent, true
			}
		}
	}
	return Entry{}, false
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
	ent := Entry{
		Key:    key,
		Offset: off,
		Len:    uint32(len(data)),
	}
	slotIdx := s.reserveSlot()
	if slotIdx == math.MaxUint32 {
		return false
	}
	s.tab.SetSlot(slotIdx, ent)

	bfIdx := filterIndex(slotIdx)
	s.growBloomFilters(bfIdx)
	s.bfMu.RLock()
	defer s.bfMu.RUnlock()
	s.bfs[bfIdx].add(key)

	return true
}

// growBloomFilters ensures the bloom filter slice is long enough to contain the given index.
func (s *Shard) growBloomFilters(idx int) {
	s.bfMu.RLock()
	l := len(s.bfs)
	s.bfMu.RUnlock()
	if l > idx {
		return
	}

	s.bfMu.Lock()
	defer s.bfMu.Unlock()
	for len(s.bfs) <= idx {
		s.bfs = append(s.bfs, bloom2048{})
	}
}

func (s *Shard) localDelete(key Key) bool {
	for i := uint32(0); i < s.mf.TableLen; i++ {
		ent := s.tab.Slot(i)
		if ent.Key == key {
			s.tab.Tombstone(i)
			return true
		}
	}
	return false
}

func (s *Shard) reserveSlot() uint32 {
	slot := atomic.AddUint32(&s.mf.TableLen, 1) - 1
	if slot >= s.tab.Capacity() {
		return math.MaxUint32
	}
	return slot
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
	return New(childRoot), nil
}

// filterIndex returns the index of the filter that contains the slot.
// The first slot is taken up by the header, so [0, 126] is the first filter.
func filterIndex(slotIdx uint32) int {
	return int(slotIdx) / 128
}

func slotBeg(filterIdx int) uint32 {
	return uint32(filterIdx * 128)
}

func slotEnd(filterIdx int) uint32 {
	return uint32(filterIdx+1) * 128
}

// Copy copies entries from src{Tab,Pack} to dst{Tab,Pack}.
// If there is not enough space available in either the table or the pack, then an error is returned.
// Copy will not copy tombstones, and will sort all the entries by key before copying.
func Copy(srcLen uint32, srcTab Table, srcPack Pack, dstTab Table, dstPack Pack) (int, error) {
	// todo are the slots that need to be copied, in sorted order by key.
	var todo []uint32
	var cumSize uint32
	for i := uint32(0); i < srcLen; i++ {
		ent := srcTab.Slot(i)
		if ent.IsTombstone() {
			continue
		}
		todo = append(todo, i)
		cumSize += ent.Len
	}
	if len(todo) > int(dstTab.Capacity()-srcLen) {
		return 0, fmt.Errorf("dstTab has %d slots, but %d are needed", dstTab.Capacity()-srcLen, len(todo))
	}
	if cumSize > dstPack.FreeSpace() {
		return 0, fmt.Errorf("dstPack has %d free space, but %d is needed", dstPack.FreeSpace(), cumSize)
	}
	slices.SortFunc(todo, func(a, b uint32) int {
		return KeyCompare(srcTab.Slot(a).Key, srcTab.Slot(b).Key)
	})
	for newSlot, oldSlot := range todo {
		if uint32(newSlot) >= dstTab.Capacity() {
			return 0, fmt.Errorf("dstTab is full")
		}
		ent := srcTab.Slot(oldSlot)
		var newOffset uint32
		if err := srcPack.Get(ent.Offset, ent.Len, func(data []byte) {
			newOffset = dstPack.Append(data)
		}); err != nil {
			return 0, fmt.Errorf("while copying: %w", err)
		}
		if newOffset == math.MaxUint32 {
			return 0, fmt.Errorf("dstPack is full")
		}
		dstTab.SetSlot(uint32(newSlot), Entry{
			Key:    ent.Key,
			Offset: newOffset,
			Len:    ent.Len,
		})
	}
	return len(todo), nil
}

type ErrShardFull struct{}

func (e ErrShardFull) Error() string {
	return "shard is full"
}

func IsErrShardFull(err error) bool {
	return errors.As(err, &ErrShardFull{})
}
