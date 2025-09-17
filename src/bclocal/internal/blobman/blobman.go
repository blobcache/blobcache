package blobman

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
)

// Key is a 128 bit key.
// The 0th bit is considered the first bit, and that is at k[0] & (1 << 0).
type Key [2]uint64

// ShiftIn shifts the key into 0.
// The lowest bits are discarded, zeros are shifted in to the highest bits.
func (k Key) ShiftIn(i int) Key {
	return Key{k[0]>>i | k[1]<<(64-i), k[0]<<i | k[1]>>(64-i)}
}

func (k Key) Uint8(i int) uint8 {
	return byte(k[i>>6] >> (i & 0x3f))
}

func (k Key) Uint16(i int) uint16 {
	return uint16(k[i>>4] >> (i & 0x0f))
}

// Uint64 returns the 64 bit integer at the given index.
// The index is 0 or 1.
func (k Key) Uint64(i int) uint64 {
	if i&1 == 0 {
		return k[0]
	} else {
		return k[1]
	}
}

func (k Key) IsZero() bool {
	return k == Key{}
}

func (k Key) Data() (ret [16]byte) {
	binary.LittleEndian.PutUint64(ret[:8], k[0])
	binary.LittleEndian.PutUint64(ret[8:], k[1])
	return ret
}

// Prefix121 is a prefix of at most 121 bits.
// Prefix121 takes up 128 bits.
type Prefix121 struct {
	data    [15]byte
	numBits uint8
}

func NewPrefix121(data [15]byte, numBits uint8) Prefix121 {
	if numBits > 121 {
		panic(numBits)
	}
	return Prefix121{data: data, numBits: numBits}
}

func (p Prefix121) ShiftIn(i int) Prefix121 {
	shiftInBytes(p.data[:], i)
	return Prefix121{data: p.data, numBits: p.numBits + uint8(i)}
}

// shiftInBytes performs a logical shift towards zero.
func shiftInBytes(data []byte, i int) {
	bi := big.NewInt(0)
	bi.SetBytes(data)
	bi.Rsh(bi, uint(i))
}

func (p Prefix121) Data() (ret [15]byte) {
	return p.data
}

func (p Prefix121) Len() int {
	return int(p.numBits)
}

func (p Prefix121) Path() string {
	ret := "_.pack"
	if p.Len() > 0 {
		data := p.Data()
		ret = hex.EncodeToString(data[:p.Len()/8]) + ".pack"
	}
	return ret
}

// Loc is a location in the store.
type Loc struct {
	Prefix Prefix121
	Gen    uint32
	Offset uint32
	Len    uint32
}

// Store stores blobs on in the filesystem.
type Store struct {
	root *os.Root

	shard shard
}

func New(root *os.Root) *Store {
	st := &Store{root: root}
	st.shard.root = root
	st.shard.prefix = NewPrefix121([15]byte{}, 0)
	return st
}

// Put finds a spot for key, and writes data to it.
// If the key already exists, then the write is ignored and false is returned.
func (db *Store) Put(key Key, data []byte) bool {
	return db.shard.put(key, data)
}

// Get finds key if it exists and calls fn with the data.
// The data must not be used outside the callback.
func (db *Store) Get(key Key, buf []byte, fn func(data []byte)) bool {
	return db.shard.get(key, buf, fn)
}

// Delete overwrites any tables containing key with a tombstone.
func (db *Store) Delete(key Key) {
}

type shard struct {
	root     *os.Root
	mu       sync.Mutex
	loaded   bool
	tab      Table
	pack     Pack
	idx      [1 << 16]uint32
	children [256]atomic.Pointer[shard]
	prefix   Prefix121
}

func (s *shard) load(numRows uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded {
		return nil
	}
	// Open or create table/pack files for this shard's prefix.
	pf, err := LoadPackFile(s.root, s.prefix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			pf, err = CreatePackFile(s.root, s.prefix, MaxPackSize)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	tf, err := LoadTableFile(s.root, s.prefix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			tf, err = CreateTableFile(s.root, s.prefix, DefaultMaxIndexSize)
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
	s.loaded = true
	return nil
}

func (s *shard) maxRows() uint32 {
	return uint32(len(s.tab.mm)) / TableEntrySize
}

func fingerprintFromKey(k Key) Prefix121 {
	kd := k.Data()
	var p15 [15]byte
	copy(p15[:], kd[:15])
	return NewPrefix121(p15, 121)
}

func (s *shard) bucketForKey(k Key) uint16 {
	return k.Uint16(s.prefix.Len())
}

func prefixFromKeyAtLen(k Key, bitLen int) Prefix121 {
	kd := k.Data()
	var p15 [15]byte
	copy(p15[:], kd[:15])
	return NewPrefix121(p15, uint8(bitLen))
}

func (s *shard) put(key Key, data []byte) bool {
	if err := s.load(0); err != nil {
		return false
	}
	fp := fingerprintFromKey(key)
	b := s.bucketForKey(key)
	head := atomic.LoadUint32(&s.idx[b])
	// Walk chain at this node to check for existing key
	row := head
	for row != 0 {
		ent := s.tab.Slot(row - 1)
		if ent.Prefix == fp {
			return false
		}
		if ent.Prev == 0 {
			break
		}
		row = ent.Prev
	}
	// Try deeper child first if it exists (longest prefix available)
	childIdx := key.Uint8(s.prefix.Len())
	if child := s.children[childIdx].Load(); child != nil {
		if child.put(key, data) {
			return true
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
		childPrefix := prefixFromKeyAtLen(key, s.prefix.Len()+8)
		child := s.children[childIdx].Load()
		if child == nil {
			newChild := &shard{root: s.root, prefix: childPrefix}
			s.children[childIdx].CompareAndSwap(nil, newChild)
			child = s.children[childIdx].Load()
		}
		return child.put(key, data)
	}
	// Check table capacity
	if s.tab.Len() >= s.maxRows() {
		// Create child and insert there
		childPrefix := prefixFromKeyAtLen(key, s.prefix.Len()+8)
		child := s.children[childIdx].Load()
		if child == nil {
			newChild := &shard{root: s.root, prefix: childPrefix}
			s.children[childIdx].CompareAndSwap(nil, newChild)
			child = s.children[childIdx].Load()
		}
		return child.put(key, data)
	}
	// Append index entry
	ent := IndexEntry{
		Prefix: fp,
		Offset: off + 4,
		Len:    uint32(len(data)),
		Prev:   head,
		Bucket: b,
	}
	newRow := s.tab.Append(ent)
	atomic.StoreUint32(&s.idx[b], newRow+1)
	return true
}

func (s *shard) get(key Key, buf []byte, fn func(data []byte)) bool {
	if err := s.load(0); err != nil {
		return false
	}
	fp := fingerprintFromKey(key)
	// Try deeper child first if it exists
	childIdx := key.Uint8(s.prefix.Len())
	if child := s.children[childIdx].Load(); child != nil {
		if child.get(key, buf, fn) {
			return true
		}
	}
	b := s.bucketForKey(key)
	row := atomic.LoadUint32(&s.idx[b])
	for row != 0 {
		ent := s.tab.Slot(row - 1)
		if ent.Prefix == fp {
			// Found
			return s.pack.Get(ent.Offset, ent.Len, fn)
		}
		if ent.Prev == 0 {
			break
		}
		row = ent.Prev
	}
	return false
}
