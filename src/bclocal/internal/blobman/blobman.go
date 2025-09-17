package blobman

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
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

// ToPrefix takes the first numBits bits of the key and includes those in a prefix.
// The last 7 bits of the key must be dropped.
// ToPrefix will panic, the same as NewPrefix121, if numBits is greater than 121.
func (k Key) ToPrefix(numBits uint8) Prefix121 {
	data := k.Data()
	return NewPrefix121([15]byte(data[:15]), numBits)
}

// Prefix121 is a prefix of at most 121 bits.
// Prefix121 takes up 128 bits.
// A prefix refers to a set of keys.
type Prefix121 struct {
	data    [15]byte
	numBits uint8
}

func NewPrefix121(data [15]byte, numBits uint8) Prefix121 {
	if numBits > 121 {
		numBits = 121
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
	if p.Len() > 0 {
		data := p.Data()
		hexData := hex.AppendEncode(nil, data[:p.Len()/8])
		sb := strings.Builder{}
		for i := 0; i < len(hexData); i += 2 {
			if i > 0 {
				sb.WriteString("/")
			}
			sb.Write(hexData[i : i+2])
		}
		return sb.String()
	} else {
		return "_"
	}
}

func (p Prefix121) PackPath() string {
	return p.Path() + ".pack"
}

func (p Prefix121) TablePath() string {
	return p.Path() + ".tab"
}

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
func (db *Store) Delete(key Key) {
}

type storeCtx struct {
	root *os.Root
}

type shard struct {
	mu     sync.RWMutex
	loaded bool
	tab    Table
	pack   Pack
	// mem maps full-key fingerprint to table row (zero-based)
	mem map[Prefix121]uint32

	children [256]atomic.Pointer[shard]
}

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

func (s *shard) bucketForKey(k Key) uint16 { return k.Uint16(0) }

func (s *shard) put(sctx *storeCtx, key Key, numBits uint8, data []byte) (bool, error) {
	if err := s.load(sctx, key.ToPrefix(numBits), 0); err != nil {
		return false, err
	}
	// Duplicate check using in-memory map
	s.mu.RLock()
	_, exists := s.mem[key.ToPrefix(numBits)]
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
		return child.put(sctx, key.ShiftIn(8), numBits-8, data)
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
		return child.put(sctx, key.ShiftIn(8), numBits-8, data)
	}
	// Append index entry
	b := s.bucketForKey(key)
	head := atomic.LoadUint32((*uint32)(unsafe.Pointer(&s.tab.mm[b*TableEntrySize])))
	ent := IndexEntry{
		Prefix: key.ToPrefix(numBits),
		Offset: off + 4,
		Len:    uint32(len(data)),
		Prev:   head,
		Bucket: b,
	}
	newRow := s.tab.Append(ent)
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&s.tab.mm[b*TableEntrySize])), newRow+1)
	// Update in-memory map
	s.mu.Lock()
	s.mem[key.ToPrefix(numBits)] = newRow
	s.mu.Unlock()
	return true, nil
}

func (s *shard) get(root *os.Root, key Key, numBits uint8, buf []byte, fn func(data []byte)) (bool, error) {
	// Check local map first
	s.mu.RLock()
	row, ok := s.mem[key.ToPrefix(numBits)]
	s.mu.RUnlock()
	if ok {
		ent := s.tab.Slot(row)
		if s.pack.Get(ent.Offset, ent.Len, fn) {
			return true, nil
		}
	}
	childIdx := key.Uint8(0)
	if child := s.children[childIdx].Load(); child != nil {
		return child.get(root, key.ShiftIn(8), numBits+8, buf, fn)
	}
	return false, nil
}
