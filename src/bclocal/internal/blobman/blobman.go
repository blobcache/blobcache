package blobman

import (
	"encoding/binary"
	"encoding/hex"
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
	return &Store{root: root}
}

// Put finds a spot for key, and writes data to it.
// If the key already exists, then the write is ignored and false is returned.
func (db *Store) Put(key Key, data []byte) bool {
	return db.shard.put(key, data)
}

// Get finds key if it exists and calls fn with the data.
// The data must not be used outside the callback.
func (db *Store) Get(key Key, buf []byte, fn func(data []byte)) bool {
	return false
}

// Delete overwrites any tables containing key with a tombstone.
func (db *Store) Delete(key Key) {
}

type shard struct {
	mu       sync.Mutex
	loaded   bool
	tab      Table
	pack     Pack
	idx      [1 << 16]uint32
	children [256]atomic.Pointer[shard]
}

func (s *shard) load(root *os.Root, prefix Prefix121, numRows uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded {
		return nil
	}
	pf, err := LoadPackFile(root, prefix)
	if err != nil {
		return err
	}
	tf, err := LoadTableFile(root, prefix)
	if err != nil {
		return err
	}
	table, err := NewTable(tf, numRows)
	if err != nil {
		return err
	}
	pack, err := NewPack(pf, table.Slot(table.Len()).Offset)
	if err != nil {
		return err
	}
	s.tab = table
	s.pack = pack
	s.loaded = true
	return nil
}

func newShard(idx Table, pack Pack) *shard {
	return &shard{idx: idx, pack: pack}
}
