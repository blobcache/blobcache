package shard

import (
	"encoding/binary"
	"slices"
)

// Key is a 128 bit key.
// The 0th bit is considered the first bit, and that is at k[0] & (1 << 0).
type Key [2]uint64

func KeyFromBytes(b []byte) Key {
	return Key{
		binary.LittleEndian.Uint64(b[:8]),
		binary.LittleEndian.Uint64(b[8:]),
	}
}

func KeyCompare(a, b Key) int {
	dataA := a.Data()
	dataB := b.Data()
	return slices.Compare(dataA[:], dataB[:])
}

// RotateAway specifies the amount of bits to rotate the key away from the 0th bit.
func (k Key) RotateAway(i int) Key {
	return Key{k[0]>>i | k[1]<<(64-i), k[0]<<i | k[1]>>(64-i)}
}

// ShiftIn shifts the key into 0.
// The lowest bits are discarded, zeros are shifted in to the highest bits.
func (k Key) ShiftIn(i int) Key {
	return Key{k[0]>>i | k[1]<<(64-i), k[0]<<i | k[1]>>(64-i)}
}

// Uint8 returns the 8 bit integer at the given index.
// Indexes are interpretted modulo 16.
// Uint8(0) is bits [0, 7], Uint8(1) is bits [8, 15].
func (k Key) Uint8(i int) uint8 {
	idx := i % 16
	if idx < 8 {
		return uint8(k[0] >> (idx * 8))
	}
	return uint8(k[1] >> ((idx - 8) * 8))
}

// Uint8Len returns the number of 8 bit integers in the key.
func (k Key) Uint8Len() int {
	return 16
}

// Uint16 returns the 16 bit integer at the given index.
// Indexes are interpretted modulo 8.
// Uint16(0) is bits [0, 15], Uint16(1) is bits [16, 31].
func (k Key) Uint16(i int) uint16 {
	idx := i % 8
	if idx < 4 {
		return uint16(k[0] >> (idx * 16))
	}
	return uint16(k[1] >> ((idx - 4) * 16))
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

func (k Key) Bytes() []byte {
	d := k.Data()
	return d[:]
}

const (
	TableFileExt = ".slot"
	PackFileExt  = ".pack"
)
