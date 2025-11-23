package shard

import (
	"encoding/binary"
	"math/bits"
	"slices"
)

// Key is a 192 bit key.
// The 0th bit is considered the first bit, and that is at k[0] & (1 << 0).
type Key [3]uint64

func KeyFromBytes(b []byte) Key {
	return Key{
		binary.LittleEndian.Uint64(b[0:8]),
		binary.LittleEndian.Uint64(b[8:16]),
		binary.LittleEndian.Uint64(b[16:24]),
	}
}

func KeyCompare(a, b Key) int {
	return slices.Compare(a[:], b[:])
}

// RotateAway specifies the amount of bits to rotate the key away from the 0th bit.
// The rotation is performed away from zero (towards more significant bit positions) modulo the key length.
func (k Key) RotateAway(i int) Key {
	if i >= 64 {
		panic("rotations larger than 64 not supported")
	}
	return Key{
		k[0]<<i | maskLow(bits.RotateLeft64(k[2], i), i),
		k[1]<<i | maskLow(bits.RotateLeft64(k[0], i), i),
		k[2]<<i | maskLow(bits.RotateLeft64(k[1], i), i),
	}
}

// maskLow applies a mask which *includes* the low bits
func maskLow(x uint64, n int) uint64 {
	return x & ((1 << n) - 1)
}

// Uint8 returns the 8 bit integer at the given index.
// Indexes are interpretted modulo 24.
// Uint8(0) is bits [0, 7], Uint8(1) is bits [8, 15].
func (k Key) Uint8(i int) uint8 {
	return k.Data()[i%k.Uint8Len()]
}

// Uint8Len returns the number of 8 bit integers in the key.
func (k Key) Uint8Len() int {
	return 24
}

// Uint64 returns the 64 bit integer at the given index.
// The index is 0 or 1.
func (k Key) Uint64(i int) uint64 {
	return k[i]
}

func (k Key) IsZero() bool {
	return k == Key{}
}

func (k Key) Data() (ret [24]byte) {
	binary.LittleEndian.PutUint64(ret[0:8], k[0])
	binary.LittleEndian.PutUint64(ret[8:16], k[1])
	binary.LittleEndian.PutUint64(ret[16:24], k[2])
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
