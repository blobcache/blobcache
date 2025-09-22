package blobman

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
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

func (k Key) Rotate(i int) Key {
	return Key{k[0]>>i | k[1]<<(64-i), k[0]<<i | k[1]>>(64-i)}
}

// ShiftIn shifts the key into 0.
// The lowest bits are discarded, zeros are shifted in to the highest bits.
func (k Key) ShiftIn(i int) Key {
	return Key{k[0]>>i | k[1]<<(64-i), k[0]<<i | k[1]>>(64-i)}
}

func (k Key) Uint8(i int) uint8 {
	return byte(k[i>>6] >> (i & 0x3f))
}

// Uint8Len returns the number of 8 bit integers in the key.
func (k Key) Uint8Len() int {
	return 16
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

func (k Key) Bytes() []byte {
	d := k.Data()
	return d[:]
}

// ShardID is a prefix of at most 120 bits.
// ShardID takes up 128 bits.
// A prefix refers to a set of keys.
type ShardID struct {
	data    [15]byte
	numBits uint8
}

func NewShardID(data [15]byte, numBits uint8) ShardID {
	if numBits%8 != 0 {
		panic("numBits must be a multiple of 8")
	}
	if numBits > 120 {
		numBits = 120
	}
	return ShardID{data: data, numBits: numBits}
}

func (p ShardID) ShiftIn(i int) ShardID {
	shiftInBytes(p.data[:], i)
	return ShardID{data: p.data, numBits: p.numBits + uint8(i)}
}

// shiftInBytes performs a logical shift towards zero.
func shiftInBytes(data []byte, i int) {
	bi := big.NewInt(0)
	bi.SetBytes(data)
	bi.Rsh(bi, uint(i))
}

func (p ShardID) Data() (ret [15]byte) {
	return p.data
}

func (p ShardID) Len() int {
	return int(p.numBits)
}

func (p ShardID) Path() (string, error) {
	if p.Len()%8 != 0 {
		return "", fmt.Errorf("bitLen must be a multiple of 8. have %d", p.Len())
	}
	data := p.Data()
	hexData := hex.AppendEncode(nil, data[:p.Len()/8])
	sb := strings.Builder{}
	for i := 0; i < len(hexData); i += 2 {
		if i > 0 {
			sb.WriteRune(filepath.Separator)
		}
		sb.Write(hexData[i : i+2])
	}
	return sb.String(), nil
}

const (
	TableFileExt = ".slot"
	PackFileExt  = ".pack"
)

// FileKey uniquely identifies a {Table, Pack} file within the system.
type FileKey struct {
	// Shard uniquely identifies the shard
	ShardID ShardID
	// Gen uniquely identifies the generation of the file within the shard
	Gen uint64
}

func (fk FileKey) PackPath() (string, error) {
	p, err := fk.ShardID.Path()
	if err != nil {
		return "", err
	}
	return filepath.Join(p, PackFilename(fk.Gen)), nil
}

func (fk FileKey) TablePath() (string, error) {
	p, err := fk.ShardID.Path()
	if err != nil {
		return "", err
	}
	return filepath.Join(p, TableFilename(fk.Gen)), nil
}
