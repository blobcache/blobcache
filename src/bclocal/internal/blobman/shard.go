package blobman

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"blobcache.io/blobcache/src/bclocal/internal/blobman/shard"
)

type (
	Shard = shard.Shard
)

// CreateShard creates a new shard in the filesystem.
// If the shard already exists, than os.ErrExist is returned.
// The root should be the global root.
func CreateShard(root *os.Root, prefix ShardID, maxTableSize, maxPackSize uint32) (*Shard, error) {
	p := prefix.Path()
	if err := root.Mkdir(p, 0o755); err != nil {
		return nil, err
	}
	root2, err := root.OpenRoot(p)
	if err != nil {
		return nil, err
	}
	sh := shard.New(root2)
	if err := sh.Hydrate(maxTableSize, maxPackSize); err != nil {
		return nil, err
	}
	return sh, nil
}

// OpenShard opens a shard that already exists in the filesystem.
// The root should be the global root.
func OpenShard(root *os.Root, prefix ShardID, maxTableSize, maxPackSize uint32) (*Shard, error) {
	p := prefix.Path()
	root2, err := root.OpenRoot(p)
	if err != nil {
		return nil, err
	}
	sh := shard.New(root2)
	if err := sh.Hydrate(maxTableSize, maxPackSize); err != nil {
		return nil, err
	}
	return sh, nil
}

// ShardID is a prefix of at most 120 bits.
// ShardID takes up 128 bits.
// A prefix refers to a set of keys.
// This does not belong in the shard package because Shards don't
// know of their own ID or anything about the other shards in the system.
type ShardID struct {
	data    [15]byte
	numBits uint8
}

func newShardID(data [15]byte, numBits uint8) ShardID {
	if numBits%8 != 0 {
		panic("numBits must be a multiple of 8")
	}
	if numBits > 120 {
		numBits = 120
	}
	// TODO: zero out the extra bits
	return ShardID{data: data, numBits: numBits}
}

func shardIDFromBytes(x []byte) ShardID {
	data := [15]byte{}
	copy(data[:], x)
	return newShardID(data, uint8(len(x)*8))
}

// Child appends the given index to the shard id and returns the new shard id.
func (sh ShardID) Child(k uint8) ShardID {
	sh2 := sh
	idx := sh2.numBits / 8
	if idx >= uint8(len(sh2.data)) {
		// additional children cannot be represented.
		panic(sh2)
	}
	sh2.data[idx] = k
	sh2.numBits += 8
	return sh2
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

func (p ShardID) Path() string {
	if p.Len()%8 != 0 {
		panic(fmt.Errorf("bitLen must be a multiple of 8. have %d", p.Len()))
	}
	if p.Len() == 0 {
		return "."
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
	return sb.String()
}

// FileKey uniquely identifies a {Table, Pack} file within the system.
type FileKey struct {
	// Shard uniquely identifies the shard
	ShardID ShardID
	// Gen uniquely identifies the generation of the file within the shard
	Gen uint32
}

func (fk FileKey) PackPath() string {
	p := fk.ShardID.Path()
	return filepath.Join(p, shard.PackFilename(fk.Gen))
}

func (fk FileKey) TablePath() string {
	p := fk.ShardID.Path()
	return filepath.Join(p, shard.TableFilename(fk.Gen))
}

func shardIDAndKey(x CID, depth int) (ShardID, shard.Key) {
	return shardIDFromBytes(x[:depth]), shard.KeyFromBytes(x[depth:])
}
