package blobman

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

const DefaultMaxPackSize = 1 << 26

// Pack is an append-only file on disk.
type Pack struct {
	f       *os.File
	maxSize uint32
	offset  uint32
	mm      mmap.MMap
}

// CreatePackFile creates a file configured for a pack in the filesystem, and returns it.
func CreatePackFile(root *os.Root, prefix Prefix120, maxSize uint32) (*os.File, error) {
	p, err := prefix.PackPath()
	if err != nil {
		return nil, err
	}
	if strings.Contains(p, "/") {
		if err := root.Mkdir(filepath.Dir(p), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	f, err := root.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(maxSize)); err != nil {
		return nil, err
	}
	return f, nil
}

func LoadPackFile(root *os.Root, prefix Prefix120) (*os.File, error) {
	p, err := prefix.PackPath()
	if err != nil {
		return nil, err
	}
	return root.OpenFile(p, os.O_RDWR, 0o644)
}

// NewPack calls f.Stat() to determine the max size
// Then it mmaps the file.
func NewPack(f *os.File, nextOffset uint32) (Pack, error) {
	finfo, err := f.Stat()
	if err != nil {
		return Pack{}, err
	}
	maxSize := finfo.Size()
	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return Pack{}, err
	}
	if len(mm) != int(maxSize) {
		return Pack{}, fmt.Errorf("file size does not match max size: %d != %d", len(mm), maxSize)
	}
	return Pack{f: f, mm: mm, offset: nextOffset, maxSize: uint32(maxSize)}, nil
}

func (pk Pack) Close() error {
	return errors.Join(pk.mm.Unmap(), pk.f.Close())
}

func (pk *Pack) CanAppend(dataLen uint32) bool {
	return atomic.LoadUint32(&pk.offset)+dataLen <= pk.maxSize
}

// Append appends data to the pack and returns the offset of the data.
func (pk *Pack) Append(data []byte) uint32 {
	offsetPtr := &pk.offset
	// check if it would exceed the max size.
	if offset := atomic.LoadUint32(offsetPtr); offset+uint32(len(data)) > pk.maxSize {
		return math.MaxUint32 // the pack is full.
	}
	// otherwise, allocate space and copy the data.
	offset := atomic.AddUint32(offsetPtr, uint32(len(data))) - uint32(len(data))
	if offset > uint32(len(pk.mm)) {
		return math.MaxUint32 // the pack is full.
	}
	copy(pk.mm[offset:], data)
	return offset
}

// Get reads data from the pack by offset and size.
func (pk *Pack) Get(offset, size uint32, fn func(data []byte)) bool {
	if offset+size > uint32(len(pk.mm)) || offset > uint32(len(pk.mm)) {
		return false
	}
	fn(pk.mm[offset : offset+size])
	return true
}

func (pk *Pack) Flush() error {
	return pk.mm.Flush()
}

func ptrUint32(buf *[4]byte) *uint32 {
	return (*uint32)(unsafe.Pointer(buf))
}

type BitMap []uint64

func (bm BitMap) Get(i int) bool {
	return bm[i/64]&(1<<(i%64)) != 0
}

func (bm *BitMap) Set(i int, v bool) {
	for len(*bm) <= i/64 {
		*bm = append(*bm, 0)
	}
	if v {
		(*bm)[i/64] |= 1 << (i % 64)
	} else {
		(*bm)[i/64] &= ^(1 << (i % 64))
	}
}
