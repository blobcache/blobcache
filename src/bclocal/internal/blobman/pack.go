package blobman

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync/atomic"

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

func PackFilename(gen uint32) string {
	return fmt.Sprintf("%08x"+PackFileExt, gen)
}

// CreatePackFile creates a file configured for a pack in the filesystem, and returns it.
func CreatePackFile(root *os.Root, gen uint32, maxSize uint32) (*os.File, error) {
	p := PackFilename(gen)
	f, err := root.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(maxSize)); err != nil {
		return nil, err
	}
	return f, nil
}

func LoadPackFile(root *os.Root, gen uint32) (*os.File, error) {
	p := PackFilename(gen)
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

// Close unmaps the pack and closes the file.
// It DOES NOT flush the mmap to disk.
func (pk Pack) Close() error {
	return errors.Join(pk.mm.Unmap(), pk.f.Close())
}

func (pk Pack) FreeSpace() uint32 {
	return pk.maxSize - atomic.LoadUint32(&pk.offset)
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
func (pk *Pack) Get(offset, length uint32, fn func(data []byte)) error {
	if offset+length > uint32(len(pk.mm)) || offset > uint32(len(pk.mm)) {
		return fmt.Errorf("blobman.Pack: offset and length out of bounds offset=%d length=%d len(mm)=%d", offset, length, len(pk.mm))
	}
	fn(pk.mm[offset : offset+length])
	return nil
}

func (pk *Pack) Flush() error {
	return pk.mm.Flush()
}
