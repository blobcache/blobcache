package blobman

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	mmap "github.com/edsrzf/mmap-go"
)

const (
	TableEntrySize      = 32
	DefaultMaxIndexSize = 1 << 20
)

func CreateTableFile(root *os.Root, prefix Prefix121, maxSize uint32) (*os.File, error) {
	f, err := root.OpenFile(prefix.Path(), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(maxSize)); err != nil {
		return nil, err
	}
	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	if len(mm) != int(maxSize) {
		return nil, fmt.Errorf("mmaped region does not match max size: %d != %d", len(mm), maxSize)
	}
	return f, nil
}

func LoadTableFile(root *os.Root, prefix Prefix121) (*os.File, error) {
	return root.OpenFile(prefix.Path(), os.O_RDWR, 0o644)
}

// Table is an unordered append-only list of entries.
// Each entry points into a pack, and all entries are the same size.
type Table struct {
	f       *os.File
	nextRow uint32
	mm      mmap.MMap
}

// NewTable mmaps a file and returns a Table.
// count should be the number of rows in the table.
func NewTable(f *os.File, count uint32) (Table, error) {
	finfo, err := f.Stat()
	if err != nil {
		return Table{}, err
	}
	maxLen := finfo.Size() / TableEntrySize

	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return Table{}, err
	}
	if len(mm) != int(maxLen) {
		return Table{}, fmt.Errorf("mmaped region does not match max size: %d != %d", len(mm), maxLen)
	}
	return Table{f: f, nextRow: count, mm: mm}, nil
}

// Len returns the number of rows in the table.
func (idx *Table) Len() uint32 {
	return atomic.LoadUint32(&idx.nextRow)
}

func (idx *Table) Append(ent IndexEntry) uint32 {
	rowIdx := atomic.AddUint32(&idx.nextRow, 1) - 1
	idx.SetSlot(rowIdx, ent)
	return rowIdx
}

func (idx *Table) Slot(slot uint32) (ret IndexEntry) {
	beg := slot * TableEntrySize
	end := beg + TableEntrySize
	ret.load((*[TableEntrySize]byte)(idx.mm[beg:end]))
	return ret
}

func (idx *Table) SetSlot(slot uint32, ent IndexEntry) {
	beg := slot * TableEntrySize
	end := beg + TableEntrySize
	ent.save((*[TableEntrySize]byte)(idx.mm[beg:end]))
}

func (idx *Table) Close() error {
	return errors.Join(idx.mm.Unmap(), idx.f.Close())
}

type IndexEntry struct {
	Prefix Prefix121
	Offset uint32
}

func (ent *IndexEntry) save(buf *[TableEntrySize]byte) {
	data := ent.Prefix.Data()
	copy(buf[:], data[:])
	binary.LittleEndian.PutUint32(buf[16:16+4], ent.Offset)
}

func (ent *IndexEntry) load(buf *[TableEntrySize]byte) {

}
