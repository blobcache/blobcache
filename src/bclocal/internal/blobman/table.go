package blobman

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"

	mmap "github.com/edsrzf/mmap-go"
)

const (
	// TableEntrySize is the size of a row in the table.
	// It actually uses less space than this, but we want to align to 4096 bytes.
	TableEntrySize  = 32
	TableHeaderSize = TableEntrySize
	PageSize        = 4096
	EntriesPerPage  = PageSize / TableEntrySize
	// DefaultMaxTableLen is the maximum length of a table in rows.
	DefaultMaxTableLen = (1<<20 - TableHeaderSize) / TableEntrySize
)

func CreateTableFile(root *os.Root, prefix Prefix120, maxSize uint32) (*os.File, error) {
	p := prefix.TablePath()
	if err := root.Mkdir(filepath.Dir(p), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
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

func LoadTableFile(root *os.Root, prefix Prefix120) (*os.File, error) {
	p := prefix.TablePath()
	return root.OpenFile(p, os.O_RDWR, 0o644)
}

// Table is an unordered append-only list of entries.
// Each entry points into a pack, and all entries are the same size.
type Table struct {
	f       *os.File
	gen     uint64
	nextRow uint32
	mm      mmap.MMap
}

// NewTable mmaps a file and returns a Table.
// count should be the number of rows in the table.
func NewTable(f *os.File) (Table, error) {
	finfo, err := f.Stat()
	if err != nil {
		return Table{}, err
	}
	maxLen := finfo.Size() / TableEntrySize

	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return Table{}, err
	}
	if len(mm) != int(maxLen*TableEntrySize) {
		return Table{}, fmt.Errorf("mmaped region does not match max size: %d != %d", len(mm), maxLen*TableEntrySize)
	}
	gen := binary.LittleEndian.Uint64(mm[0:8])
	count := binary.LittleEndian.Uint32(mm[8:12])
	return Table{f: f, gen: gen, nextRow: count, mm: mm}, nil
}

// Len returns the number of rows in the table.
func (idx *Table) Len() uint32 {
	return atomic.LoadUint32(&idx.nextRow)
}

// SlotOffset returns the offset of the i-th slot in the table.
// Slot 0 starts at 4 bytes, to make room for the row count at the beginning.
func (idx *Table) SlotOffset(i uint32) uint32 {
	return TableHeaderSize + i*TableEntrySize
}

func (idx *Table) Append(ent TableEntry) uint32 {
	rowIdx := atomic.AddUint32(&idx.nextRow, 1) - 1
	idx.SetSlot(rowIdx, ent)
	// set the next row in the table header
	binary.LittleEndian.PutUint32(idx.mm[8:12], rowIdx+1)
	return rowIdx
}

func (idx *Table) Slot(i uint32) (ret TableEntry) {
	// skip the row count at the beginning
	beg := idx.SlotOffset(i)
	end := beg + TableEntrySize
	ret.load((*[TableEntrySize]byte)(idx.mm[beg:end]))
	return ret
}

func (idx *Table) SetSlot(slot uint32, ent TableEntry) {
	beg := idx.SlotOffset(slot)
	end := beg + TableEntrySize
	ent.save((*[TableEntrySize]byte)(idx.mm[beg:end]))
}

func (idx *Table) Flush() error {
	return idx.mm.Flush()
}

func (idx *Table) Close() error {
	return errors.Join(idx.mm.Unmap(), idx.f.Close())
}

// Capacity is the total number of rows that can be stored in the table.
func (idx Table) Capacity() uint32 {
	return uint32(len(idx.mm)-TableHeaderSize) / TableEntrySize
}

// Tombstone writes a tombstone at the given slot.
func (idx *Table) Tombstone(slot uint32) {
	idx.SetSlot(slot, TableEntry{
		Key:    Key{},
		Offset: math.MaxUint32,
		Len:    0,
	})
}

type TableEntry struct {
	Key Key

	Offset uint32
	Len    uint32
}

func (ent *TableEntry) save(buf *[TableEntrySize]byte) {
	data := ent.Key.Data()
	copy(buf[:], data[:])
	binary.LittleEndian.PutUint32(buf[16:20], ent.Offset)
	binary.LittleEndian.PutUint32(buf[20:24], ent.Len)
}

func (ent *TableEntry) load(buf *[TableEntrySize]byte) {
	ent.Key = KeyFromBytes(buf[:16])
	ent.Offset = binary.LittleEndian.Uint32(buf[16:20])
	ent.Len = binary.LittleEndian.Uint32(buf[20:24])
}
