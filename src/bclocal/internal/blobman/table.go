package blobman

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"

	mmap "github.com/edsrzf/mmap-go"
)

const (
	// TableEntrySize is the size of a row in the table.
	// It actually uses less space than this, but we want to align to 4096 bytes.
	TableEntrySize = 32
	PageSize       = 4096
	EntriesPerPage = PageSize / TableEntrySize
	// DefaultMaxTableLen is the maximum length of a table in rows.
	DefaultMaxTableLen = (1 << 20) / TableEntrySize
)

// TableFilename returns the filename for a table.
// This is just the filename, the directory will contain the shard prefix.
func TableFilename(gen uint32) string {
	return fmt.Sprintf("%08x"+TableFileExt, gen)
}

// CreateTableFile creates a file configured for a table in the filesystem, and returns it.
// maxSize is the maximum size of the table in bytes, NOT the number of rows.
func CreateTableFile(shardRoot *os.Root, gen uint32, maxSize uint32) (*os.File, error) {
	p := TableFilename(gen)
	f, err := shardRoot.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(maxSize)); err != nil {
		return nil, err
	}
	return f, nil
}

func LoadTableFile(shardRoot *os.Root, gen uint32) (*os.File, error) {
	p := TableFilename(gen)
	return shardRoot.OpenFile(p, os.O_RDWR, 0o644)
}

// Table is an unordered append-only list of entries.
// Each entry points into a pack, and all entries are the same size.
type Table struct {
	f      *os.File
	maxLen uint32

	mm mmap.MMap
}

// NewTable mmaps a file and returns a Table.
// count should be the number of rows in the table.
func NewTable(f *os.File) (Table, error) {
	finfo, err := f.Stat()
	if err != nil {
		return Table{}, err
	}
	maxLen := (finfo.Size()) / TableEntrySize

	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return Table{}, err
	}
	if len(mm) != int(maxLen*TableEntrySize) {
		return Table{}, fmt.Errorf("mmaped region does not match max size: %d != %d", len(mm), maxLen*TableEntrySize)
	}
	return Table{f: f, mm: mm, maxLen: uint32(maxLen)}, nil
}

// SlotOffset returns the offset of the i-th slot in the table.
// Slot 0 starts at 4 bytes, to make room for the row count at the beginning.
func (idx *Table) SlotOffset(i uint32) uint32 {
	return i * TableEntrySize
}

func (idx *Table) Slot(i uint32) (ret Entry) {
	beg := idx.SlotOffset(i)
	end := beg + TableEntrySize
	ret.load((*[TableEntrySize]byte)(idx.mm[beg:end]))
	return ret
}

func (idx *Table) SetSlot(slot uint32, ent Entry) {
	beg := idx.SlotOffset(slot)
	end := beg + TableEntrySize
	ent.save((*[TableEntrySize]byte)(idx.mm[beg:end]))
}

// Capacity is the total number of rows that can be stored in the table.
func (idx Table) Capacity() uint32 {
	return uint32(len(idx.mm)) / TableEntrySize
}

func (idx *Table) Flush() error {
	return idx.mm.Flush()
}

func (idx *Table) Close() error {
	return errors.Join(idx.mm.Unmap(), idx.f.Close())
}

// Tombstone writes a tombstone at the given slot.
func (idx *Table) Tombstone(slot uint32) {
	idx.SetSlot(slot, Entry{
		Key:    Key{},
		Offset: math.MaxUint32,
		Len:    0,
	})
}

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// Entry is a single entry in the table.
type Entry struct {
	Key Key

	Offset uint32
	Len    uint32
}

func (ent *Entry) IsTombstone() bool {
	return ent.Offset == math.MaxUint32
}

func (ent *Entry) save(buf *[TableEntrySize]byte) {
	data := ent.Key.Data()
	copy(buf[:], data[:])
	binary.LittleEndian.PutUint32(buf[16:20], ent.Offset)
	binary.LittleEndian.PutUint32(buf[20:24], ent.Len)

	checksum := crc32.Checksum(buf[:24], crc32Table)
	binary.LittleEndian.PutUint32(buf[24:28], checksum)
	binary.LittleEndian.PutUint32(buf[28:32], ^checksum)
}

func (ent *Entry) load(buf *[TableEntrySize]byte) bool {
	ent.Key = KeyFromBytes(buf[:16])
	ent.Offset = binary.LittleEndian.Uint32(buf[16:20])
	ent.Len = binary.LittleEndian.Uint32(buf[20:24])

	actual := crc32.Checksum(buf[:24], crc32Table)
	expected := binary.LittleEndian.Uint32(buf[24:28])
	expectedInv := binary.LittleEndian.Uint32(buf[28:32])

	return actual == expected && expectedInv == ^expected
}
