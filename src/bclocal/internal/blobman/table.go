package blobman

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	mmap "github.com/edsrzf/mmap-go"
)

const (
	TableEntrySize      = 32
	DefaultMaxIndexSize = 1 << 20
)

func CreateTableFile(root *os.Root, prefix Prefix121, maxSize uint32) (*os.File, error) {
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

func LoadTableFile(root *os.Root, prefix Prefix121) (*os.File, error) {
	p := prefix.TablePath()
	return root.OpenFile(p, os.O_RDWR, 0o644)
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
	if len(mm) != int(maxLen*TableEntrySize) {
		return Table{}, fmt.Errorf("mmaped region does not match max size: %d != %d", len(mm), maxLen*TableEntrySize)
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
	Len    uint32
	Prev   uint32 // row+1 of previous head in bucket, 0 if none
	Bucket uint16
}

func (ent *IndexEntry) save(buf *[TableEntrySize]byte) {
	data := ent.Prefix.Data()
	copy(buf[:], data[:])
	buf[15] = ent.Prefix.numBits
	binary.LittleEndian.PutUint32(buf[16:16+4], ent.Offset)
	binary.LittleEndian.PutUint32(buf[20:20+4], ent.Len)
	binary.LittleEndian.PutUint32(buf[24:24+4], ent.Prev)
	binary.LittleEndian.PutUint16(buf[28:28+2], ent.Bucket)
}

func (ent *IndexEntry) load(buf *[TableEntrySize]byte) {
	var data [15]byte
	copy(data[:], buf[:15])
	nb := buf[15]
	ent.Prefix = NewPrefix121(data, nb)
	ent.Offset = binary.LittleEndian.Uint32(buf[16 : 16+4])
	ent.Len = binary.LittleEndian.Uint32(buf[20 : 20+4])
	ent.Prev = binary.LittleEndian.Uint32(buf[24 : 24+4])
	ent.Bucket = binary.LittleEndian.Uint16(buf[28 : 28+2])
}
