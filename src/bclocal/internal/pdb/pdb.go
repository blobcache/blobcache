// package pdb has utilities for working with the Pebble database.
package pdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

type TableID uint32

// TKey is a table key.
type TKey struct {
	TableID TableID
	Key     []byte
}

func (k TKey) Marshal(out []byte) []byte {
	out = binary.BigEndian.AppendUint32(out, uint32(k.TableID))
	out = append(out, k.Key...)
	return out
}

func ParseTKey(k []byte) (TKey, error) {
	if len(k) < 4 {
		return TKey{}, fmt.Errorf("key is too short to contain tableID")
	}
	return TKey{TableID: TableID(binary.BigEndian.Uint32(k[:4])), Key: k[4:]}, nil
}

func TableDelete(ba WO, tid TableID, key []byte) error {
	return ba.Delete(TKey{TableID: tid, Key: key}.Marshal(nil), nil)
}

func TablePut(ba WO, tid TableID, key []byte, value []byte) error {
	return ba.Set(TKey{TableID: tid, Key: key}.Marshal(nil), value, nil)
}

// RO is a read-only interface for the Pebble database.
type RO interface {
	Get(k []byte) (v []byte, closer io.Closer, err error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

// WO is a write-only interface for the Pebble database.
type WO interface {
	Set(k, v []byte, opts *pebble.WriteOptions) error
	Delete(k []byte, opts *pebble.WriteOptions) error
}

// Exists returns true if the key exists in the database, who's value satisfies a predicate.
// If pred is nil, it is ignored.
func Exists(sp RO, k []byte, pred func([]byte) bool) (bool, error) {
	val, closer, err := sp.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	if pred != nil {
		return pred(val), nil
	}
	return true, nil
}

// MVTag (Multi-Version Tag) is used to order transactions on volumes.
type MVTag uint64

func (mvt MVTag) Marshal(out []byte) []byte {
	return binary.BigEndian.AppendUint64(out, uint64(mvt))
}

// MVKey is a multi-version key.
// Not all tables use MVKeys.
type MVKey struct {
	TableID TableID
	Key     []byte
	Version MVTag
}

func (k MVKey) Marshal(out []byte) []byte {
	out = binary.BigEndian.AppendUint32(out, uint32(k.TableID))
	out = append(out, k.Key...)
	out = k.Version.Marshal(out)
	return out
}

func ParseMVKey(k []byte) (MVKey, error) {
	if len(k) < 4+8 {
		return MVKey{}, fmt.Errorf("key too short to contain tableID and MVCCID")
	}
	tid := TableID(binary.BigEndian.Uint32(k[:4]))
	mvcc := MVTag(binary.BigEndian.Uint64(k[len(k)-8:]))
	data := k[4 : len(k)-8]
	return MVKey{
		TableID: tid,
		Key:     data,
		Version: mvcc,
	}, nil
}

// MVRow is a multi-version row.
type MVRow struct {
	Key   MVKey
	Value []byte
}

// MVSet is a set of MVIDs.
type MVSet = map[MVTag]struct{}

// MVGet gets the most recent value for the given MVKey from the database, where the version is not in excluding.
// Excluding should be a fast, deterministic function, that returns true if the version should be excluded.
// If no row is found, (nil, no-op-closer, nil) is returned.
func MVGet(sp RO, tid TableID, data []byte, excluding func(MVTag) bool) (*MVRow, io.Closer, error) {
	if excluding == nil {
		excluding = func(mvid MVTag) bool {
			return false
		}
	}
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: MVKey{
			TableID: tid,
			Key:     data,
			Version: 0,
		}.Marshal(nil),
		UpperBound: MVKey{
			TableID: tid,
			Key:     data,
			Version: MVTag(^uint64(0)),
		}.Marshal(nil),
		SkipPoint: func(k []byte) bool {
			// return true to skip
			mvk, err := ParseMVKey(k)
			if err != nil {
				return false
			}
			return excluding(mvk.Version)
		},
	})
	if err != nil {
		return nil, nil, err
	}
	if iter.Last() {
		k, err := ParseMVKey(iter.Key())
		if err != nil {
			return nil, nil, errors.Join(err, iter.Close())
		}
		return &MVRow{Key: k, Value: iter.Value()}, iter, nil
	} else {
		// key not found, return nil
		// also return iter so the caller can close it without nil checking
		return nil, iter, iter.Close()
	}
}

// IncrUint64 looks for a 64 bit integer at key adds delta to it and saves it.
// If the key is not found that is equivalent to reading a 0.
// The new value is returned.
func IncrUint64(ba *pebble.Batch, key []byte, delta int64) (uint64, error) {
	n, err := func() (uint64, error) {
		v, closer, err := ba.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return 0, nil
			}
			return 0, err
		}
		defer closer.Close()
		if len(v) != 8 {
			return 0, fmt.Errorf("invalid value length: %d", len(v))
		}
		return binary.BigEndian.Uint64(v), nil
	}()
	if err != nil {
		return 0, err
	}
	n += uint64(delta)
	if delta != 0 {
		if err := ba.Set(key, binary.BigEndian.AppendUint64(nil, n), nil); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func IncrUint32(ba *pebble.Batch, key []byte, delta int32, deleteZero bool) (uint32, error) {
	n, err := func() (uint32, error) {
		v, closer, err := ba.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return 0, nil
			}
			return 0, err
		}
		defer closer.Close()
		if len(v) != 4 {
			return 0, fmt.Errorf("invalid value length: %d", len(v))
		}
		return binary.BigEndian.Uint32(v), nil
	}()
	if err != nil {
		return 0, err
	}
	if delta == 0 {
		// no change, just return it.
		return n, nil
	}
	n += uint32(delta)
	if n == 0 && deleteZero {
		// delete the key if the value is 0.
		if err := ba.Delete(key, nil); err != nil {
			return 0, err
		}
	} else {
		// set the new value.
		if err := ba.Set(key, binary.BigEndian.AppendUint32(nil, n), nil); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func TableLowerBound(tableID TableID) []byte {
	return TKey{TableID: tableID}.Marshal(nil)
}

func TableUpperBound(tableID TableID) []byte {
	return TKey{TableID: tableID + 1}.Marshal(nil)
}

// Compact iterates over the snapshot, and write delete to Batch to compact a table with Multi-Version keys.
// The snapshot should be from before the batch.
// There can be an arbitrary amount of time between the snapshot and the batch, and this algorithm will still be correct.
// This assumes that MVIDs are never reused.
func Compact(sp *pebble.Snapshot, ba *pebble.Batch, tableID TableID, exclude func(MVTag) bool) error {
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: MVKey{
			TableID: tableID,
			Key:     nil,
			Version: 0,
		}.Marshal(nil),
		UpperBound: MVKey{
			TableID: tableID + 1,
			Key:     nil,
		}.Marshal(nil),
		SkipPoint: func(k []byte) bool {
			mvk, err := ParseMVKey(k)
			if err != nil {
				return false
			}
			return exclude(mvk.Version)
		},
	})
	if err != nil {
		return err
	}
	var prevKey []byte
	var prevVersion MVTag
	for iter.Next() {
		k, err := ParseMVKey(iter.Key())
		if err != nil {
			return err
		}
		// if the key is the same as the previous key, then we can delete the previous verion.
		// was the most recent version of it's key.
		if bytes.Equal(prevKey, k.Key) {
			if err := ba.Delete(MVKey{
				TableID: tableID,
				Key:     prevKey,
				Version: prevVersion,
			}.Marshal(nil), nil); err != nil {
				return err
			}
		}
		prevKey = append(prevKey[:0], k.Key...)
		prevVersion = k.Version
	}
	return nil
}

// Undo removes all the rows in a given table with a specific version.
func Undo(sp RO, ba WO, tid TableID, prefix []byte, mvid MVTag) error {
	var upperBound []byte
	if len(prefix) == 0 {
		upperBound = TableUpperBound(tid)
	} else {
		upperBound = TKey{TableID: tid, Key: PrefixUpperBound(prefix)}.Marshal(nil)
	}
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: TKey{TableID: tid, Key: prefix}.Marshal(nil),
		UpperBound: upperBound,
		SkipPoint: func(k []byte) bool {
			mvk, err := ParseMVKey(k)
			if err != nil {
				return true
			}
			return mvk.Version != mvid
		},
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		k, err := ParseTKey(iter.Key())
		if err != nil {
			return err
		}
		if err := ba.Delete(k.Marshal(nil), nil); err != nil {
			return err
		}
	}
	return nil
}

func PrefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			prefix[i]++
			return prefix
		}
	}
	return append(prefix, 0xff)
}

func DoRO(db *pebble.DB, fn func(*pebble.Snapshot) error) error {
	sn := db.NewSnapshot()
	defer sn.Close()
	return fn(sn)
}

// DoRW creates an indexed batch and calls fn with it.
// if fn returns nil, the batch is committed.
func DoRW(db *pebble.DB, fn func(*pebble.Batch) error) error {
	ba := db.NewIndexedBatch()
	//defer ba.Close()
	if err := fn(ba); err != nil {
		return err
	}
	return ba.Commit(nil)
}
