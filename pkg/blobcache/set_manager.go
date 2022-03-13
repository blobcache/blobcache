package blobcache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
)

const (
	setNamesPrefix     = "names\x00"
	setItemsPrefix     = "items\x00"
	setSeq             = "seq"
	setRefCountsPrefix = "rc\x00"
)

// setManager manages multiple sets backed by a database
type setManager struct {
	db bcdb.DB
}

func newSetManager(db bcdb.DB) *setManager {
	return &setManager{db: db}
}

func (sm *setManager) open(x string) cadata.Set {
	return &set{db: sm.db, name: x}
}

func (sm *setManager) drop(ctx context.Context, name string) error {
	// TODO: remove all items, decrement reference counts
	return nil
}

func (sm *setManager) union() cadata.Set {
	return &unionSet{db: sm.db}
}

func (sm *setManager) getRefCount(ctx context.Context, id cadata.ID) (count uint64, _ error) {
	if err := sm.db.View(ctx, func(tx bcdb.Tx) error {
		var err error
		count, err = rcGet(tx, id)
		return err
	}); err != nil {
		return 0, err
	}
	return count, nil
}

type set struct {
	db   bcdb.DB
	name string
}

func (s *set) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	var exists bool
	if err := s.db.View(ctx, func(tx bcdb.Tx) error {
		i, err := s.getIntID(tx, false)
		if err != nil {
			return err
		}
		if i == 0 {
			exists = false
			return nil
		}
		k := itemKeyFor(i, id)
		v, err := tx.Get(k)
		exists = v != nil
		return err
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *set) List(ctx context.Context, first []byte, ids []cadata.ID) (int, error) {
	var n int
	err := s.db.View(ctx, func(tx bcdb.Tx) error {
		i, err := s.getIntID(tx, false)
		if err != nil {
			return err
		}
		if i == 0 {
			return cadata.ErrEndOfList
		}
		span := itemSpanFor(i, first)
		n = 0
		stopIter := errors.New("stop iteration")
		if err := tx.ForEach(span, func(k, _ []byte) error {
			if n >= len(ids) {
				return stopIter
			}
			id, err := idFromItemKey(k)
			if err != nil {
				return err
			}
			ids[n] = id
			n++
			return nil
		}); err != nil {
			if errors.Is(err, stopIter) {
				err = nil
			}
			return err
		}
		return cadata.ErrEndOfList
	})
	return n, err
}

func (s *set) Add(ctx context.Context, id cadata.ID) error {
	return s.db.Update(ctx, func(tx bcdb.Tx) error {
		n, err := s.getIntID(tx, true)
		if err != nil {
			return err
		}
		k := itemKeyFor(n, id)
		if err := tx.Put(k, nil); err != nil {
			return err
		}
		_, err = rcIncr(tx, id)
		return err
	})
}

func (s *set) Delete(ctx context.Context, id cadata.ID) error {
	return s.db.Update(ctx, func(tx bcdb.Tx) error {
		i, err := s.getIntID(tx, false)
		if err != nil {
			return err
		}
		if i == 0 {
			return nil
		}
		k := itemKeyFor(i, id)
		v, err := tx.Get(k)
		if err != nil {
			return err
		}
		if v != nil {
			if _, err := rcDecr(tx, id); err != nil {
				return err
			}
		}
		if err := tx.Delete(k); err != nil {
			return err
		}
		return s.maybeDrop(tx, i)
	})
}

func (s *set) getIntID(tx bcdb.Tx, create bool) (uint64, error) {
	nameKey := s.nameKey()
	v, err := tx.Get(nameKey)
	if err != nil {
		return 0, err
	}
	if v != nil {
		i, err := parseUint64(v)
		if err != nil {
			return 0, err
		}
		return i, nil
	}
	if !create {
		return 0, nil
	}
	i, err := bcdb.Increment(tx, []byte(setSeq))
	if err != nil {
		return 0, err
	}
	if err := tx.Put(nameKey, uint64Bytes(i)); err != nil {
		return 0, err
	}
	return i, nil
}

func (s *set) maybeDrop(tx bcdb.Tx, i uint64) error {
	span := itemSpanFor(i, nil)
	nonEmpty := false
	if err := tx.ForEach(span, func(k, v []byte) error {
		nonEmpty = true
		return nil
	}); err != nil {
		return err
	}
	if !nonEmpty {
		return tx.Delete(s.nameKey())
	}
	return nil
}

const setItemKeySize = len(setItemsPrefix) + 8 + cadata.IDSize

func (s *set) nameKey() []byte {
	return append([]byte(setNamesPrefix), []byte(s.name)...)
}

func itemKeyFor(n uint64, id cadata.ID) (ret []byte) {
	ret = append(ret, []byte(setItemsPrefix)...)
	ret = append(ret, uint64Bytes(n)...)
	ret = append(ret, id[:]...)
	return ret
}

func itemSpanFor(n uint64, first []byte) state.ByteSpan {
	prefix := append([]byte(setItemsPrefix), uint64Bytes(n)...)
	span := state.ByteSpan{
		Begin: append(prefix, first...),
		End:   bcdb.PrefixEnd(prefix),
	}
	return span
}

func idFromItemKey(k []byte) (cadata.ID, error) {
	if len(k) < setItemKeySize {
		return cadata.ID{}, fmt.Errorf("key too short to be set-item %q", k)
	}
	return cadata.IDFromBytes(k[len(setItemsPrefix)+8:]), nil
}

func rcKey(id cadata.ID) (ret []byte) {
	ret = append(ret, setRefCountsPrefix...)
	ret = append(ret, id[:]...)
	return ret
}

func rcDecr(tx bcdb.Tx, id cadata.ID) (uint64, error) {
	k := rcKey(id)
	x, err := bcdb.Decrement(tx, k)
	if err != nil {
		return 0, err
	}
	if x == 0 {
		if err := tx.Delete(k); err != nil {
			return 0, err
		}
	}
	return x, nil
}

func rcIncr(tx bcdb.Tx, id cadata.ID) (uint64, error) {
	k := rcKey(id)
	return bcdb.Increment(tx, k)
}

func rcGet(tx bcdb.Tx, id cadata.ID) (uint64, error) {
	k := rcKey(id)
	v, err := tx.Get(k)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("not a 64 bit integer")
	}
	return binary.BigEndian.Uint64(v), nil
}

type unionSet struct {
	db bcdb.DB
}

func (us unionSet) Add(ctx context.Context, id cadata.ID) error {
	return errors.New("union set is read only")
}

func (us unionSet) Delete(ctx context.Context, id cadata.ID) error {
	return errors.New("union set is read only")
}

func (us unionSet) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	var exists bool
	if err := us.db.View(ctx, func(tx bcdb.Tx) error {
		n, err := rcGet(tx, id)
		if err != nil {
			return err
		}
		exists = n > 0
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (s unionSet) List(ctx context.Context, first []byte, ids []cadata.ID) (n int, _ error) {
	span := state.ByteSpan{
		Begin: append([]byte(setRefCountsPrefix), first...),
		End:   bcdb.PrefixEnd([]byte(setRefCountsPrefix)),
	}
	err := s.db.View(ctx, func(tx bcdb.Tx) error {
		var i int
		if err := tx.ForEach(span, func(k, _ []byte) error {
			id := cadata.IDFromBytes(k)
			ids[i] = id
			i++
			return nil
		}); err != nil {
			return err
		}
		n = i
		return cadata.ErrEndOfList
	})
	return n, err
}

func uint64Bytes(x uint64) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:]
}

func parseUint64(x []byte) (uint64, error) {
	if len(x) < 8 {
		return 0, fmt.Errorf("too short to be uint64 %q", x)
	}
	return binary.BigEndian.Uint64(x), nil
}
