package blobcache

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"

	"github.com/blobcache/blobcache/pkg/bcdb"
)

const (
	refCountPrefix = "rc\x00"
	pinPrefix      = "pin\x00"
	metadataPrefix = "metadata\x00"
	idSequence     = "id_seq"
)

type PinSetInfo struct {
	Count uint64
}

// pinSetStore stores metadata about PinSets and which blobs are contained by which pinsets
type pinSetStore struct {
	db bcdb.DB
}

func newPinSetStore(db bcdb.DB) *pinSetStore {
	return &pinSetStore{
		db: db,
	}
}

// Create creates a new PinSet
func (s *pinSetStore) Create(ctx context.Context, opts PinSetOptions) (*PinSetID, error) {
	data, err := json.Marshal(PinSetInfo{})
	if err != nil {
		return nil, err
	}
	var id PinSetID
	if err := s.db.Update(ctx, func(tx bcdb.Tx) error {
		seq, err := bcdb.Increment(tx, []byte(idSequence))
		if err != nil {
			return err
		}
		id = PinSetID(seq)
		return tx.Put(s.metadataKey(id), data)
	}); err != nil {
		return nil, err
	}
	return &id, nil
}

// Get returns a pinset by id
// Get returns (nil, nil) if no PinSet at id exists
func (s *pinSetStore) Get(ctx context.Context, id PinSetID) (ret *PinSetInfo, _ error) {
	err := s.db.View(ctx, func(tx bcdb.Tx) error {
		k := s.metadataKey(id)
		value, err := tx.Get(k)
		if err != nil {
			return err
		}
		if value == nil {
			return ErrPinSetNotFound
		}
		var info PinSetInfo
		if err := json.Unmarshal(value, &info); err != nil {
			return err
		}
		ret = &info
		return nil
	})
	return ret, err
}

// Delete ensures a pinset does not exist
func (s *pinSetStore) Delete(ctx context.Context, id PinSetID) error {
	return s.db.Update(ctx, func(tx bcdb.Tx) error {
		k := s.metadataKey(id)
		value, err := tx.Get(k)
		if err != nil {
			return err
		}
		if value == nil {
			return nil
		}
		if err := tx.Delete(k); err != nil {
			return err
		}
		// decrement all the pins
		return s.forEachPin(tx, id, nil, func(blobID cadata.ID) error {
			if _, err := s.rcDecr(tx, blobID); err != nil {
				return err
			}
			return tx.Delete(k)
		})
	})
}

// Pin ensures that a pinset contain a blob
func (s *pinSetStore) Pin(ctx context.Context, psID PinSetID, id cadata.ID) error {
	err := s.db.Update(ctx, func(tx bcdb.Tx) error {
		if value, err := tx.Get(s.metadataKey(psID)); err != nil {
			return err
		} else if value == nil {
			return ErrPinSetNotFound
		}
		pinKey := s.pinKey(psID, id)
		if value, err := tx.Get(pinKey); err != nil {
			return err
		} else if value != nil {
			return nil
		}
		if err := tx.Put(pinKey, nil); err != nil {
			return err
		}
		_, err := s.rcIncr(tx, id)
		if err != nil {
			return err
		}
		if err := s.updateInfo(tx, psID, func(info *PinSetInfo) (*PinSetInfo, error) {
			if info == nil {
				return nil, ErrPinSetNotFound
			}
			info.Count++
			return info, nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Unpin ensures that a pinset does not contain a blob
func (s *pinSetStore) Unpin(ctx context.Context, psID PinSetID, id cadata.ID) (ret uint64, _ error) {
	err := s.db.Update(ctx, func(tx bcdb.Tx) error {
		if value, err := tx.Get(s.metadataKey(psID)); err != nil {
			return err
		} else if value == nil {
			return ErrPinSetNotFound
		}
		pinKey := s.pinKey(psID, id)
		if value, err := tx.Get(pinKey); err != nil {
			return err
		} else if value == nil {
			count, err := s.rcGet(tx, id)
			if err != nil {
				return err
			}
			ret = count
			return nil
		}
		if err := tx.Delete(pinKey); err != nil {
			return err
		}
		count, err := s.rcDecr(tx, id)
		if err != nil {
			return err
		}
		if err := s.updateInfo(tx, psID, func(info *PinSetInfo) (*PinSetInfo, error) {
			if info == nil {
				return nil, ErrPinSetNotFound
			}
			info.Count--
			return info, nil
		}); err != nil {
			return err
		}
		ret = count
		return nil
	})
	if err != nil {
		return 0, err
	}
	return ret, nil
}

// Exists returns true iff a pinset contains id
func (s *pinSetStore) Exists(ctx context.Context, psID PinSetID, id cadata.ID) (bool, error) {
	var exists bool
	err := s.db.View(ctx, func(tx bcdb.Tx) error {
		if value, err := tx.Get(s.metadataKey(psID)); err != nil {
			return err
		} else if value == nil {
			return ErrPinSetNotFound
		}
		value, err := tx.Get(s.pinKey(psID, id))
		if err != nil {
			return err
		}
		exists = value != nil
		return nil
	})
	return exists, err
}

// List lists all the items in the pinset
func (s *pinSetStore) List(ctx context.Context, psID PinSetID, first []byte, ids []cadata.ID) (n int, err error) {
	stopIter := errors.New("stop iteration")
	err = s.db.View(ctx, func(tx bcdb.Tx) error {
		if value, err := tx.Get(s.metadataKey(psID)); err != nil {
			return err
		} else if value == nil {
			return ErrPinSetNotFound
		}
		var count int
		err := s.forEachPin(tx, psID, first, func(id cadata.ID) error {
			if count >= len(ids) {
				return stopIter
			}
			ids[count] = id
			count++
			return nil
		})
		if errors.Is(err, stopIter) {
			err = nil
		} else {
			err = cadata.ErrEndOfList
		}
		n = count
		return err
	})
	return n, err
}

func (s *pinSetStore) GetRefCount(ctx context.Context, id cadata.ID) (count uint64, _ error) {
	if err := s.db.View(ctx, func(tx bcdb.Tx) error {
		var err error
		count, err = s.rcGet(tx, id)
		return err
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *pinSetStore) updateInfo(tx bcdb.Tx, psID PinSetID, fn func(*PinSetInfo) (*PinSetInfo, error)) error {
	key := s.metadataKey(psID)
	value, err := tx.Get(key)
	if err != nil {
		return err
	}
	var x *PinSetInfo
	if len(value) > 0 {
		x = &PinSetInfo{}
		if err := json.Unmarshal(value, x); err != nil {
			return err
		}
	}
	y, err := fn(x)
	if err != nil {
		return err
	}
	value2, err := json.Marshal(y)
	if err != nil {
		return err
	}
	return tx.Put(key, value2)
}

func (s *pinSetStore) forEachPin(tx bcdb.Tx, psID PinSetID, first []byte, fn func(cadata.ID) error) error {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(psID))
	prefix := append([]byte(pinPrefix), buf[:]...)
	span := state.ByteRange{
		Begin: append(prefix, first...),
		End:   bcdb.PrefixEnd(prefix),
	}
	return tx.ForEach(span, func(key, value []byte) error {
		_, blobID, err := s.parsePin(key)
		if err != nil {
			return nil
		}
		return fn(blobID)
	})
}

func (s *pinSetStore) metadataKey(id PinSetID) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(id))
	return append([]byte(metadataPrefix), buf[:]...)
}

func (s *pinSetStore) pinKey(psID PinSetID, blobID cadata.ID) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(psID))
	k := []byte(pinPrefix)
	k = append(k, buf[:]...)
	k = append(k, blobID[:]...)
	return k
}

func (s *pinSetStore) parsePin(k []byte) (PinSetID, cadata.ID, error) {
	if len(k) < len(pinPrefix)+8+cadata.IDSize {
		return 0, cadata.ID{}, errors.Errorf("key=%q len=%d is too short to be pin", k, len(k))
	}
	psID := PinSetID(binary.BigEndian.Uint64(k[len(pinPrefix) : len(pinPrefix)+8]))
	blobID := cadata.IDFromBytes(k[len(pinPrefix)+8:])
	return psID, blobID, nil
}

func (s *pinSetStore) rcKey(id cadata.ID) []byte {
	return append([]byte(refCountPrefix), id[:]...)
}

func (s *pinSetStore) rcDecr(tx bcdb.Tx, id cadata.ID) (uint64, error) {
	k := s.rcKey(id)
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

func (s *pinSetStore) rcIncr(tx bcdb.Tx, id cadata.ID) (uint64, error) {
	k := s.rcKey(id)
	return bcdb.Increment(tx, k)
}

func (s *pinSetStore) rcGet(tx bcdb.Tx, id cadata.ID) (uint64, error) {
	k := s.rcKey(id)
	v, err := tx.Get(k)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.Errorf("not a 64 bit integer")
	}
	return binary.BigEndian.Uint64(v), nil
}
