package blobcache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blobcache/blobcache/pkg/bcdb"
)

const (
	metadataPrefix = "metadata\x00"
	idSequence     = "id_seq"
)

type PinSetInfo struct {
	CreatedAt time.Time `json:"created_at"`
}

// pinSetStore stores metadata about PinSets
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
	data, err := json.Marshal(PinSetInfo{
		CreatedAt: time.Now(),
	})
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
		return tx.Delete(k)
	})
}

func (s *pinSetStore) Update(ctx context.Context, psID PinSetID, fn func(*PinSetInfo) (*PinSetInfo, error)) error {
	return s.db.Update(ctx, func(tx bcdb.Tx) error {
		return s.updateTx(tx, psID, fn)
	})
}

func (s *pinSetStore) updateTx(tx bcdb.Tx, psID PinSetID, fn func(*PinSetInfo) (*PinSetInfo, error)) error {
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

func (s *pinSetStore) metadataKey(id PinSetID) []byte {
	return append([]byte(metadataPrefix), uint64Bytes(uint64(id))...)
}
