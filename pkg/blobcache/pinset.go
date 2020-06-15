package blobcache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
)

const (
	bucketPinSets      = "pinsets"
	bucketPinSetNames  = "pinsets-names"
	bucketPinRefCounts = "pinrefcount"

	idBucketFmt = "%016x"
)

var (
	ErrPinSetExists   = errors.New("pinset exists")
	ErrPinSetNotFound = errors.New("pinset not found")
)

type PinSetID int64

type PinSet struct {
	ID    PinSetID `json:"id"`
	Name  string   `json:"name"`
	Root  blobs.ID `json:"root"`
	Count uint64   `json:"count"`
}

type PinSetStore struct {
	db bcstate.TxDB
}

func NewPinSetStore(db bcstate.TxDB) *PinSetStore {
	return &PinSetStore{
		db: db,
	}
}

func (s *PinSetStore) Create(ctx context.Context, name string) (PinSetID, error) {
	var id PinSetID
	err := s.db.WriteTx(ctx, func(tx bcstate.DB) error {
		b := tx.Bucket(path.Join(bucketPinSets))
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		id = PinSetID(seq)
		return err
	})
	return id, err
}

func (s *PinSetStore) Pin(ctx context.Context, psID PinSetID, id blobs.ID) error {
	err := s.db.WriteTx(ctx, func(tx bcstate.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcstate.Exists(b, idToKey(psID))
		if err != nil {
			return err
		}
		if !exists {
			return ErrPinSetNotFound
		}

		pinSetB := tx.Bucket(idToBucket(psID))
		if err := pinSetB.Put(id[:], []byte{}); err != nil {
			return err
		}

		rc := tx.Bucket(bucketPinRefCounts)
		return pinIncr(rc, id)
	})
	return err
}

func (s *PinSetStore) Unpin(ctx context.Context, psID PinSetID, id blobs.ID) error {
	err := s.db.WriteTx(ctx, func(tx bcstate.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcstate.Exists(b, idToKey(psID))
		if err != nil {
			return err
		}
		if !exists {
			return ErrPinSetNotFound
		}

		pinSetB := tx.Bucket(idToBucket(psID))
		if err := pinSetB.Delete(id[:]); err != nil {
			return err
		}

		rc := tx.Bucket(bucketPinRefCounts)
		return pinDecr(rc, id)
	})
	return err
}

func (s *PinSetStore) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	var exists bool
	err := s.db.ReadTx(ctx, func(tx bcstate.DB) error {
		var err error
		rc := tx.Bucket(bucketPinRefCounts)
		exists, err = bcstate.Exists(rc, id[:])
		return err
	})
	return exists, err
}

func (s *PinSetStore) Get(ctx context.Context, id PinSetID) (*PinSet, error) {
	//TODO: cache this in the pinsets bucket
	// so we don't have to build the Trie every time
	var ps *PinSet
	err := s.db.ReadTx(ctx, func(tx bcstate.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcstate.Exists(b, idToKey(id))
		if err != nil {
			return err
		}
		if !exists {
			return ErrPinSetNotFound
		}

		pinSetB := tx.Bucket(idToBucket(id))
		t := tries.New(blobs.NewMem())
		count := uint64(0)
		err = pinSetB.ForEach(nil, nil, func(k, v []byte) error {
			return t.Put(ctx, k, nil)
		})
		if err != nil {
			return err
		}
		ps = &PinSet{
			ID:    id,
			Root:  blobs.Hash(t.Marshal()),
			Count: count,
		}
		return nil
	})
	return ps, err
}

func (s *PinSetStore) Delete(ctx context.Context, id PinSetID) error {
	return s.db.WriteTx(ctx, func(tx bcstate.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcstate.Exists(b, idToKey(id))
		if err != nil {
			return err
		}
		if !exists {
			return ErrPinSetNotFound
		}

		// first decrement all the pins
		rc := tx.Bucket(bucketPinRefCounts)
		pinSetB := tx.Bucket(idToBucket(id))
		err = pinSetB.ForEach(nil, nil, func(k, v []byte) error {
			blobID := blobs.ID{}
			copy(blobID[:], k)
			if err := pinDecr(rc, blobID); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return b.Delete(idToKey(id))
	})
}

func (s *PinSetStore) List(ctx context.Context, prefix []byte, ids []blobs.ID) (n int, err error) {
	err = s.db.ReadTx(ctx, func(tx bcstate.DB) error {
		rc := tx.Bucket(bucketPinRefCounts)
		return rc.ForEach(prefix, bcstate.PrefixEnd(prefix), func(k, v []byte) error {
			if n >= len(ids) {
				return blobs.ErrTooMany
			}
			copy(ids[n][:], k)
			n++
			return nil
		})
	})
	return n, err
}

func pinIncr(b bcstate.KV, id blobs.ID) error {
	key := id[:]
	return b.GetF(key, func(data []byte) error {
		if data == nil {
			data := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(key, 1)
			data = data[:n]
		} else {
			x, _ := binary.Uvarint(data)
			x++
			n := binary.PutUvarint(data, x)
			data = data[:n]
		}
		return b.Put(key, data)
	})
}

func pinDecr(b bcstate.KV, id blobs.ID) error {
	key := id[:]
	return b.GetF(key, func(data []byte) error {
		if data == nil {
			return errors.New("can't decrement null")
		}
		x, _ := binary.Uvarint(data)
		x--
		if x == 0 {
			return b.Delete(key)
		}
		data2 := make([]byte, 8)
		n := binary.PutUvarint(data2, x)
		return b.Put(key, data2[:n])
	})
}

func idToBucket(id PinSetID) string {
	return path.Join(bucketPinSets, fmt.Sprintf(idBucketFmt, id))
}

func idToKey(id PinSetID) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(id))
	return buf[:]
}

func keyToID(x []byte) PinSetID {
	return PinSetID(binary.BigEndian.Uint64(x))
}
