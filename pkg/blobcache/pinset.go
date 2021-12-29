package blobcache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/dgraph-io/badger/v2"
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

type PinSetStore struct {
	db bcdb.DB
}

func NewPinSetStore(db *badger.DB) *PinSetStore {
	return &PinSetStore{
		db: db,
	}
}

// Create creates a new PinSet
func (s *PinSetStore) Create(ctx context.Context, opts PinSetOptions) (PinSetID, error) {
	var id PinSetID
	err := s.db.Update(ctx, func(tx bcdb.Tx) error {
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

// Get returns a pinset by id
func (s *PinSetStore) Get(ctx context.Context, id PinSetID) (*PinSet, error) {
	// TODO: cache this in the pinsets bucket
	// so we don't have to build the Trie every time
	var ps *PinSet
	err := s.db.View(ctx, func(tx bcdb.Tx) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcdb.Exists(b, idToKey(id))
		if err != nil {
			return err
		}
		if !exists {
			return ErrPinSetNotFound
		}

		pinSetB := tx.Bucket(idToBucket(id))
		t := tries.New()
		count := uint64(0)
		if err := pinSetB.ForEach(nil, nil, func(k, v []byte) error {
			t.Entries = append(t.Entries, &tries.Entry{
				Key: k,
			})
			count++
			return nil
		}); err != nil {
			return err
		}
		root, err := tries.PostNode(ctx, stores.NewMem(), t)
		if err != nil {
			return err
		}
		ps = &PinSet{
			ID:    id,
			Root:  root.ID,
			Count: count,
		}
		return nil
	})
	return ps, err
}

// Delete ensures a pinset does not exist
func (s *PinSetStore) Delete(ctx context.Context, id PinSetID) error {
	return s.db.WriteTx(ctx, func(tx bcdb.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcdb.Exists(b, idToKey(id))
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}

		// first decrement all the pins
		rc := tx.Bucket(bucketPinRefCounts)
		pinSetB := tx.Bucket(idToBucket(id))
		err = pinSetB.ForEach(nil, nil, func(k, v []byte) error {
			blobID := cadata.ID{}
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

// Pin ensures that a pinset contain a blob
func (s *PinSetStore) Pin(ctx context.Context, psID PinSetID, id cadata.ID) error {
	err := s.db.WriteTx(ctx, func(tx bcdb.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcdb.Exists(b, idToKey(psID))
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

// Unpin ensures that a pinset does not contain a blob
func (s *PinSetStore) Unpin(ctx context.Context, psID PinSetID, id cadata.ID) error {
	err := s.db.WriteTx(ctx, func(tx bcdb.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcdb.Exists(b, idToKey(psID))
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

// Exists returns true iff a pinset contains id
func (s *PinSetStore) Exists(ctx context.Context, psID PinSetID, id cadata.ID) (bool, error) {
	var exists bool
	err := s.db.ReadTx(ctx, func(tx bcdb.DB) error {
		b := tx.Bucket(bucketPinSets)
		exists, err := bcdb.Exists(b, idToKey(psID))
		if err != nil {
			return err
		}
		if !exists {
			return ErrPinSetNotFound
		}
		pinSetB := tx.Bucket(idToBucket(psID))
		return pinSetB.GetF(id[:], func([]byte) error {
			exists = true
			return nil
		})
	})
	return exists, err
}

// List lists all the items in the pinset
func (s *PinSetStore) List(ctx context.Context, pinSetID PinSetID, prefix []byte, ids []cadata.ID) (n int, err error) {
	err = s.db.ReadTx(ctx, func(tx bcdb.DB) error {
		rc := tx.Bucket(bucketPinRefCounts)
		return rc.ForEach(prefix, bcdb.PrefixEnd(prefix), func(k, v []byte) error {
			b := tx.Bucket(bucketPinSets)
			exists, err := bcdb.Exists(b, idToKey(pinSetID))
			if err != nil {
				return err
			}
			if !exists {
				return ErrPinSetNotFound
			}
			pinSetB := tx.Bucket(idToBucket(pinSetID))
			return pinSetB.ForEach(prefix, bcdb.PrefixEnd(prefix), func(k, v []byte) error {
				if n >= len(ids) {
					return nil
				}
				copy(ids[n][:], k)
				n++
				return nil
			})
		})
	})
	return n, err
}

func pinIncr(b bcdb.KV, id cadata.ID) error {
	key := id[:]
	return b.GetF(key, func(data []byte) error {
		if data == nil {
			data = make([]byte, binary.MaxVarintLen64)
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

func pinDecr(b bcdb.KV, id cadata.ID) error {
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
