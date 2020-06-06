package blobcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	bolt "go.etcd.io/bbolt"
)

const (
	bucketPinSets      = "pinsets"
	bucketPinSetNames  = "pinsets-names"
	bucketPinRefCounts = "pinrefcount"
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
	db *bolt.DB
}

func NewPinSetStore(db *bolt.DB) *PinSetStore {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketPinSets))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(bucketPinRefCounts))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return &PinSetStore{
		db: db,
	}
}

func (s *PinSetStore) Create(ctx context.Context, name string) (PinSetID, error) {
	var id PinSetID
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPinSets))
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		id = PinSetID(seq)
		_, err = b.CreateBucketIfNotExists(idToKey(id))
		if err == bolt.ErrBucketExists {
			err = ErrPinSetExists
		}
		return err
	})
	return id, err
}

func (s *PinSetStore) Pin(ctx context.Context, psID PinSetID, id blobs.ID) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		rc := tx.Bucket([]byte(bucketPinRefCounts))
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket(idToKey(psID))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		if err := pinSetB.Put(id[:], []byte{}); err != nil {
			return err
		}
		return pinIncr(rc, id)
	})
	return err
}

func (s *PinSetStore) Unpin(ctx context.Context, psID PinSetID, id blobs.ID) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		rc := tx.Bucket([]byte(bucketPinRefCounts))
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket(idToKey(psID))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		if err := pinSetB.Delete(id[:]); err != nil {
			return err
		}
		return pinDecr(rc, id)
	})
	return err
}

func (s *PinSetStore) IsPinned(ctx context.Context, id blobs.ID) (bool, error) {
	var pinned bool

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPinSets))
		// iterate through buckets with cursor
		c := b.Cursor()
		c.First()
		for key, value := c.Next(); key != nil; {
			if value != nil {
				// skip over non bucket values
				continue
			}
			pinSetB := b.Bucket(key)
			v := pinSetB.Get(id[:])
			if v != nil {
				pinned = true
				return nil
			}
		}
		return nil
	})
	return pinned, err
}

func (s *PinSetStore) Get(ctx context.Context, id PinSetID) (*PinSet, error) {
	//TODO: cache this in the pinsets bucket
	// so we don't have to build the Trie every time

	var ps *PinSet
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket(idToKey(id))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		t := tries.New(blobs.NewMem())
		count := uint64(0)
		err := pinSetB.ForEach(func(k, v []byte) error {
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
	return s.db.Update(func(tx *bolt.Tx) error {
		rc := tx.Bucket([]byte(bucketPinRefCounts))
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket(idToKey(id))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		// first decrement all the pins
		c := pinSetB.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			blobID := blobs.ID{}
			copy(blobID[:], k)
			if err := pinDecr(rc, blobID); err != nil {
				return err
			}
		}
		return b.DeleteBucket(idToKey(id))
	})
}

func (s *PinSetStore) List(ctx context.Context, prefix []byte, ids []blobs.ID) (n int, err error) {
	err2 := s.db.View(func(tx *bolt.Tx) error {
		rc := tx.Bucket([]byte(bucketPinRefCounts))

		c := rc.Cursor()
		c.Seek(prefix)
		for k, _ := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			if n >= len(ids) {
				err = blobs.ErrTooMany
				return nil
			}
			copy(ids[n][:], k)
			n++
		}
		return nil
	})
	if err2 != nil {
		return 0, err
	}

	return n, err
}

func pinIncr(b *bolt.Bucket, id blobs.ID) error {
	key := id[:]
	data := b.Get(key)
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
}

func pinDecr(b *bolt.Bucket, id blobs.ID) error {
	key := id[:]
	data := b.Get(key)
	if data == nil {
		return errors.New("can't decrement null")
	}
	x, _ := binary.Uvarint(data)
	x--
	if x == 0 {
		return b.Delete(key)
	}
	n := binary.PutUvarint(data, x)
	data = data[:n]
	return b.Put(key, data)
}

func idToKey(id PinSetID) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(id))
	return buf[:]
}

func keyToID(x []byte) PinSetID {
	return PinSetID(binary.BigEndian.Uint64(x))
}
