package blobcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/trie"
	bolt "go.etcd.io/bbolt"
)

const (
	bucketPinSets      = "pinsets"
	bucketPinRefCounts = "pinrefcount"
)

var (
	ErrPinSetExists   = errors.New("pinset exists")
	ErrPinSetNotFound = errors.New("pinset not found")
)

type PinSet struct {
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

func (s *PinSetStore) Create(ctx context.Context, name string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPinSets))
		_, err := b.CreateBucketIfNotExists([]byte(name))
		if err == bolt.ErrBucketExists {
			err = ErrPinSetExists
		}
		return err
	})
	return err
}

func (s *PinSetStore) Pin(ctx context.Context, name string, id blobs.ID) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		rc := tx.Bucket([]byte(bucketPinRefCounts))
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket([]byte(name))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		if err := pinSetB.Put([]byte(name), []byte{}); err != nil {
			return err
		}
		return pinIncr(rc, []byte(name))
	})
	return err
}

func (s *PinSetStore) Unpin(ctx context.Context, name string, id blobs.ID) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		rc := tx.Bucket([]byte(bucketPinRefCounts))
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket([]byte(name))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		if err := pinSetB.Delete([]byte(name)); err != nil {
			return err
		}
		return pinDecr(rc, []byte(name))
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

func (s *PinSetStore) Get(ctx context.Context, name string) (*PinSet, error) {
	//TODO: cache this in the pinsets bucket
	// i.e. pinset name -> json pinset data
	// so we don't have to build the Trie every time

	var ps *PinSet
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket([]byte(name))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		t := trie.New(blobs.NewMem())
		count := uint64(0)
		err := pinSetB.ForEach(func(k, v []byte) error {
			return t.Put(ctx, k, nil)
		})
		if err != nil {
			return err
		}
		ps = &PinSet{
			Name:  name,
			Root:  t.ID(),
			Count: count,
		}
		return nil
	})
	return ps, err
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

func pinIncr(b *bolt.Bucket, key []byte) error {
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

func pinDecr(b *bolt.Bucket, key []byte) error {
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
