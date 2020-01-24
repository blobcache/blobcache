package blobcache

import (
	"context"
	"errors"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/trie"
	bolt "go.etcd.io/bbolt"
)

const (
	bucketPinSets = "pinsets"
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

func NewPinSetStore(db *bolt.DB) (*PinSetStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketPinSets))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &PinSetStore{
		db: db,
	}, nil
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
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket([]byte(name))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		return pinSetB.Put([]byte(name), []byte{})
	})
	return err
}

func (s *PinSetStore) Unpin(ctx context.Context, name string, id blobs.ID) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketPinSets))
		pinSetB := b.Bucket([]byte(name))
		if pinSetB == nil {
			return ErrPinSetNotFound
		}
		return pinSetB.Put([]byte(name), []byte{})
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

func (s *PinSetStore) ListWithPrefix(ctx context.Context, name string, prefix []byte) ([]blobs.ID, error) {
	panic("not implemented")
}
