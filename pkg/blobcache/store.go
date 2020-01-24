package blobcache

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	bolt "go.etcd.io/bbolt"
)

const bucketData = "data"

type LocalStore struct {
	db       *bolt.DB
	capacity uint64
}

func newLocalStore(db *bolt.DB, capacity uint64) (*LocalStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketData))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &LocalStore{
		db:       db,
		capacity: capacity,
	}, nil
}

func (s *LocalStore) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketData))
		v := b.Get(id[:])
		if v == nil {
			return blobs.ErrNotFound
		}
		data = append([]byte{}, v...)
		return nil
	})
	return data, err
}

func (s *LocalStore) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	exists := false
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketData))
		v := b.Get(id[:])
		exists = v == nil
		return nil
	})
	return exists, err
}

func (s *LocalStore) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketData))
		return b.Put(id[:], data)
	})
	return id, err
}

func (s *LocalStore) Count() (int, error) {
	count := -1
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketData))
		stats := b.Stats()
		count = stats.KeyN
		return nil
	})
	return count, err
}
