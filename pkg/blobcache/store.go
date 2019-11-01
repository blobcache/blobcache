package blobcache

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	bolt "go.etcd.io/bbolt"
)

type LocalStore struct {
	db       *bolt.DB
	capacity uint64
}

func (s *LocalStore) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	panic("not implemented")
}

func (s *LocalStore) Exists(ctx context.Context, id blobs.ID) ([]byte, error) {
	panic("not implemented")
}

func (s *LocalStore) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	panic("not implemented")
}
