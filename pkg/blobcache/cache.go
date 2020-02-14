package blobcache

import (
	"context"
	"errors"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

var ErrCacheFull = errors.New("cache is full")

type Cache interface {
	Put(ctx context.Context, key, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)

	SizeUsed() uint64
	SizeTotal() uint64
}

type localStore struct {
	c Cache
}

func newLocalStore(c Cache) *localStore {
	return &localStore{c: c}
}

func (s *localStore) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	data, err := s.c.Get(ctx, id[:])
	if err != nil {
		err = blobs.ErrNotFound
	}
	return data, err
}

func (s *localStore) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	_, err := s.Get(ctx, id)
	return err != nil, err
}
