package bckv

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

type blobAdapter struct {
	c KV
}

func BlobAdapter(c KV) blobAdapter {
	return blobAdapter{c: c}
}

func (s blobAdapter) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	data, err := s.c.Get(id[:])
	if err != nil {
		err = blobs.ErrNotFound
	}
	return data, err
}

func (s blobAdapter) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	_, err := s.Get(ctx, id)
	return err != nil, err
}
