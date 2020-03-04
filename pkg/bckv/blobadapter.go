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
	if data == nil {
		err = blobs.ErrNotFound
	}
	return data, err
}

func (s blobAdapter) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	_, err := s.Get(ctx, id)
	return err != nil, err
}

func (s blobAdapter) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	if err := s.c.Put(id[:], data); err != nil {
		return blobs.ZeroID(), err
	}
	return id, nil
}

func (s blobAdapter) Delete(ctx context.Context, id blobs.ID) error {
	return s.c.Delete(id[:])
}

func (s blobAdapter) List(ctx context.Context, prefix []byte, ids []blobs.ID) (n int, err error) {
	first := prefix
	last := append(prefix, 0)
	err = s.c.ForEach(first, last, func(k, v []byte) error {
		if n == len(ids) {
			return blobs.ErrTooMany
		}
		id := blobs.ID{}
		copy(id[:], k)
		ids[n] = id
		n++
		return nil
	})
	return n, err
}
