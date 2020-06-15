package bcstate

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
)

type blobAdapter struct {
	c KV
}

func BlobAdapter(c KV) blobAdapter {
	return blobAdapter{c: c}
}

func (s blobAdapter) GetF(ctx context.Context, id blobs.ID, fn func(data []byte) error) error {
	return s.c.GetF(id[:], fn)
}

func (s blobAdapter) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	exists := false
	err := s.GetF(ctx, id, func(data []byte) error {
		exists = true
		return nil
	})
	return exists, err
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
