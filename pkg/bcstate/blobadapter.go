package bcstate

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type blobAdapter struct {
	c       KV
	maxSize int
	hf      cadata.HashFunc
}

func BlobAdapter(c KV, maxSize int, hf cadata.HashFunc) blobAdapter {
	return blobAdapter{c: c}
}

func (s blobAdapter) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	var n int
	err := s.c.GetF(id[:], func(data []byte) error {
		n = copy(buf, data)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s blobAdapter) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	exists := false
	err := s.c.GetF(id[:], func(data []byte) error {
		exists = true
		return nil
	})
	return exists, err
}

func (s blobAdapter) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	id := s.hf(data)
	if err := s.c.Put(id[:], data); err != nil {
		return cadata.ID{}, err
	}
	return id, nil
}

func (s blobAdapter) Delete(ctx context.Context, id cadata.ID) error {
	return s.c.Delete(id[:])
}

func (s blobAdapter) List(ctx context.Context, prefix []byte, ids []cadata.ID) (n int, err error) {
	first := prefix
	last := append(prefix, 0)
	err = s.c.ForEach(first, last, func(k, v []byte) error {
		if n >= len(ids) {
			return nil
		}
		id := cadata.ID{}
		copy(id[:], k)
		ids[n] = id
		n++
		return nil
	})
	return n, err
}

func (s blobAdapter) MaxSize() int {
	return s.maxSize
}

func (s blobAdapter) Hash(x []byte) cadata.ID {
	return s.hf(x)
}
