package blobcache

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type store struct {
	bc     Service
	pinSet Handle
}

// NewStore returns a cadata.Store
func NewStore(bc Service, pinSet Handle) cadata.Store {
	return &store{
		bc:     bc,
		pinSet: pinSet,
	}
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return s.bc.Post(ctx, s.pinSet, data)
}

func (s *store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return s.bc.Get(ctx, s.pinSet, id, buf)
}

func (s *store) Add(ctx context.Context, id cadata.ID) error {
	return s.bc.Add(ctx, s.pinSet, id)
}

func (s *store) Delete(ctx context.Context, id cadata.ID) error {
	return s.bc.Delete(ctx, s.pinSet, id)
}

func (s *store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return s.bc.Exists(ctx, s.pinSet, id)
}

func (s *store) List(ctx context.Context, id cadata.ID, ids []cadata.ID) (n int, err error) {
	return s.bc.List(ctx, s.pinSet, id, ids)
}

func (s *store) Hash(x []byte) cadata.ID {
	return Hash(x)
}

func (s *store) MaxSize() int {
	return MaxSize
}
