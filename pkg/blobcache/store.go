package blobcache

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type store struct {
	bc       Service
	pinSetID PinSetID
}

func NewStore(bc Service, pinSetID PinSetID) cadata.Store {
	return &store{
		bc:       bc,
		pinSetID: pinSetID,
	}
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return s.bc.Post(ctx, s.pinSetID, data)
}

func (s *store) Add(ctx context.Context, id cadata.ID) error {
	return s.bc.Add(ctx, s.pinSetID, id)
}

func (s *store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return s.bc.Get(ctx, s.pinSetID, id, buf)
}

func (s *store) Delete(ctx context.Context, id cadata.ID) error {
	return s.bc.Delete(ctx, s.pinSetID, id)
}

func (s *store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return s.bc.Exists(ctx, s.pinSetID, id)
}

func (s *store) List(ctx context.Context, first []byte, ids []cadata.ID) (n int, err error) {
	return s.bc.List(ctx, s.pinSetID, first, ids)
}

func (s *store) Hash(x []byte) cadata.ID {
	return Hash(x)
}

func (s *store) MaxSize() int {
	return MaxSize
}
