package blobcache

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
)

type store struct {
	bc       API
	pinSetID PinSetID
}

func NewStore(bc API, pinSetID PinSetID) blobs.Store {
	return &store{
		bc:       bc,
		pinSetID: pinSetID,
	}
}

func (s *store) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	return s.bc.Post(ctx, s.pinSetID, data)
}

func (s *store) GetF(ctx context.Context, id blobs.ID, fn func([]byte) error) error {
	return s.bc.GetF(ctx, id, fn)
}

func (s *store) Delete(ctx context.Context, id blobs.ID) error {
	return s.bc.Unpin(ctx, s.pinSetID, id)
}

func (s *store) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	return s.bc.Exists(ctx, s.pinSetID, id)
}

func (s *store) List(ctx context.Context, prefix []byte, ids []blobs.ID) (n int, err error) {
	return s.bc.List(ctx, s.pinSetID, prefix, ids)
}
