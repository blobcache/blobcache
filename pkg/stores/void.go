package stores

import (
	"context"

	"go.brendoncarroll.net/state/cadata"
)

var _ cadata.Store = Void{}

type Void struct{}

func (s Void) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return cadata.DefaultHash(data), nil
}

func (s Void) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return 0, cadata.ErrNotFound{Key: id}
}

func (s Void) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return false, nil
}

func (s Void) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	return 0, nil
}

func (s Void) Delete(ctx context.Context, id cadata.ID) error {
	return nil
}

func (s Void) MaxSize() int {
	return MaxSize
}

func (s Void) Hash(x []byte) cadata.ID {
	return s.Hash(x)
}
