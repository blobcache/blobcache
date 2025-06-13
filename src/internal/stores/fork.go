package stores

import (
	"context"

	"go.brendoncarroll.net/state/cadata"
)

var (
	_ cadata.Getter      = &Fork{}
	_ cadata.PostExister = &Fork{}
)

// Fork is a write layer on top of a read-only store.
type Fork struct {
	R cadata.Getter
	W interface {
		cadata.Poster
		cadata.Exister
		cadata.Deleter
	}
}

func (f *Fork) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return f.W.Post(ctx, data)
}

func (f *Fork) Delete(ctx context.Context, id cadata.ID) error {
	return f.W.Delete(ctx, id)
}

func (f *Fork) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	return f.W.Exists(ctx, id)
}

func (f *Fork) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return f.R.Get(ctx, id, buf)
}

func (f *Fork) Hash(data []byte) cadata.ID {
	return f.R.Hash(data)
}

func (f *Fork) MaxSize() int {
	return f.R.MaxSize()
}
