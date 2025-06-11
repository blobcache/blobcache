package stores

import (
	"context"

	"go.brendoncarroll.net/state/cadata"
)

type UnionGet []cadata.Getter

func (u UnionGet) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	for _, s := range u {
		n, err := s.Get(ctx, id, buf)
		if err != nil && !cadata.IsNotFound(err) {
			return 0, err
		}
		if err == nil {
			return n, nil
		}
	}
	return 0, cadata.ErrNotFound{Key: id}
}

func (u UnionGet) Hash(x []byte) cadata.ID {
	return u[0].Hash(x)
}

func (u UnionGet) MaxSize() int {
	return u[0].MaxSize()
}
