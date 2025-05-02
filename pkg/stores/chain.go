package stores

import (
	"context"
	"fmt"

	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/kv"
)

type ReadOnly interface {
	cadata.Getter
	cadata.Lister
}

type ReadChain []ReadOnly

func (c ReadChain) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	errs := []error{}
	for _, s := range c {
		n, err := s.Get(ctx, id, buf)
		if err != nil {
			if cadata.IsNotFound(err) {
				continue
			}
			errs = append(errs, err)
		} else {
			return n, err
		}
	}
	if len(errs) > 0 {
		return 0, fmt.Errorf("multiple errors: %v", errs)
	}
	return 0, cadata.ErrNotFound{Key: id}
}

func (c ReadChain) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (n int, err error) {
	for _, s := range c {
		if n == len(ids) {
			return n, nil
		}
		if l, ok := s.(cadata.Lister); ok {
			n2, err := l.List(ctx, span, ids[n:])
			if err != nil {
				return 0, err
			}
			n += n2
		}
	}
	return n, nil
}

func (c ReadChain) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	errs := []error{}
	for _, s := range c {
		exists, err := kv.ExistsUsingList(ctx, s, id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if exists {
			return true, nil
		}
	}
	if len(errs) > 0 {
		return false, fmt.Errorf("multiple errors: %v", errs)
	}
	return false, nil
}

func (c ReadChain) MaxSize() int {
	return c[0].MaxSize()
}

func (c ReadChain) Hash(x []byte) cadata.ID {
	return c[0].Hash(x)
}
