package blobs

import (
	"context"
	"fmt"
)

type ReadChain []Getter

func (c ReadChain) Get(ctx context.Context, id ID) (Blob, error) {
	errs := []error{}
	for _, s := range c {
		data, err := s.Get(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		return data, nil
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("multiple errors: %v", errs)
	}
	return nil, ErrNotFound
}

func (c ReadChain) List(ctx context.Context, prefix []byte, ids []ID) (n int, err error) {
	for _, s := range c {
		if l, ok := s.(Lister); ok {
			n2, err := l.List(ctx, prefix, ids[n:])
			if err == ErrTooMany {
				return -1, err
			}
			n += n2
		}
	}
	return n, nil
}

func (c ReadChain) Exists(ctx context.Context, id ID) (bool, error) {
	errs := []error{}
	for _, s := range c {
		exists, err := s.Exists(ctx, id)
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
