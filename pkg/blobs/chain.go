package blobs

import (
	"context"
	"fmt"
)

type ReadChain []Getter

func (c ReadChain) GetF(ctx context.Context, id ID, f func(Blob) error) error {
	errs := []error{}
	for _, s := range c {
		err := s.GetF(ctx, id, func(data []byte) error {
			return f(data)
		})
		if err != nil {
			if err == ErrNotFound {
				continue
			}
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple errors: %v", errs)
	}
	return ErrNotFound
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
