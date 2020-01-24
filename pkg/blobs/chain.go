package blobs

import (
	"context"
	"errors"
	"fmt"
	"log"
)

type Chain []Getter

func (c Chain) Get(ctx context.Context, id ID) (Blob, error) {
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

func (c Chain) Post(ctx context.Context, b Blob) (ID, error) {
	var ret ID

	for _, s := range c {
		if p, ok := s.(Poster); ok {
			id, err := p.Post(ctx, b)
			if err != nil {
				log.Println(err)
			}
			ret = id
		}
	}
	if ret == ZeroID() {
		return ret, errors.New("no successful posts")
	}
	return ret, nil
}

func (c Chain) List(ctx context.Context, prefix []byte, ids []ID) (n int, err error) {
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
