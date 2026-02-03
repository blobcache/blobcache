package pools

import (
	"context"
	"errors"
)

// OpenClose is a pool of resources which have
// an open and close cost to be amoritized by staying in the pool.
type OpenClose[T any] struct {
	freelist chan T
	mk       func(context.Context) (T, error)
	close    func(T) error
}

func NewOpenClose[T any](n int, mk func(ctx context.Context) (T, error), close func(T) error) OpenClose[T] {
	return OpenClose[T]{
		freelist: make(chan T, n),
		mk:       mk,
		close:    close,
	}
}

func (p *OpenClose[T]) Take(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case x := <-p.freelist:
		return x, nil
	default:
		return p.mk(ctx)
	}
}

func (p *OpenClose[T]) Give(ctx context.Context, x T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.freelist <- x:
		return nil
	default:
		return p.close(x)
	}
}

func (p *OpenClose[T]) CloseAll() (retErr error) {
	for {
		select {
		case x := <-p.freelist:
			err := p.close(x)
			retErr = errors.Join(retErr, err)
		default:
			return retErr
		}
	}
}

func (p *OpenClose[T]) Len() int {
	return len(p.freelist)
}

func (p *OpenClose[T]) Cap() int {
	return cap(p.freelist)
}
