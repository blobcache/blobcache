package merklelog

import (
	"context"

	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/streams"
)

type Iterator struct {
	state State
	store schema.RO
	beg   Pos
	end   Pos
}

func NewIterator(state State, s schema.RO, beg, end Pos) *Iterator {
	return &Iterator{
		state: state,
		store: s,
		beg:   beg,
		end:   end,
	}
}

func (it *Iterator) Next(ctx context.Context, dst *CID) error {
	if it.beg >= it.end {
		return streams.EOS()
	}
	// TODO: we should keep more of the tree nodes in memory to avoid re-reading them
	// Get traverses from the root for every call.
	cid, err := Get(ctx, it.store, it.state, it.beg)
	if err != nil {
		return err
	}
	*dst = cid
	it.beg++
	return nil
}

type slidingPairIterator[T any] struct {
	inner  streams.Iterator[T]
	window [2]T
	idx    int
}

func newSlidingPairIterator[T any](inner streams.Iterator[T]) *slidingPairIterator[T] {
	return &slidingPairIterator[T]{inner: inner}
}

func (it *slidingPairIterator[T]) Next(ctx context.Context, dst *[2]T) error {
	if it.idx < 1 {
		if err := it.inner.Next(ctx, &it.window[it.idx%2]); err != nil {
			return err
		}
		it.idx++
	}
	if err := it.inner.Next(ctx, &it.window[it.idx%2]); err != nil {
		return err
	}
	it.idx++
	if it.idx%2 == 0 {
		dst[0] = it.window[0]
		dst[1] = it.window[1]
	} else {
		dst[0] = it.window[1]
		dst[1] = it.window[0]
	}
	return nil
}

type mapIterator[X, Y any] struct {
	inner streams.Iterator[X]
	fn    func(*X) Y
}

func newMapIterator[X, Y any](inner streams.Iterator[X], fn func(*X) Y) *mapIterator[X, Y] {
	return &mapIterator[X, Y]{inner: inner, fn: fn}
}

func (it *mapIterator[X, Y]) Next(ctx context.Context, dst *Y) error {
	var x X
	if err := it.inner.Next(ctx, &x); err != nil {
		return err
	}
	*dst = it.fn(&x)
	return nil
}
