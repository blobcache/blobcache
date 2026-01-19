package merklelog

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
	"go.brendoncarroll.net/exp/streams"
)

type Iterator struct {
	state State
	store bcsdk.RO
	beg   Pos
	end   Pos
}

func NewIterator(state State, s bcsdk.RO, beg, end Pos) *Iterator {
	return &Iterator{
		state: state,
		store: s,
		beg:   beg,
		end:   end,
	}
}

func (it *Iterator) Next(ctx context.Context, dst []CID) (int, error) {
	if it.beg >= it.end {
		return 0, streams.EOS()
	}
	// TODO: we should keep more of the tree nodes in memory to avoid re-reading them
	// Get traverses from the root for every call.
	cid, err := Get(ctx, it.store, it.state, it.beg)
	if err != nil {
		return 0, err
	}
	dst[0] = cid
	it.beg++
	return 1, nil
}

type slidingPairIterator[T any] struct {
	inner  streams.Iterator[T]
	window [2]T
	idx    int
}

func newSlidingPairIterator[T any](inner streams.Iterator[T]) *slidingPairIterator[T] {
	return &slidingPairIterator[T]{inner: inner}
}

func (it *slidingPairIterator[T]) Next(ctx context.Context, dst [][2]T) (int, error) {
	if it.idx < 1 {
		if err := streams.NextUnit(ctx, it.inner, &it.window[it.idx%2]); err != nil {
			return 0, err
		}
		it.idx++
	}
	if err := streams.NextUnit(ctx, it.inner, &it.window[it.idx%2]); err != nil {
		return 0, err
	}
	it.idx++
	if it.idx%2 == 0 {
		dst[0][0] = it.window[0]
		dst[0][1] = it.window[1]
	} else {
		dst[0][0] = it.window[1]
		dst[0][1] = it.window[0]
	}
	return 1, nil
}

type mapIterator[X, Y any] struct {
	inner streams.Iterator[X]
	fn    func(*X) Y
}

func newMapIterator[X, Y any](inner streams.Iterator[X], fn func(*X) Y) *mapIterator[X, Y] {
	return &mapIterator[X, Y]{inner: inner, fn: fn}
}

func (it *mapIterator[X, Y]) Next(ctx context.Context, dst []Y) (int, error) {
	var x X
	if err := streams.NextUnit(ctx, it.inner, &x); err != nil {
		return 0, err
	}
	dst[0] = it.fn(&x)
	return 1, nil
}
