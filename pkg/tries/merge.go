package tries

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
)

func Merge(ctx context.Context, s blobs.GetPostDelete, ts ...Trie) (Trie, error) {
	switch {
	case len(ts) == 0:
		panic("cannot merge 0 tries")
	case len(ts) == 1:
		return ts[0], nil
	default:
		left, err := Merge(ctx, s, ts[:len(ts)/2]...)
		if err != nil {
			return nil, err
		}
		right, err := Merge(ctx, s, ts[len(ts)/2:]...)
		if err != nil {
			return nil, err
		}
		return merge(ctx, s, left, right)
	}
}

func merge(ctx context.Context, s blobs.GetPostDelete, a, b Trie) (Trie, error) {
	// TODO: make this more efficient
	aPrefix := a.GetPrefix()
	bPrefix := b.GetPrefix()
	p := aPrefix
	if len(bPrefix) < len(aPrefix) {
		p = bPrefix
	}

	m := NewWithPrefix(s, p)
	for _, t := range []Trie{a, b} {
		err := ForEach(ctx, t, nil, func(k, v []byte) error {
			if err := m.Put(ctx, k, v); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
