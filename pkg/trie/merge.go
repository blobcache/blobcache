package trie

import (
	"bytes"
	"context"
	"errors"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

func Merge(ctx context.Context, a, b *Trie) (*Trie, error) {
	switch {
	case a.Children != nil && b.Children != nil:
		return merge2Parents(ctx, a, b)
	case a.Children != nil:
		return mergeParentChild(ctx, a, b)
	case b.Children != nil:
		return mergeParentChild(ctx, b, a)
	default:
		return mergeChildren(ctx, a, b)
	}
}

func mergeChildren(ctx context.Context, ts ...*Trie) (*Trie, error) {
	m := New(ts[0].store)
	for _, t := range ts {
		for _, e := range t.Entries {
			if err := m.Put(ctx, e); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

func mergeParentChild(ctx context.Context, parent, child *Trie) (*Trie, error) {
	m := New(parent.store)
	m.Prefix = parent.Prefix
	m.Children = new([256]blobs.ID)

	splitEntries := [256][]Pair{}
	for _, p := range child.Entries {
		i := p.Key[0]
		p2 := Pair{Key: p.Key[1:], Value: p.Value}
		splitEntries[i] = append(splitEntries[i], p2)
	}

	for i, ref := range parent.Children {
		data, err := parent.store.Get(ctx, ref)
		if err != nil {
			return nil, err
		}
		subT1, err := FromBytes(parent.store, data)
		if err != nil {
			return nil, err
		}
		subT2 := &Trie{
			store:   parent.store,
			Prefix:  append(child.Prefix, byte(i)),
			Entries: splitEntries[i],
		}

		m2, err := Merge(ctx, subT1, subT2)
		if err != nil {
			return nil, err
		}
		ref, err := parent.store.Post(ctx, m2.Marshal())
		if err != nil {
			return nil, err
		}
		m.Children[i] = ref
	}
	return m, nil
}

func merge2Parents(ctx context.Context, a, b *Trie) (*Trie, error) {
	if bytes.Compare(a.Prefix, b.Prefix) != 0 {
		return nil, errors.New("cannot merge tries with different prefix")
	}
	m := New(a.store)
	m.Children = new([256]blobs.ID)
	m.Prefix = a.Prefix

	for i := range a.Children {
		// A
		refA := a.Children[i]
		data, err := a.store.Get(ctx, refA)
		if err != nil {
			return nil, err
		}
		subTA, err := FromBytes(a.store, data)
		if err != nil {
			return nil, err
		}

		// B
		refB := b.Children[i]
		data, err = b.store.Get(ctx, refB)
		if err != nil {
			return nil, err
		}
		subTB, err := FromBytes(b.store, data)
		if err != nil {
			return nil, err
		}

		// merged
		m2, err := Merge(ctx, subTA, subTB)
		if err != nil {
			return nil, err
		}
		ref, err := a.store.Post(ctx, m2.Marshal())
		if err != nil {
			return nil, err
		}
		m.Children[i] = ref
	}

	return m, nil
}
