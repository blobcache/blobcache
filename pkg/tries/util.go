package tries

import (
	"bytes"
	"context"
	"errors"

	"github.com/blobcache/blobcache/pkg/blobs"
)

type ctxKey int

const (
	ctxDeleteBlobs = ctxKey(iota)
)

func CtxDeleteBlobs(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxDeleteBlobs, true)
}

func CtxGetDeleteBlobs(ctx context.Context) bool {
	x := ctx.Value(ctxDeleteBlobs)
	if x == nil {
		return false
	}
	return x.(bool)
}

func ForEach(ctx context.Context, t Trie, prefix []byte, fn func(k, v []byte) error) error {
	triePrefix := t.GetPrefix()
	l := len(triePrefix)
	switch {
	case bytes.HasPrefix(triePrefix, prefix):
		// x >= t
		if t.IsParent() {
			for i := 0; i < 256; i++ {
				child, err := t.GetChild(ctx, byte(i))
				if err != nil {
					return err
				}
				if err := ForEach(ctx, child, prefix, fn); err != nil {
					return err
				}
			}
		}
		for _, pair := range t.ListEntries() {
			if !bytes.HasPrefix(pair.Key, prefix) {
				continue
			}
			if err := fn(pair.Key, pair.Value); err != nil {
				return err
			}
		}

	case bytes.HasPrefix(prefix, triePrefix):
		// t >= x
		if t.IsParent() {
			child, err := t.GetChild(ctx, prefix[l])
			if err != nil {
				return err
			}
			if err := ForEach(ctx, child, prefix, fn); err != nil {
				return err
			}
		}
		for _, pair := range t.ListEntries() {
			if !bytes.HasPrefix(pair.Key, prefix) {
				continue
			}
			if err := fn(pair.Key, pair.Value); err != nil {
				return err
			}
		}

	default:
		return errors.New("bad prefix")
	}
	return nil
}

type ListDelete interface {
	blobs.Lister
	blobs.Deleter
}

func GCStore(ctx context.Context, store ListDelete, ts ...Trie) error {
	refs := map[blobs.ID]struct{}{}
	for _, t := range ts {
		addRefs(ctx, refs, t)
	}

	err := blobs.ForEach(ctx, store, func(id blobs.ID) error {
		if _, exists := refs[id]; !exists {
			if err := store.Delete(ctx, id); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func addRefs(ctx context.Context, refs map[blobs.ID]struct{}, t Trie) error {
	if t.IsParent() {
		for i := 0; i < 256; i++ {
			child, err := t.GetChild(ctx, byte(i))
			if err != nil {
				return err
			}
			addRefs(ctx, refs, child)

			id := t.GetChildRef(byte(i))
			refs[id] = struct{}{}
		}
	}
	return nil
}
