package tries

import (
	"bytes"
	"context"

	"blobcache.io/blobcache/src/internal/bccrypto"
	lru "github.com/hashicorp/golang-lru"
	"go.brendoncarroll.net/state/cadata"
)

type Machine struct {
	cache  *lru.Cache
	crypto *bccrypto.Worker
}

func NewMachine() *Machine {
	cache, _ := lru.New(16)
	return &Machine{
		cache:  cache,
		crypto: bccrypto.NewWorker(nil),
	}
}

// PostSlice returns a new instance containing ents
func (o *Machine) PostSlice(ctx context.Context, s cadata.Poster, ents []*Entry) (*Root, error) {
	return o.postNode(ctx, s, ents)
}

// Get retrieves a value at key if it exists, otherwise ErrNotExist is returned
func (o *Machine) Get(ctx context.Context, s cadata.Getter, root Root, key []byte) ([]byte, error) {
	if !bytes.HasPrefix(key, root.Prefix) {
		return nil, ErrNotExist
	}
	key = compressKey(root.Prefix, key)
	ents, err := o.getNode(ctx, s, root, false)
	if err != nil {
		return nil, err
	}
	if root.IsParent {
		for _, ent := range ents {
			if len(ent.Key) == 0 && len(key) == 0 {
				return ent.Value, nil
			}
			if bytes.HasPrefix(key, ent.Key) {
				root2, err := rootFromEntry(ent)
				if err != nil {
					return nil, err
				}
				return o.Get(ctx, s, *root2, key)
			}
		}
	} else {
		for _, ent := range ents {
			if bytes.Equal(key, ent.Key) {
				return ent.Value, nil
			}
		}
	}
	return nil, ErrNotExist
}

// Put returns a copy of root where key maps to value, and all other mappings are unchanged.
func (o *Machine) Put(ctx context.Context, s writeStore, root Root, key, value []byte) (*Root, error) {
	e := &Entry{Key: key, Value: value}
	return o.PutBatch(ctx, s, root, []*Entry{e})
}

// PutBatch performs a batch of put operations on ents, returning a new instance
// reflecting all the changes.
func (o *Machine) PutBatch(ctx context.Context, s writeStore, root Root, ents []*Entry) (*Root, error) {
	if root.IsParent {
		e, children, err := o.getParent(ctx, s, root, true)
		if err != nil {
			return nil, err
		}
		e2, groups := groupEntries(ents)
		if e == nil {
			e = e2
		}
		for i := range groups {
			group := groups[i]
			child := children[i]
			child2, err := o.PutBatch(ctx, s, child, group)
			if err != nil {
				return nil, err
			}
			children[i] = *child2
		}
		return o.postParent(ctx, s, children[:], e)
	} else {
		xs, err := o.getNode(ctx, s, root, true)
		if err != nil {
			return nil, err
		}
		ys := append(xs, ents...)
		return o.postNode(ctx, s, ys)
	}
}

// Delete removes
func (o *Machine) Delete(ctx context.Context, s cadata.Store, root Root, key []byte) (*Root, error) {
	if !bytes.HasPrefix(key, root.Prefix) {
		return &root, nil
	}
	key = compressKey(root.Prefix, key)
	if root.IsParent {
		panic("deleting from parent not implemented")
	} else {
		xs, err := o.getNode(ctx, s, root, false)
		if err != nil {
			return nil, err
		}
		var ys []*Entry
		for _, ent := range xs {
			if !bytes.Equal(key, ent.Key) {
				ys = append(ys, ent)
			}
		}
		return o.postLeaf(ctx, s, ys)
	}
}
