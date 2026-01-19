package tries

import (
	"context"

	"go.brendoncarroll.net/state/cadata"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
)

type Walker struct {
	// ShouldWalk is called immediately before traversing a node.
	// ShouldWalk must be set; always return true to naively walk everything.
	ShouldWalk func(root Root) bool
	// EntryFn, if set, is called for every entry.
	EntryFn func(Entry) error
	// NodeFn, must be set and is called after visiting every node.
	NodeFn func(root Root) error
}

// Walk walks a Trie calling methods on Walker throughout the traversal.
// w.ShouldWalk is called before walking a node, if false is returned the node is skipped
// w.EntryFn is called for every entry in a node
// w.NodeFn is called for the node after all the entries reachable from it have been walked.
func (mach *Machine) Walk(ctx context.Context, s schema.RO, root Root, w Walker) error {
	if !w.ShouldWalk(root) {
		return nil
	}
	node, err := mach.getNode(ctx, s, Index(root))
	if err != nil {
		return err
	}

	el, err := node.Entries()
	if err != nil {
		return err
	}
	for i := 0; i < el.Len(); i++ {
		x := el.At(i)
		switch x.Which() {
		case triescnp.Entry_Which_value:
			if w.EntryFn != nil {
				var ent Entry
				if err := ent.fromCNP(x); err != nil {
					return err
				}
				ent.Key = expandKey(root.Prefix, ent.Key)
				if err := w.EntryFn(ent); err != nil {
					return err
				}
			}
		case triescnp.Entry_Which_index:
			var ient Index
			if err := ient.fromCNP(x); err != nil {
				return err
			}
			ient.Prefix = expandKey(root.Prefix, ient.Prefix)
			root2 := Root(ient)
			return mach.Walk(ctx, s, root2, w)
		}
	}
	return w.NodeFn(root)
}

// Sync ensures that data structure exists in dst, using src to retrieve missing pieces.
// Sync is only correct if dangling references can be guarenteed to not exist in dst.
func (o *Machine) Sync(ctx context.Context, dst schema.WO, src schema.RO, root Root, fn func(Entry) error) error {
	return o.Walk(ctx, src, root, Walker{
		ShouldWalk: func(root Root) bool {
			var exists [1]bool
			err := dst.Exists(ctx, []blobcache.CID{root.Ref.CID}, exists[:])
			if err != nil {
				return false
			}
			return !exists[0]
		},
		EntryFn: fn,
		NodeFn: func(root Root) error {
			return schema.CopyBlob(ctx, src, dst, root.Ref.CID)
		},
	})
}

func (o *Machine) Populate(ctx context.Context, s schema.RO, root Root, set cadata.Set, fn func(Entry) error) error {
	return o.Walk(ctx, s, root, Walker{
		ShouldWalk: func(root Root) bool {
			exists, err := set.Exists(ctx, root.Ref.CID)
			if err != nil {
				return false
			}
			return !exists
		},
		EntryFn: fn,
		NodeFn: func(root Root) error {
			return set.Add(ctx, root.Ref.CID)
		},
	})
}
