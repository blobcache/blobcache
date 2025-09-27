package tries

import (
	"context"

	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/kv"
	"golang.org/x/sync/errgroup"
)

type Walker struct {
	ShouldWalk func(root Root) bool
	EntryFn    func(*Entry) error
	NodeFn     func(root Root) error
}

// Walk walks a Trie calling methods on Walker throughout the traversal.
// w.ShouldWalk is called before walking a node, if false is returned the node is skipped
// w.EntryFn is called for every entry in a node
// w.NodeFn is called for the node after all the entries reachable from it have been walked.
func (o *Machine) Walk(ctx context.Context, s schema.RO, root Root, w Walker) error {
	if !w.ShouldWalk(root) {
		return nil
	}
	ents, err := o.getNode(ctx, s, root, true)
	if err != nil {
		return err
	}
	if root.IsParent {
		eg := errgroup.Group{}
		for _, ent := range ents {
			if len(ent.Key) == 0 {
				if err := w.EntryFn(ent); err != nil {
					return err
				}
			} else {
				root2, err := rootFromEntry(ent)
				if err != nil {
					return err
				}
				eg.Go(func() error {
					return o.Walk(ctx, s, *root2, w)
				})
			}
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	} else {
		for _, ent := range ents {
			if err := w.EntryFn(ent); err != nil {
				return err
			}
		}
	}
	return w.NodeFn(root)
}

// Sync ensures that data structure exists in dst, using src to retrieve missing pieces.
// Sync is only correct if dangling references can be guarenteed to not exist in dst.
func (o *Machine) Sync(ctx context.Context, dst, src cadata.Store, root Root, fn func(*Entry) error) error {
	return o.Walk(ctx, src, root, Walker{
		ShouldWalk: func(root Root) bool {
			exists, err := kv.ExistsUsingList(ctx, dst, root.Ref.ID)
			if err != nil {
				exists = false
			}
			return !exists
		},
		EntryFn: fn,
		NodeFn: func(root Root) error {
			return cadata.Copy(ctx, dst, src, root.Ref.ID)
		},
	})
}

func (o *Machine) Populate(ctx context.Context, s cadata.Store, root Root, set cadata.Set, fn func(*Entry) error) error {
	return o.Walk(ctx, s, root, Walker{
		ShouldWalk: func(root Root) bool {
			exists, err := set.Exists(ctx, root.Ref.ID)
			if err != nil {
				exists = false
			}
			return !exists
		},
		EntryFn: fn,
		NodeFn: func(root Root) error {
			return set.Add(ctx, root.Ref.ID)
		},
	})
}
