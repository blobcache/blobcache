package tries

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
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
func (o *Operator) Walk(ctx context.Context, s cadata.Store, root Root, w Walker) error {
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
func (o *Operator) Sync(ctx context.Context, dst, src cadata.Store, root Root, fn func(*Entry) error) error {
	return o.Walk(ctx, src, root, Walker{
		ShouldWalk: func(root Root) bool {
			exists, err := dst.Exists(ctx, root.Ref.ID)
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

func (o *Operator) Populate(ctx context.Context, s cadata.Store, root Root, set cadata.Set, fn func(*Entry) error) error {
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
