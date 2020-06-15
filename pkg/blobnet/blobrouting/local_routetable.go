package blobrouting

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
)

var _ RouteTable = &LocalRT{}

type Indexable interface {
	blobs.Lister
	blobs.Getter
}

type LocalRT struct {
	localID p2p.PeerID
	store   Indexable
}

func NewLocalRT(store Indexable, localID p2p.PeerID) *LocalRT {
	return &LocalRT{
		localID: localID,
		store:   store,
	}
}

func (rt *LocalRT) Put(context.Context, blobs.ID, p2p.PeerID) error {
	panic("don't call put on local route table")
}

func (rt *LocalRT) Query(ctx context.Context, prefix []byte) (tries.Trie, error) {
	ids := make([]blobs.ID, tries.HowManyUniform(len(prefix), 64))
	n, err := rt.store.List(ctx, prefix, ids)
	switch {
	case err == blobs.ErrTooMany:
		children := [256]tries.Trie{}
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			t, err := rt.Query(ctx, prefix2)
			if err != nil {
				return nil, err
			}
			children[i] = t
		}
		return tries.NewParent(ctx, blobs.Void{}, children)
	case err != nil:
		return nil, err
	default:
		t := tries.NewWithPrefix(nil, prefix)
		for _, id := range ids[:n] {
			key := makeKey(id, rt.localID)
			if err := t.Put(ctx, key, nil); err != nil {
				return nil, err
			}
		}
		return t, nil
	}
}

func (rt *LocalRT) Lookup(ctx context.Context, blobID blobs.ID) ([]p2p.PeerID, error) {
	exists, err := rt.store.Exists(ctx, blobID)
	if err != nil {
		return nil, err
	}
	if exists {
		return []p2p.PeerID{rt.localID}, nil
	}
	return nil, nil
}
