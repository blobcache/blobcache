package blobrouting

import (
	"context"
	"time"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	"github.com/jonboulle/clockwork"
)

var _ RouteTable = &LocalRT{}

type Indexable interface {
	blobs.Lister
	blobs.Getter
}

type LocalRT struct {
	localID p2p.PeerID
	store   Indexable
	clock   clockwork.Clock
}

func NewLocalRT(store Indexable, localID p2p.PeerID, clock clockwork.Clock) *LocalRT {
	return &LocalRT{
		localID: localID,
		store:   store,
		clock:   clock,
	}
}

func (rt *LocalRT) Put(context.Context, blobs.ID, p2p.PeerID, time.Time) error {
	panic("don't call Put on local route table")
}

func (rt *LocalRT) Delete(context.Context, blobs.ID, p2p.PeerID) error {
	panic("don't call Delete on local route table")
}

func (rt *LocalRT) GetTrie(ctx context.Context, prefix []byte) (tries.Trie, error) {
	ids := make([]blobs.ID, tries.HowManyUniform(len(prefix), 64))
	n, err := rt.store.List(ctx, prefix, ids)
	switch {
	case err == blobs.ErrTooMany:
		children := [256]tries.Trie{}
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			t, err := rt.GetTrie(ctx, prefix2)
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

func (rt *LocalRT) Lookup(ctx context.Context, blobID blobs.ID) ([]RTEntry, error) {
	exists, err := rt.store.Exists(ctx, blobID)
	if err != nil {
		return nil, err
	}
	if exists {
		entry := RTEntry{
			PeerID:    rt.localID,
			SightedAt: rt.clock.Now().UTC(),
		}
		return []RTEntry{entry}, nil
	}
	return nil, nil
}
