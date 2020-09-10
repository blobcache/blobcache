package blobrouting

import (
	"context"
	"time"

	"github.com/blobcache/blobcache/pkg/blobs"
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

func (rt *LocalRT) List(ctx context.Context, prefix []byte, entries []RTEntry) (int, error) {
	ids := make([]blobs.ID, len(entries))
	n, err := rt.store.List(ctx, prefix, ids)
	if err != nil {
		return -1, err
	}
	now := time.Now().UTC()
	for i, id := range ids[:n] {
		entries[i] = RTEntry{
			BlobID:    id,
			PeerID:    rt.localID,
			SightedAt: now,
		}
	}
	return n, nil
}
