package blobnet

import (
	"context"
	"io"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
)

type BlobEntry struct {
	Key       [16]byte
	PeerID    p2p.PeerID
	ExpiresAt time.Time
}

type BlobRouterParams struct {
	Swarm p2p.SecureAskSwarm
	PeerStore

	CacheSize int
}

type BlobRouter struct {
	peerSwarm *PeerSwarm
	cache     *kademlia.Cache
	cf        context.CancelFunc
}

func NewBlobRouter(params BlobRouterParams) *BlobRouter {
	cacheSize := params.CacheSize
	if cacheSize < 1 {
		cacheSize = 1e6
	}

	peerSwarm := NewPeerSwarm(params.Swarm, params.PeerStore)
	localID := peerSwarm.LocalID()

	ctx, cf := context.WithCancel(context.Background())
	br := &BlobRouter{
		peerSwarm: peerSwarm,
		cache:     kademlia.NewCache(localID[:], cacheSize, 0),
		cf:        cf,
	}
	peerSwarm.OnAsk(br.handleAsk)
	go br.run(ctx)

	return br
}

func (br *BlobRouter) Close() error {
	br.cf()
	return nil
}

func (br *BlobRouter) WhoHas(id blobs.ID) *p2p.PeerID {
	// TODO
	return nil
}

func (br *BlobRouter) run(ctx context.Context) {
	select {
	case <-ctx.Done():
	}
}

func (br *BlobRouter) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
}
