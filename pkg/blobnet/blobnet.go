package blobnet

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/blobrouting"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peerrouting"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

const (
	ChannelPeerRoutingV0 = "blobcache/peer-routing-v0"
	ChannelBlobRoutingV0 = "blobcache/blob-routing-v0"
	ChannelFetchingV0    = "blobcache/fetching-v0"
)

type Params struct {
	PeerStore peers.PeerStore
	Mux       simplemux.Muxer
	KV        bckv.KV
	Local     blobs.Getter
	Clock     clockwork.Clock
}

type Blobnet struct {
	mux        simplemux.Muxer
	peerRouter *peerrouting.Router
	blobRouter *blobrouting.Router
	fetcher    *Fetcher
}

func NewBlobNet(params Params) *Blobnet {
	mux := params.Mux
	bn := &Blobnet{
		mux: mux,
	}

	// peer router
	rSwarm, err := bn.mux.OpenChannel(ChannelPeerRoutingV0)
	if err != nil {
		panic(err)
	}
	bn.peerRouter = peerrouting.NewRouter(peerrouting.RouterParams{
		PeerSwarm: peers.NewPeerSwarm(rSwarm.(p2p.SecureAskSwarm), params.PeerStore),
		Clock:     params.Clock,
	})

	// blob router
	brSwarm, err := bn.mux.OpenChannel(ChannelBlobRoutingV0)
	if err != nil {
		panic(err)
	}
	bn.blobRouter = blobrouting.NewRouter(blobrouting.RouterParams{
		PeerSwarm:  peers.NewPeerSwarm(brSwarm.(p2p.SecureAskSwarm), params.PeerStore),
		PeerRouter: bn.peerRouter,
		KV:         params.KV.Bucket("blob-router"),
		Clock:      params.Clock,
	})

	// fetcher
	fSwarm, err := bn.mux.OpenChannel(ChannelFetchingV0)
	if err != nil {
		panic(err)
	}
	bn.fetcher = NewFetcher(FetcherParams{
		PeerSwarm: peers.NewPeerSwarm(fSwarm.(p2p.SecureAskSwarm), params.PeerStore),
		Local:     params.Local,
	})

	return bn
}

func (bn *Blobnet) bootstrap(ctx context.Context) {
	bn.peerRouter.Bootstrap(ctx)
}

func (bn *Blobnet) Close() error {
	closers := []interface {
		Close() error
	}{
		bn.peerRouter,
		bn.blobRouter,
	}

	for _, c := range closers {
		if err := c.Close(); err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (bn *Blobnet) HaveLocally(ctx context.Context, id blobs.ID) error {
	return bn.blobRouter.Invalidate(ctx, id)
}

func (bn *Blobnet) GoneLocally(ctx context.Context, id blobs.ID) error {
	return bn.blobRouter.Invalidate(ctx, id)
}

func (bn *Blobnet) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	return bn.fetcher.Get(ctx, id)
}

func (bn *Blobnet) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	_, err := bn.Get(ctx, id)
	return err == nil, err
}
