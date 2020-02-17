package blobnet

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	log "github.com/sirupsen/logrus"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

const (
	ChannelPeerRoutingV0 = "blobcache/peer-routing-v0"
	ChannelBlobRoutingV0 = "blobcache/blob-routing-v0"
	ChannelFetchingV0    = "blobcache/fetching-v0"
)

type Params struct {
	PeerStore PeerStore
	Mux       simplemux.Muxer
	KV        bckv.KV
	Local     blobs.Getter
}

type Blobnet struct {
	mux        simplemux.Muxer
	peerRouter *Router
	blobRouter *BlobRouter
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
	bn.peerRouter = NewRouter(RouterParams{
		Swarm:     rSwarm.(p2p.SecureAskSwarm),
		PeerStore: params.PeerStore,
	})

	// blob router
	brSwarm, err := bn.mux.OpenChannel(ChannelBlobRoutingV0)
	if err != nil {
		panic(err)
	}
	bn.blobRouter = NewBlobRouter(BlobRouterParams{
		Swarm:     brSwarm.(p2p.SecureAskSwarm),
		PeerStore: params.PeerStore,
		KV:        params.KV.Bucket("blob-router"),
	})

	// fetcher
	fSwarm, err := bn.mux.OpenChannel(ChannelFetchingV0)
	if err != nil {
		panic(err)
	}
	bn.fetcher = NewFetcher(FetcherParams{
		Swarm:     fSwarm.(p2p.SecureAskSwarm),
		PeerStore: params.PeerStore,
		Local:     params.Local,
	})

	return bn
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

func (bn *Blobnet) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	return bn.fetcher.Get(ctx, id)
}

func (bn *Blobnet) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	_, err := bn.Get(ctx, id)
	return err == nil, err
}
