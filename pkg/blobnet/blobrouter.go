package blobnet

import (
	"context"
	"io"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type BlobRouterParams struct {
	PeerRouter *Router
	Swarm      p2p.SecureAskSwarm
	PeerStore
	KV      bckv.KV
	Indexer *indexing.LocalIndexer
}

type BlobRouter struct {
	peerSwarm *PeerSwarm
	store     *BlobLocStore
	crawler   *Crawler
	cf        context.CancelFunc
}

func NewBlobRouter(params BlobRouterParams) *BlobRouter {
	peerSwarm := NewPeerSwarm(params.Swarm, params.PeerStore)
	localID := peerSwarm.LocalID()
	store := newBlobLocStore(params.KV, localID[:])

	ctx, cf := context.WithCancel(context.Background())
	crawler := newCrawler(params.PeerRouter, peerSwarm, store)
	go crawler.run(ctx)

	br := &BlobRouter{
		peerSwarm: peerSwarm,
		store:     store,
		crawler:   crawler,
		cf:        cf,
	}
	peerSwarm.OnAsk(br.handleAsk)

	return br
}

func (br *BlobRouter) Close() error {
	br.cf()
	return nil
}

func (br *BlobRouter) WhoHas(id blobs.ID) []p2p.PeerID {
	bloc := br.store.Get(id)
	peerID := p2p.PeerID{}
	copy(peerID[:], bloc.PeerId)
	return []p2p.PeerID{peerID}
}

func (br *BlobRouter) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	req := &ListBlobsReq{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		log.Error(err)
		return
	}

	br.Close()
}
