package blobrouting

import (
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/bcproto"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peerrouting"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	proto "github.com/golang/protobuf/proto"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

type (
	ListBlobsReq = bcproto.ListBlobsReq
	ListBlobsRes = bcproto.ListBlobsRes
)

type PeerSwarm interface {
	AskPeer(ctx context.Context, id p2p.PeerID, data []byte) ([]byte, error)
	OnAsk(p2p.AskHandler)
	LocalID() p2p.PeerID
}

type RouterParams struct {
	PeerSwarm
	PeerRouter *peerrouting.Router
	KV         bckv.KV
	LocalBlobs Indexable
	Clock      clockwork.Clock
}

type Router struct {
	peerSwarm  PeerSwarm
	peerRouter *peerrouting.Router

	store   *BlobLocStore
	crawler *Crawler
	cf      context.CancelFunc
}

func NewRouter(params RouterParams) *Router {
	peerSwarm := params.PeerSwarm
	localID := peerSwarm.LocalID()
	store := newBlobLocStore(params.KV, localID[:])

	ctx, cf := context.WithCancel(context.Background())

	br := &Router{
		peerRouter: params.PeerRouter,
		peerSwarm:  peerSwarm,
		store:      store,
		cf:         cf,
	}
	peerSwarm.OnAsk(br.handleAsk)
	go br.run(ctx)

	return br
}

func (br *Router) run(ctx context.Context) {
	crawler := newCrawler(br.peerRouter, br.peerSwarm, br.store)
	crawler.run(ctx)
}

func (br *Router) Close() error {
	br.cf()
	return nil
}

func (br *Router) WhoHas(id blobs.ID) []p2p.PeerID {
	bloc := br.store.Get(id)
	peerID := p2p.PeerID{}
	copy(peerID[:], bloc.PeerId)
	return []p2p.PeerID{peerID}
}

func (br *Router) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	req := &ListBlobsReq{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		log.Error(err)
		return
	}
	res, err := br.handleRequest(ctx, req)
	if err != nil {
		log.Error(err)
		return
	}
	resData, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}
	w.Write(resData)
}

func (br *Router) handleRequest(ctx context.Context, req *ListBlobsReq) (*ListBlobsRes, error) {
	return nil, errors.New("not implemented")
}
