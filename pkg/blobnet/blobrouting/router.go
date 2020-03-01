package blobrouting

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/bcproto"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peerrouting"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
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

	routeTable *KadRT
	crawler    *Crawler
	cf         context.CancelFunc
}

func NewRouter(params RouterParams) *Router {
	peerSwarm := params.PeerSwarm
	localID := peerSwarm.LocalID()

	ctx, cf := context.WithCancel(context.Background())

	br := &Router{
		peerRouter: params.PeerRouter,
		peerSwarm:  peerSwarm,
		routeTable: NewKadRT(blobs.NewMem(), localID[:]),
		cf:         cf,
	}
	peerSwarm.OnAsk(br.handleAsk)
	go br.run(ctx)

	return br
}

func (br *Router) run(ctx context.Context) {
	crawler := newCrawler(br.peerRouter, br, br.peerSwarm)
	crawler.run(ctx)
}

func (br *Router) Close() error {
	br.cf()
	return nil
}

func (br *Router) WhoHas(ctx context.Context, blobID blobs.ID) []p2p.PeerID {
	peerIDs, err := br.routeTable.Lookup(ctx, blobID)
	if err != nil {
		log.Error(err)
		return nil
	}
	return peerIDs
}

func (br *Router) Query(ctx context.Context, prefix []byte) (tries.Trie, error) {
	return br.routeTable.Query(ctx, prefix)
}

func (br *Router) request(ctx context.Context, nextHop p2p.PeerID, req *ListBlobsReq) (*ListBlobsRes, error) {
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	resData, err := br.peerSwarm.AskPeer(ctx, nextHop, reqData)
	if err != nil {
		return nil, err
	}
	res := &ListBlobsRes{}
	if err := proto.Unmarshal(resData, res); err != nil {
		return nil, err
	}
	return res, nil
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
	rt := req.GetRoutingTag()
	localID := br.peerSwarm.LocalID()
	if rt == nil || bytes.HasPrefix(localID[:], rt.DstId) {
		return br.localRequest(ctx, req)
	}
	return br.forwardRequest(ctx, req)
}

func (br *Router) forwardRequest(ctx context.Context, req *ListBlobsReq) (*ListBlobsRes, error) {
	rt2, nextHop := br.peerRouter.ForwardWhere(req.GetRoutingTag())
	if rt2 == nil {
		return nil, errors.New("error forwarding")
	}
	req2 := &ListBlobsReq{
		Prefix: req.Prefix,
	}
	return br.request(ctx, nextHop, req2)
}

func (br *Router) localRequest(ctx context.Context, req *ListBlobsReq) (*ListBlobsRes, error) {
	trie, err := br.routeTable.Query(ctx, req.Prefix)
	if err != nil {
		return nil, err
	}
	data := trie.Marshal()
	id := blobs.Hash(data)
	res := &ListBlobsRes{
		TrieData: data,
		TrieHash: id[:],
	}
	return res, nil
}
