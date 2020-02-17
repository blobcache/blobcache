package blobnet

import (
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type FetcherParams struct {
	PeerRouter *Router
	BlobRouter *BlobRouter
	Swarm      p2p.SecureAskSwarm
	PeerStore
	Local blobs.Getter
}

type Fetcher struct {
	peerRouter *Router
	blobRouter *BlobRouter

	peerSwarm *PeerSwarm
	local     blobs.Getter
}

func NewFetcher(params FetcherParams) *Fetcher {
	peerSwarm := NewPeerSwarm(params.Swarm, params.PeerStore)
	f := &Fetcher{
		peerRouter: params.PeerRouter,
		blobRouter: params.BlobRouter,
		peerSwarm:  peerSwarm,
		local:      params.Local,
	}
	peerSwarm.OnAsk(f.handleAsk)

	return f
}

func (f *Fetcher) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	return f.get(ctx, id, nil, 3)
}

func (f *Fetcher) get(ctx context.Context, id blobs.ID, redirect *GetReq, n int) ([]byte, error) {
	var (
		req = &GetReq{
			BlobId: id[:],
		}
		nextHop p2p.PeerID
	)

	if redirect != nil {
		nextHop = f.peerRouter.lm.Peer(int(req.RoutingTag.Path[0]))
		req = redirect
	} else {
		peers := f.blobRouter.WhoHas(id)
		if len(peers) > 0 {
			req.RoutingTag, nextHop = f.peerRouter.Lookup(peers[0])
			req.Found = true
		} else {
			id := f.peerRouter.Closest(id[:])
			req.RoutingTag, nextHop = f.peerRouter.Lookup(id)
			req.Found = false
		}

		if nextHop.Equals(f.peerSwarm.LocalID()) {
			return nil, errors.New("no peer closer to target")
		}
	}

	res, err := f.getReq(ctx, nextHop, req)
	if err != nil {
		return nil, err
	}

	switch x := res.Res.(type) {
	case *GetRes_Data:
		actualID := blobs.Hash(x.Data)
		if !id.Equals(actualID) {
			return nil, errors.New("got bad blob from peer")
		}
		return x.Data, nil

	case *GetRes_Redirect:
		r := x.Redirect
		req2 := &GetReq{
			RoutingTag: r.RoutingTag,
			Found:      r.Found,
			BlobId:     id[:],
		}
		if n <= 0 {
			return nil, errors.New("out of redirects")
		}
		return f.get(ctx, id, req2, n-1)

	default:
		return nil, blobs.ErrNotFound
	}
}

func (f *Fetcher) getReq(ctx context.Context, nextHop p2p.PeerID, req *GetReq) (*GetRes, error) {
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	resData, err := f.peerSwarm.Ask(ctx, nextHop, reqData)
	if err != nil {
		return nil, err
	}
	res := &GetRes{}
	if err := proto.Unmarshal(resData, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (f *Fetcher) handleAsk(ctx context.Context, m *p2p.Message, w io.Writer) {
	req := &GetReq{}
	if err := proto.Unmarshal(m.Payload, req); err != nil {
		log.Error(err)
		return
	}
	res, err := f.handleGetReq(ctx, req)
	if err != nil {
		log.Error(err)
		return
	}
	data, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}
	w.Write(data)
}

func (f *Fetcher) handleGetReq(ctx context.Context, req *GetReq) (*GetRes, error) {
	// try local
	id := blobs.ID{}
	copy(id[:], req.BlobId)
	res, err := f.tryLocal(ctx, id)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}

	// TODO: use the routers, and support redirects

	// not found
	return &GetRes{BlobId: req.BlobId}, nil
}

func (f *Fetcher) tryLocal(ctx context.Context, id blobs.ID) (*GetRes, error) {
	data, err := f.local.Get(ctx, id)
	if err == nil {
		return &GetRes{
			BlobId: id[:],
			Res:    &GetRes_Data{data},
		}, nil
	}
	if err == blobs.ErrNotFound {
		return nil, nil
	}
	return nil, err
}
