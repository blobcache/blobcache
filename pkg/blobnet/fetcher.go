package blobnet

import (
	"context"
	"errors"
	"io"

	"github.com/blobcache/blobcache/pkg/blobnet/bcproto"
	"github.com/blobcache/blobcache/pkg/blobnet/blobrouting"
	"github.com/blobcache/blobcache/pkg/blobnet/peerrouting"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type (
	GetReq          = bcproto.GetReq
	GetRes          = bcproto.GetRes
	GetRes_Data     = bcproto.GetRes_Data
	GetRes_Redirect = bcproto.GetRes_Redirect
)

type FetcherParams struct {
	PeerRouter *peerrouting.Router
	BlobRouter *blobrouting.Router
	PeerSwarm  *peers.PeerSwarm
	Local      blobs.Getter
}

type Fetcher struct {
	peerRouter *peerrouting.Router
	blobRouter *blobrouting.Router
	peerSwarm  *peers.PeerSwarm
	local      blobs.Getter
}

func NewFetcher(params FetcherParams) *Fetcher {
	f := &Fetcher{
		peerRouter: params.PeerRouter,
		blobRouter: params.BlobRouter,
		peerSwarm:  params.PeerSwarm,
		local:      params.Local,
	}
	params.PeerSwarm.OnAsk(f.handleAsk)

	return f
}

func (f *Fetcher) GetF(ctx context.Context, id blobs.ID, fn func([]byte) error) error {
	data, err := f.get(ctx, id, nil, 3)
	if err != nil {
		return err
	}
	return fn(data)
}

func (f *Fetcher) get(ctx context.Context, id blobs.ID, redirect *GetReq, n int) ([]byte, error) {
	var (
		req = &GetReq{
			BlobId: id[:],
		}
		nextHop p2p.PeerID
	)

	if redirect != nil {
		rt2, nh := f.peerRouter.ForwardWhere(req.RoutingTag)
		req = redirect
		req.RoutingTag = rt2
		nextHop = nh
	} else {
		entries := f.blobRouter.Lookup(ctx, id)
		if len(entries) > 0 {
			req.RoutingTag, nextHop = f.peerRouter.Lookup(entries[0].PeerID)
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
	resData, err := f.peerSwarm.AskPeer(ctx, nextHop, reqData)
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
	var data []byte
	err := f.local.GetF(ctx, id, func(data2 []byte) error {
		data = append([]byte{}, data2...)
		return nil
	})
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
