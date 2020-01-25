package blobnet

import (
	"context"
	"errors"
	fmt "fmt"
	"io"
	"log"

	"github.com/boltdb/bolt"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/aggswarm"
	"github.com/brendoncarroll/go-p2p/simplemux"
	proto "github.com/golang/protobuf/proto"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

type Blobnet struct {
	mux        simplemux.Muxer
	fetchSwarm p2p.AskSwarm

	router  *Router
	indexer *Indexer

	cache blobs.Getter
}

func NewBlobNet(mdb *bolt.DB, cache blobs.Getter, swarm p2p.Swarm) *Blobnet {
	mux := simplemux.MultiplexSwarm(swarm)
	bn := &Blobnet{
		cache: cache,
		mux:   mux,
	}

	rSwarm, err := bn.mux.OpenChannel("blobcache/routing-v0")
	if err != nil {
		panic(err)
	}
	bn.router = NewRouter(rSwarm.(p2p.AskSwarm))

	iSwarm, err := bn.mux.OpenChannel("blobcache/indexing-v0")
	if err != nil {
		panic(err)
	}
	bn.indexer, err = NewIndexer(mdb, iSwarm.(p2p.AskSwarm), bn.router)
	if err != nil {
		panic(err)
	}

	fSwarm, err := bn.mux.OpenChannel("blobcache/fetching-v0")
	if err != nil {
		panic(err)
	}
	fSwarm.(p2p.Asker).OnAsk(bn.handleAsk)
	bn.fetchSwarm = fSwarm.(p2p.AskSwarm)
	return bn
}

func (bn *Blobnet) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	peerID, path := bn.nextPeer(ctx, id)
	req := &GetReq{
		RoutingTag: &RoutingTag{
			DstId: peerID[:],
			Path:  path,
		},
		BlobId: id[:],
	}
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	addr := &aggswarm.Edge{
		PeerID: peerID,
	}
	data, err := bn.fetchSwarm.Ask(ctx, addr, reqData)
	if err != nil {
		return nil, err
	}
	if len(data) < 1 {
		return nil, nil
	}

	res := &GetRes{}
	if err := proto.Unmarshal(data, res); err != nil {
		return nil, err
	}
	switch x := res.Res.(type) {
	case *GetRes_Data:
		id2 := blobs.Hash(x.Data)
		if !id2.Equals(id) {
			return nil, fmt.Errorf("bad blob from peer %v", peerID)
		}
		return x.Data, nil
	case *GetRes_Redirect:
		return nil, errors.New("redirect not supported")
	default:
		return nil, nil
	}
}

func (bn *Blobnet) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	return blobs.ID{}, nil
}

func (bn *Blobnet) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	req := &GetReq{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		log.Println(err)
		return
	}
	id := blobs.ID{}
	copy(id[:], req.BlobId)

	// try local
	data, err := bn.getLocal(ctx, id)
	if err != nil {
		log.Println(err)
	}
	if data != nil {
		res := &GetRes{
			BlobId: req.BlobId,
			Res:    &GetRes_Data{data},
		}
		data, err := proto.Marshal(res)
		if err != nil {
			panic(err)
		}
		w.Write(data)
		return
	}

	// forward

	// empty
	res := &GetRes{
		BlobId: req.BlobId,
	}
	resBytes, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}
	w.Write(resBytes)
}

func (bn *Blobnet) getLocal(ctx context.Context, id blobs.ID) ([]byte, error) {
	return bn.cache.Get(ctx, id)
}

func (bn *Blobnet) nextPeer(ctx context.Context, id blobs.ID) (p2p.PeerID, []uint64) {
	panic("not implemented")
}
