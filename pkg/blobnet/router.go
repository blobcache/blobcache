package blobnet

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/aggswarm"
	"github.com/brendoncarroll/go-p2p/kademlia"
	proto "github.com/golang/protobuf/proto"
)

type Path []uint64

type Router struct {
	swarm p2p.AskSwarm

	mu    sync.RWMutex
	cache *kademlia.Cache
}

func NewRouter(swarm p2p.AskSwarm) *Router {
	locus := swarm.LocalAddr().(*aggswarm.Edge).PeerID[:]
	return &Router{
		swarm: swarm,
		cache: kademlia.NewCache(locus, 128, 1),
	}
}

func (r *Router) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	r.queryPeers(ctx)

	for {
		select {
		case <-ticker.C:
			r.queryPeers(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Router) queryPeers(ctx context.Context) {
	// get list of peers
	peers := append([]p2p.PeerID{}, r.swarm.OneHop())
	r.cache.ForEach(func(key []byte, v interface{}) bool {
		peerID := p2p.PeerID{}
		copy(peerID[:], e.Key)
		peers = append(peers, peerID)
		return true
	})

	wg := sync.WaitGroup{}
	wg.Add(len(peers))
	toDelete := make(chan p2p.PeerID)
	for i, peerID := range peers {
		i := i
		peerID := peerID
		go func() {
			if _, err := r.queryPeer(ctx, peerID); err != nil {
				log.Println("ERROR:", err)
				log.Println("removing peer", peerID)
				toDelete<-peerID
			}
			wg.Done()
		}()
	}

	select {
	case <-ctx.Done():
		log.Println(err)
	case peerID := <-toDelete:
		r.cache.Delete(peerID[:])
	}
	wg.Wait()
	close (toDelete)
}

func (r *Router) queryPeer(ctx context.Context, peerID p2p.PeerID) ([]p2p.PeerID, error) {
	basePath := r.Lookup(peerID)
	req := &ListPeersReq{
		DstId: peerID,
		Path:  basePath,
	}
	res, err := r.ListPeers(ctx, &req)
	if err != nil {
		return nil, err
	}

	peers := []p2p.PeerID{}
	for _, peerInfo := range res.PeerInfos {
		path := basePath
		for _, index := range peerInfo.Path {
			path = append(path, uint(index))
		}
		r.cache.Add(peerInfo.PeerID, path)
	}
	return nil
}

func (r *Router) ListPeers(ctx context.Context, peerID p2p.PeerID, req *ListPeersReq) (res *ListPeersRes, err error) {
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	r.NextHopTo()
	resData, err := r.swarm.Ask(ctx, addr, reqData)
	if err != nil {
		return nil, err
	}
	res = &ListPeersRes{}
	if err := proto.Unmarshal(resData, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (r *Router) handleAsk(ctx context.Context, reqMsg *p2p.Message) []byte {
	r.addPeer(reqMsg.SrcID)
	req := &ListPeersReq{}
	if err := proto.Unmarshal(reqMsg.Payload, req); err != nil {
		log.Println(err)
		return nil
	}

	peerInfos := []*PeerInfo{}
	for _, addr := range r.swarm.OneHop() {
		peerID := p2p.PeerID{}
		pinfo := &PeerInfo{}
		peerInfos = append(peerInfos, pinfo)
	}
	r.cache.ForEach(func(key []byte, v interface{}) bool {
		path := []uint64{}
		for _, index := range v.(Path) {
			path = append(path, uint64(index))
		}
		pinfo := &PeerInfo{
			Id:   key,
			Path: v.(Path),
		}
		peerInfos = append(peerInfos, pinfo)
	})

	res := &ListPeerRes{
		PeerID:    r.swarm.LocalID(),
		PeerInfos: peerInfos,
	}
	resData, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}
	return resData
}

func (r *Router) addPeer(id p2p.PeerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.edges[id]
	if !exists {
		r.peer2Index[id] = r.nextIndex
		r.index2Peer[r.nextIndex] = id
		r.nextIndex++
	}
}

func (r *Router) deletePeer(id p2p.PeerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index, exists := r.edges[id]
	if exists {
		delete(r.peer2Index, id)
		delete(r.index2Peer, index)
	}
}

func (r *Router) AtIndex(i uint64) p2p.PeerID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.index2Peer[i]
}

func (r *Router) ClosestTo(id p2p.PeerID) p2p.PeerID {
	return r.cache.ClosestTo(id)
}

func (r *Router) PathTo(id p2p.PeerID) Path {
	x := r.cache.Lookup(id)
	if ent == nil {
		return nil
	}
	return x.(Path)
}

func (r *Router) NextHopTo(id p2p.PeerID) (uint, error) {
	path := r.GetPathTo()
	if path == nil {
		return 0, errors.New("no path known")
	}
	return path[0], nil
}
