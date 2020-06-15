package peerrouting

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/blobcache/blobcache/pkg/blobnet/bcproto"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
	proto "github.com/golang/protobuf/proto"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

type (
	RoutingTag   = bcproto.RoutingTag
	PeerInfo     = bcproto.PeerInfo
	ListPeersReq = bcproto.ListPeersReq
	ListPeersRes = bcproto.ListPeersRes

	Path = []uint64
)

var (
	ErrNoRouteToPeer = errors.New("no route to peer")
)

type PeerSwarm interface {
	AskPeer(ctx context.Context, id p2p.PeerID, data []byte) ([]byte, error)
	OnAsk(p2p.AskHandler)

	LocalID() p2p.PeerID
	ListPeers() []p2p.PeerID

	Close() error
}

type RouterParams struct {
	PeerSwarm
	QueryPeriod time.Duration
	CacheSize   int
	Clock       clockwork.Clock
}

type Router struct {
	peerSwarm   PeerSwarm
	clock       clockwork.Clock
	queryPeriod time.Duration

	lm *LinkMap
	cf context.CancelFunc

	mu    sync.RWMutex
	cache *kademlia.Cache
}

func NewRouter(params RouterParams) *Router {
	peerSwarm := params.PeerSwarm
	queryPeriod := params.QueryPeriod
	if queryPeriod == 0 {
		queryPeriod = time.Minute
	}
	cacheSize := params.CacheSize
	if cacheSize == 0 {
		cacheSize = 128
	}

	lm := NewLinkMap()
	lm.Int(peerSwarm.LocalID())

	ctx, cf := context.WithCancel(context.Background())

	localID := peerSwarm.LocalID()
	r := &Router{
		peerSwarm:   peerSwarm,
		queryPeriod: queryPeriod,
		clock:       params.Clock,

		cf: cf,
		lm: lm,

		cache: kademlia.NewCache(localID[:], cacheSize, 1),
	}

	peerSwarm.OnAsk(r.handleAsk)

	go r.run(ctx)
	return r
}

func (r *Router) Close() error {
	r.cf()
	return r.peerSwarm.Close()
}

// Lookup returns a routing tag, and an address for the next hop peer
func (r *Router) Lookup(peerID p2p.PeerID) (*RoutingTag, p2p.PeerID) {
	path := r.PathTo(peerID)
	if path == nil {
		return nil, p2p.ZeroPeerID()
	}
	rt := &RoutingTag{
		DstId: peerID[:],
		Path:  path,
	}

	nextHopPeer := r.lm.Peer(int(rt.Path[0]))
	return rt, nextHopPeer
}

func (r *Router) PathTo(id p2p.PeerID) Path {
	r.mu.RLock()
	defer r.mu.RUnlock()

	x := r.cache.Lookup(id[:])
	if x == nil {
		// check if it's onehop
		oneHop := r.OneHop()
		for _, id2 := range oneHop {
			if id.Equals(id2) {
				return Path{uint64(r.lm.Int(id))}
			}
		}
		return nil
	}
	return append(Path{}, x.(Path)...)
}

// ForwardWhere looks up which peer rt.Path[0] corresponds to and returns
// their id, and a modified routing tag for the next peer.
// if routing tag is nil, the caller should abandon any attempt to forward the message.
func (r *Router) ForwardWhere(rt *RoutingTag) (*RoutingTag, p2p.PeerID) {
	if rt == nil {
		return nil, p2p.ZeroPeerID()
	}

	// see if we know a shorter route
	dstID := p2p.PeerID{}
	copy(dstID[:], rt.DstId)
	rt2, nextHop := r.Lookup(dstID)
	if rt2 != nil && len(rt2.Path) < len(rt.Path) {
		return rt2, nextHop
	}

	// lookup the peer at index
	if len(rt.Path) == 0 {
		return nil, p2p.ZeroPeerID()
	}
	id := r.lm.Peer(int(rt.Path[0]))
	if id == p2p.ZeroPeerID() {
		return nil, id
	}
	rt2 = &RoutingTag{
		DstId: rt.DstId,
		Path:  rt.Path[1:],
	}
	return rt2, id
}

func (r *Router) Closest(key []byte) p2p.PeerID {
	r.mu.RLock()
	e := r.cache.Closest(key)
	r.mu.RUnlock()

	closest := r.peerSwarm.LocalID()
	if e != nil {
		copy(closest[:], e.Key)
	}

	dist := kademlia.XORBytes(key, closest[:])
	for _, id := range r.MultiHop() {
		dist2 := kademlia.XORBytes(key, id[:])
		if bytes.Compare(dist2, dist) < 0 {
			closest = id
			dist = dist2
		}
	}

	return closest
}

func (r *Router) OneHop() []p2p.PeerID {
	peerIDs := r.peerSwarm.ListPeers()
	for _, peerID := range peerIDs {
		r.lm.Int(peerID)
	}
	return peerIDs
}

func (r *Router) MultiHop() []p2p.PeerID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peerIDs := []p2p.PeerID{}
	r.cache.ForEach(func(e kademlia.Entry) bool {
		id := p2p.PeerID{}
		copy(id[:], e.Key)
		peerIDs = append(peerIDs, id)
		return true
	})
	return peerIDs
}

func (r *Router) GetPeerInfos() []*PeerInfo {
	peerInfos := []*PeerInfo{}
	for _, peerID := range r.OneHop() {
		pinfo := &PeerInfo{
			Id:   peerID[:],
			Path: Path{uint64(r.lm.Int(peerID))},
		}
		peerInfos = append(peerInfos, pinfo)
	}

	r.mu.RLock()
	r.cache.ForEach(func(e kademlia.Entry) bool {
		path := []uint64{}
		for _, index := range e.Value.(Path) {
			path = append(path, uint64(index))
		}
		pinfo := &PeerInfo{
			Id:   e.Key,
			Path: path,
		}
		peerInfos = append(peerInfos, pinfo)
		return true
	})
	r.mu.RUnlock()
	return peerInfos
}

func (r *Router) Bootstrap(ctx context.Context) {
	const max = 10
	lastCount := r.cache.Count()
	for i := 0; i < max; i++ {
		r.queryPeers(ctx)
		count := r.cache.Count()
		if r.cache.IsFull() || count <= lastCount {
			break
		}
		lastCount = count
	}
}

func (r *Router) run(ctx context.Context) {
	ticker := r.clock.NewTicker(r.queryPeriod)
	defer ticker.Stop()
	log.Info("starting peer router")
	defer func() { log.Info("stopped peer router") }()

	for {
		select {
		case <-ticker.Chan():
			ctx, cf := context.WithTimeout(ctx, r.queryPeriod/2)
			r.queryPeers(ctx)
			cf()
		case <-ctx.Done():
			return
		}
	}
}

func (r *Router) queryPeers(ctx context.Context) {
	log.Debug("begin querying peers")
	peerIDs := []p2p.PeerID{}
	peerIDs = append(peerIDs, r.OneHop()...)
	peerIDs = append(peerIDs, r.MultiHop()...)

	wg := sync.WaitGroup{}
	wg.Add(len(peerIDs))
	for _, peerID := range peerIDs {
		peerID := peerID
		go func() {
			if err := r.queryPeer(ctx, peerID); err != nil {
				log.Error(err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	log.Debug("done querying peers")
}

func (r *Router) queryPeer(ctx context.Context, peerID p2p.PeerID) error {
	rt, nextHopPeer := r.Lookup(peerID)
	if rt == nil {
		return ErrNoRouteToPeer
	}

	// errors in this section mean the peer should be deleted
	res := &ListPeersRes{}
	err := func() error {
		req := &ListPeersReq{
			RoutingTag: rt,
		}
		reqData, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		resData, err := r.peerSwarm.AskPeer(ctx, nextHopPeer, reqData)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(resData, res); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		log.Error(err)
		r.deletePeer(peerID)
		return err
	}

	for _, peerInfo := range res.PeerInfos {
		path := r.PathTo(peerID)
		path = append(path, peerInfo.Path...)

		id := p2p.PeerID{}
		copy(id[:], peerInfo.Id)

		r.putPeer(id, path)
	}

	return nil
}

func (r *Router) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	req := &ListPeersReq{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		log.Error(err)
		return
	}
	var (
		rt      = req.RoutingTag
		localID = r.peerSwarm.LocalID()
		res     *ListPeersRes
		err     error
	)
	switch {
	// treat all these as asking for our peers
	case rt == nil:
		fallthrough
	case bytes.Compare(rt.DstId, localID[:]) == 0:
		fallthrough
	case len(rt.Path) < 1:
		log.WithFields(log.Fields{
			"peer_id": msg.Src,
		}).Debug("giving local peer info")
		res = r.localAsk(ctx, req)

	// a valid forwarding case
	default:
		res, err = r.forwardAsk(ctx, req)
	}

	if err != nil {
		log.Error(err)
		return
	}
	resData, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}
	if _, err := w.Write(resData); err != nil {
		log.Error(err)
	}
}

func (r *Router) forwardAsk(ctx context.Context, req *ListPeersReq) (*ListPeersRes, error) {
	rt := req.RoutingTag
	peerID := r.lm.Peer(int(rt.Path[0]))
	dstID := p2p.PeerID{}
	copy(dstID[:], rt.DstId)
	log.WithFields(log.Fields{
		"path":     rt.Path,
		"next_hop": peerID,
		"dst_id":   dstID,
	}).Debug("forwarding ListPeersReq")

	req2 := &ListPeersReq{
		RoutingTag: &RoutingTag{
			DstId: rt.DstId,
			Path:  rt.Path[1:],
		},
	}
	req2Data, err := proto.Marshal(req2)
	if err != nil {
		panic(err)
	}

	resData, err := r.peerSwarm.AskPeer(ctx, peerID, req2Data)
	if err != nil {
		return nil, err
	}
	res := &ListPeersRes{}
	if err := proto.Unmarshal(resData, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (r *Router) localAsk(ctx context.Context, req *ListPeersReq) *ListPeersRes {
	localID := r.peerSwarm.LocalID()
	res := &ListPeersRes{
		PeerId:    localID[:],
		PeerInfos: r.GetPeerInfos(),
	}
	return res
}

func (r *Router) putPeer(id p2p.PeerID, p Path) {
	// prevent ourselves from entering the cache
	localID := r.peerSwarm.LocalID()
	if id.Equals(localID) {
		return
	}
	// prevent one hop peers from entering the cache
	for _, id2 := range r.OneHop() {
		if id.Equals(id2) {
			return
		}
	}
	log := log.WithFields(log.Fields{
		"peer_id": id,
		"path":    p,
	})

	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.cache.Lookup(id[:])
	if v != nil {
		currentPath := v.(Path)
		if len(p) < len(currentPath) {
			r.cache.Put(id[:], p)
		}
		log.Info("found shorter path for peer")
	} else {
		log.Info("found new peer")
		r.cache.Put(id[:], p)
	}
}

func (r *Router) deletePeer(id p2p.PeerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.WithFields(log.Fields{
		"peer_id": id,
	}).Debug("deleting peer")

	r.cache.Delete(id[:])
}
