package blobrouting

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/bcstate"
	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
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
	DB         bcstate.DB
	LocalBlobs Indexable
	Clock      clockwork.Clock
}

type Router struct {
	peerSwarm      PeerSwarm
	peerRouter     *peerrouting.Router
	minQueryLength int
	clock          clockwork.Clock

	localRT    *LocalRT
	routeTable *KadRT
	crawler    *Crawler
	cf         context.CancelFunc

	mu     sync.RWMutex
	shards map[string]tries.Trie
}

func NewRouter(params RouterParams) *Router {
	peerSwarm := params.PeerSwarm
	localID := peerSwarm.LocalID()

	ctx, cf := context.WithCancel(context.Background())

	rtRootCell := bcstate.KVCell{KV: params.DB.Bucket("route_table/root")}
	rtStorage := params.DB.Bucket("route_table/storage")
	br := &Router{
		peerRouter:     params.PeerRouter,
		peerSwarm:      peerSwarm,
		minQueryLength: 1,
		clock:          params.Clock,

		localRT:    NewLocalRT(params.LocalBlobs, localID),
		routeTable: NewKadRT(rtRootCell, rtStorage, localID[:]),
		cf:         cf,
		shards:     make(map[string]tries.Trie),
	}
	peerSwarm.OnAsk(br.handleAsk)
	go br.run(ctx)

	return br
}

func (r *Router) run(ctx context.Context) {
	crawler := newCrawler(CrawlerParams{
		PeerRouter: r.peerRouter,
		BlobRouter: r,
		PeerSwarm:  r.peerSwarm,
		Clock:      r.clock,
	})
	crawler.run(ctx)
}

func (br *Router) Close() error {
	br.cf()
	return nil
}

func (r *Router) Put(ctx context.Context, blobID blobs.ID, peerID p2p.PeerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// evict from cache
	r.delete(blobID)

	if peerID.Equals(r.peerSwarm.LocalID()) {
		return nil
	}
	return r.routeTable.Put(ctx, blobID, peerID)
}

func (r *Router) Query(ctx context.Context, prefix []byte) (tries.Trie, error) {
	ctx = tries.CtxDeleteBlobs(ctx)

	// check the cache
	r.mu.RLock()
	trie, exists := r.shards[string(prefix)]
	r.mu.RUnlock()
	if exists {
		return trie, nil
	}

	// construct trie, and fill cache.
	lTrie, err := r.localRT.Query(ctx, prefix)
	if err != nil {
		return nil, err
	}
	rTrie, err := r.routeTable.Query(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var m tries.Trie
	if lTrie.IsParent() || rTrie.IsParent() {
		children := [256]tries.Trie{}
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			t, err := r.Query(ctx, prefix2)
			if err != nil {
				return nil, err
			}
			children[i] = t
		}
		m, err = tries.NewParent(ctx, blobs.Void{}, children)
		if err != nil {
			return nil, err
		}
	} else {
		m, err = tries.Merge(ctx, blobs.Void{}, lTrie, rTrie)
		if err != nil {
			return nil, err
		}
	}

	if m.IsParent() {
		r.mu.Lock()
		r.shards[string(prefix)] = m
		r.mu.Unlock()
	}

	return m, nil
}

func (r *Router) Lookup(ctx context.Context, blobID blobs.ID) []p2p.PeerID {
	localRes, err := r.routeTable.Lookup(ctx, blobID)
	if err != nil {
		return nil
	}
	remoteRes, err := r.routeTable.Lookup(ctx, blobID)
	if err != nil {
		return nil
	}
	return append(localRes, remoteRes...)
}

func (r *Router) WouldAccept() bitstrings.BitString {
	return r.routeTable.WouldAccept()
}

func (r *Router) Invalidate(ctx context.Context, id blobs.ID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delete(id)
	return nil
}

func (r *Router) delete(blobID blobs.ID) {
	// get a lock before calling this
	for i := range blobID {
		if _, exists := r.shards[string(blobID[:i])]; exists {
			delete(r.shards, string(blobID[:i]))
		} else {
			break
		}
	}
}

func (r *Router) request(ctx context.Context, nextHop p2p.PeerID, req *ListBlobsReq) (*ListBlobsRes, error) {
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	resData, err := r.peerSwarm.AskPeer(ctx, nextHop, reqData)
	if err != nil {
		return nil, err
	}
	res := &ListBlobsRes{}
	if err := proto.Unmarshal(resData, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (r *Router) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	req := &ListBlobsReq{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		log.Error(err)
		return
	}
	res, err := r.handleRequest(ctx, req)
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

func (r *Router) handleRequest(ctx context.Context, req *ListBlobsReq) (*ListBlobsRes, error) {
	rt := req.GetRoutingTag()
	localID := r.peerSwarm.LocalID()
	if rt == nil || bytes.HasPrefix(localID[:], rt.DstId) {
		return r.localRequest(ctx, req)
	}
	return r.forwardRequest(ctx, req)
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
	if len(req.Prefix) < br.minQueryLength {
		return &ListBlobsRes{}, nil
	}
	trie, err := br.Query(ctx, req.Prefix)
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
