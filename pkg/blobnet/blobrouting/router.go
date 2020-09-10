package blobrouting

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/bitstrings"
	"github.com/blobcache/blobcache/pkg/blobnet/bcproto"
	"github.com/blobcache/blobcache/pkg/blobnet/peerrouting"
	"github.com/blobcache/blobcache/pkg/blobs"
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

	localRT *LocalRT
	kadRT   *KadRT
	crawler *Crawler
	cf      context.CancelFunc
}

func NewRouter(params RouterParams) *Router {
	peerSwarm := params.PeerSwarm
	localID := peerSwarm.LocalID()

	ctx, cf := context.WithCancel(context.Background())

	rtStorage := params.DB.Bucket("route_table")
	br := &Router{
		peerRouter:     params.PeerRouter,
		peerSwarm:      peerSwarm,
		minQueryLength: 1,
		clock:          params.Clock,

		localRT: NewLocalRT(params.LocalBlobs, localID, params.Clock),
		kadRT:   NewKadRT(rtStorage, localID[:]),
		cf:      cf,
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

func (r *Router) Put(ctx context.Context, blobID blobs.ID, peerID p2p.PeerID, sightedAt time.Time) error {
	if peerID.Equals(r.peerSwarm.LocalID()) {
		return nil
	}
	return r.kadRT.Put(ctx, blobID, peerID, sightedAt)
}

func (r *Router) List(ctx context.Context, prefix []byte, entries []RTEntry) (int, error) {
	total := 0
	n, err := r.localRT.List(ctx, prefix, entries)
	if err != nil {
		return -1, err
	}
	total += n
	n, err = r.kadRT.List(ctx, prefix, entries[total:])
	if err != nil {
		return -1, err
	}
	total += n
	return total, nil
}

func (r *Router) Lookup(ctx context.Context, blobID blobs.ID) []RTEntry {
	localRes, err := r.localRT.Lookup(ctx, blobID)
	if err != nil {
		return nil
	}
	remoteRes, err := r.kadRT.Lookup(ctx, blobID)
	if err != nil {
		return nil
	}
	return append(localRes, remoteRes...)
}

func (r *Router) WouldAccept() bitstrings.BitString {
	return r.kadRT.WouldAccept()
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
	entries := make([]RTEntry, 1024)
	n, err := br.List(ctx, req.Prefix, entries)
	if err != nil {
		if err == blobs.ErrTooMany {
			return &bcproto.ListBlobsRes{TooMany: true}, nil
		}
		return nil, err
	}
	entries = entries[:n]
	blobLocs := make([]*bcproto.BlobLoc, len(entries))
	for i, ent := range entries {
		blobLocs[i] = &bcproto.BlobLoc{
			BlobId:    ent.BlobID[:],
			PeerId:    ent.PeerID[:],
			SightedAt: uint64(ent.SightedAt.Unix()),
		}
	}
	return &bcproto.ListBlobsRes{
		BlobLocs: blobLocs,
	}, nil
}
