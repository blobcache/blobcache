package blobrouting

import (
	"context"
	"time"

	"github.com/blobcache/blobcache/pkg/blobnet/peerrouting"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

type ShardID struct {
	PeerID p2p.PeerID
	Prefix string
}

type CrawlerParams struct {
	PeerRouter *peerrouting.Router
	BlobRouter *Router
	PeerSwarm  PeerSwarm
	Clock      clockwork.Clock
}

type Crawler struct {
	peerRouter *peerrouting.Router
	blobRouter *Router
	peerSwarm  PeerSwarm
	clock      clockwork.Clock
}

func newCrawler(params CrawlerParams) *Crawler {
	return &Crawler{
		peerRouter: params.PeerRouter,
		blobRouter: params.BlobRouter,
		peerSwarm:  params.PeerSwarm,
		clock:      params.Clock,
	}
}

func (c *Crawler) run(ctx context.Context) {
	ticker := c.clock.NewTicker(time.Minute)
	defer ticker.Stop()
	log.Info("starting blob crawler")
	defer func() { log.Info("stopped blob crawler") }()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			if err := c.crawl(ctx); err != nil {
				log.Error(err)
			}
		}
	}
}

func (c *Crawler) crawl(ctx context.Context) error {
	log.Info("begin crawling")
	defer func() { log.Info("done crawling") }()

	peerIDs := []p2p.PeerID{}
	peerIDs = append(peerIDs, c.peerRouter.OneHop()...)
	peerIDs = append(peerIDs, c.peerRouter.MultiHop()...)

	for _, peerID := range peerIDs {
		bitstr := c.blobRouter.WouldAccept()
		for _, prefix := range bitstr.EnumBytePrefixes() {
			err := c.indexPeer(ctx, peerID, prefix)
			if err == ErrShouldEvictThis {
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Crawler) indexPeer(ctx context.Context, peerID p2p.PeerID, prefix []byte) error {
	log.WithFields(log.Fields{
		"peer_id": peerID,
		"prefix":  prefix,
	}).Debug("indexing peer")
	rt, nextHop := c.peerRouter.Lookup(peerID)
	if rt == nil {
		return peerrouting.ErrNoRouteToPeer
	}
	req := &ListBlobsReq{
		RoutingTag: rt,
		Prefix:     prefix,
	}
	res, err := c.blobRouter.request(ctx, nextHop, req)
	if err != nil {
		return err
	}
	// too many, request sub prefixes
	if res.TooMany {
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		return nil
	}
	// child
	now := c.clock.Now()
	for _, blobLoc := range res.BlobLocs {
		blobID := blobs.ID{}
		copy(blobID[:], blobLoc.BlobId)
		peerID := p2p.PeerID{}
		copy(peerID[:], blobLoc.PeerId)
		sightedAt := time.Unix(int64(blobLoc.SightedAt), 0)
		if sightedAt.After(now) {
			log.Error("got route entry with time in future")
			continue
		}
		if err := c.blobRouter.Put(ctx, blobID, peerID, sightedAt); err != nil {
			return err
		}
	}
	return nil
}

func splitKey(x []byte) (blobs.ID, p2p.PeerID) {
	l := len(x)
	blobID := blobs.ID{}
	peerID := p2p.PeerID{}
	copy(blobID[:], x[l/2:])
	copy(peerID[:], x[:l/2])
	return blobID, peerID
}

func makeKey(blobID blobs.ID, peerID p2p.PeerID) []byte {
	return append(blobID[:], peerID[:]...)
}
