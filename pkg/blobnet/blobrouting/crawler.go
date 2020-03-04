package blobrouting

import (
	"context"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobnet/peerrouting"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
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

	shards map[ShardID]blobs.ID
}

func newCrawler(params CrawlerParams) *Crawler {
	return &Crawler{
		peerRouter: params.PeerRouter,
		blobRouter: params.BlobRouter,
		peerSwarm:  params.PeerSwarm,
		clock:      params.Clock,
		shards:     map[ShardID]blobs.ID{},
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
	shardID := ShardID{peerID, string(prefix)}
	rt, nextHop := c.peerRouter.Lookup(peerID)
	if rt == nil {
		delete(c.shards, shardID)
		return peerrouting.ErrNoRouteToPeer
	}

	req := &ListBlobsReq{
		RoutingTag: rt,
		Prefix:     prefix,
	}
	res, err := c.blobRouter.request(ctx, nextHop, req)
	if err != nil {
		delete(c.shards, ShardID{peerID, string(prefix)})
		return err
	}

	// Sharded below this point
	if len(res.TrieHash) < 1 || len(res.TrieData) < 1 {
		delete(c.shards, shardID)
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		return nil
	}

	// parse trie; can't resolve any of the children without a store, but we don't need to here.
	t, err := tries.FromBytes(nil, res.TrieData)
	if err != nil {
		return err
	}
	id := blobs.Hash(res.TrieData)

	// parent, need to recurse
	if t.IsParent() {
		for i := 0; i < 256; i++ {
			id := t.GetChildRef(byte(i))
			prefix2 := append(prefix, byte(i))
			shardID2 := ShardID{peerID, string(prefix2)}
			if id2, exists := c.shards[shardID2]; exists && id.Equals(id2) {
				continue
			}
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		c.putShard(shardID, id)
		return nil
	}

	// child
	for _, pair := range t.ListEntries() {
		blobID, peerID := splitKey(pair.Key)
		c.blobRouter.Put(ctx, blobID, peerID)
	}
	c.putShard(shardID, id)
	return nil
}

func (c *Crawler) putShard(shardID ShardID, id blobs.ID) {
	log.WithFields(log.Fields{
		"peer_id": shardID.PeerID,
		"prefix":  shardID.Prefix,
		"blob_id": id,
	}).Debug("synced shard")
	c.shards[shardID] = id
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
