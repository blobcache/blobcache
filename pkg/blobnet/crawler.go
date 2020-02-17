package blobnet

import (
	"context"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type ShardID struct {
	PeerID p2p.PeerID
	Prefix string
}

type Crawler struct {
	peerRouter *Router
	store      *BlobLocStore
	peerSwarm  *PeerSwarm

	shards map[ShardID]blobs.ID
}

func newCrawler(peerRouter *Router, peerSwarm *PeerSwarm, store *BlobLocStore) *Crawler {
	return &Crawler{
		peerRouter: peerRouter,
		peerSwarm:  peerSwarm,
		store:      store,
		shards:     map[ShardID]blobs.ID{},
	}
}

func (c *Crawler) run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.crawl(ctx); err != nil {
				log.Error(err)
			}
		}
	}
}

func (c *Crawler) crawl(ctx context.Context) error {
	peerIDs := []p2p.PeerID{}
	peerIDs = append(peerIDs, c.peerRouter.OneHop()...)
	peerIDs = append(peerIDs, c.peerRouter.MultiHop()...)

	for _, peerID := range peerIDs {
		bitstr := c.store.WouldAccept()
		for _, prefix := range bitstr.EnumBytePrefixes() {
			x := bitstrings.FromBytes(len(prefix)*8, prefix)
			if !c.store.WouldAccept().HasPrefix(&x) {
				continue
			}
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
	shardID := ShardID{peerID, string(prefix)}
	rt, nextHop := c.peerRouter.Lookup(peerID)
	if rt == nil {
		delete(c.shards, shardID)
		return ErrNoRouteToPeer
	}

	req := &ListBlobsReq{
		RoutingTag: rt,
		Prefix:     prefix,
	}
	res, err := c.request(ctx, nextHop, req)
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

	// parse trie
	t, err := tries.FromBytes(nil, res.TrieData)
	if err != nil {
		return err
	}
	id := blobs.Hash(res.TrieData)

	// parent, need to recurse
	if t.Children != nil {
		for i, id := range t.Children {
			prefix2 := append(prefix, byte(i))
			shardID2 := ShardID{peerID, string(prefix2)}
			if id2, exists := c.shards[shardID2]; exists && id.Equals(id2) {
				continue
			}
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		c.shards[shardID] = id
		return nil
	}

	// child
	for _, pair := range t.Entries {
		blobID, peerID := splitKey(pair.Key)
		c.store.Put(blobID, peerID)
	}
	c.shards[shardID] = id
	return nil
}

func (c *Crawler) request(ctx context.Context, nextHop p2p.PeerID, req *ListBlobsReq) (*ListBlobsRes, error) {
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	resData, err := c.peerSwarm.Ask(ctx, nextHop, reqData)
	if err != nil {
		return nil, err
	}
	res := &ListBlobsRes{}
	if err := proto.Unmarshal(resData, res); err != nil {
		return nil, err
	}
	return res, nil
}

func splitKey(x []byte) (blobs.ID, p2p.PeerID) {
	l := len(x)
	blobID := blobs.ID{}
	peerID := p2p.PeerID{}
	copy(blobID[:], x[l/2:])
	copy(peerID[:], x[:l/2])
	return blobID, peerID
}
