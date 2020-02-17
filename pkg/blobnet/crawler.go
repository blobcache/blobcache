package blobnet

import (
	"bytes"
	"context"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
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

	revs map[ShardID]blobs.ID
}

func newCrawler(peerRouter *Router, peerSwarm *PeerSwarm, store *BlobLocStore) *Crawler {
	return &Crawler{
		peerRouter: peerRouter,
		peerSwarm:  peerSwarm,
		store:      store,
		revs:       map[ShardID]blobs.ID{},
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
	rt, nextHop := c.peerRouter.Lookup(peerID)
	if rt == nil {
		delete(c.revs, ShardID{peerID, string(prefix)})
		return ErrNoRouteToPeer
	}

	req := &ListBlobsReq{
		RoutingTag: rt,
		Prefix:     prefix,
	}
	res, err := c.listBlobs(ctx, nextHop, req)
	if err != nil {
		delete(c.revs, ShardID{peerID, string(prefix)})
		return err
	}
	switch x := res.Res.(type) {
	case *ListBlobsRes_TooMany:
		if lastID, ok := c.revs[ShardID{peerID, string(prefix)}]; ok {
			if bytes.Compare(res.TrieHash, lastID[:]) == 0 {
				return nil
			}
		}
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		if len(res.TrieHash) > 0 {
			id := blobs.ID{}
			copy(id[:], res.TrieHash)
			c.revs[ShardID{peerID, string(prefix)}] = id
		}

	case *ListBlobsRes_BlobList:
		if lastID, ok := c.revs[ShardID{peerID, string(prefix)}]; ok {
			if bytes.Compare(res.TrieHash, lastID[:]) == 0 {
				return nil
			}
		}
		for _, claim := range x.BlobList.Claims {
			bloc := &BlobLoc{
				BlobId:    claim.BlobId,
				PeerId:    peerID[:],
				ExpiresAt: time.Now().Add(24 * time.Hour).Unix(),
			}
			if err := c.store.Put(bloc); err != ErrShouldEvictThis {
				return ErrShouldEvictThis
			}
		}
		if len(res.TrieHash) > 0 {
			id := blobs.ID{}
			copy(id[:], res.TrieHash)
			c.revs[ShardID{peerID, string(prefix)}] = id
		}
	}
	return nil
}

func (c *Crawler) listBlobs(ctx context.Context, nextHop p2p.PeerID, req *ListBlobsReq) (*ListBlobsRes, error) {
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

func (c *Crawler) getAtDistance(d int) []p2p.PeerID {
	pinfos := c.peerRouter.GetPeerInfos()

	peerIDs := []p2p.PeerID{}
	for _, pinfo := range pinfos {
		if len(pinfo.Path) == d {
			id := p2p.PeerID{}
			copy(id[:], pinfo.Id)
			peerIDs = append(peerIDs, id)
		}
	}
	return peerIDs
}
