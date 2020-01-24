package blobnet

import (
	"bytes"
	"context"
	"time"

	"github.com/boltdb/bolt"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/kademlia"
	proto "github.com/golang/protobuf/proto"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

const indexBucketName = "blob2peer"

type Indexer struct {
	swarm  p2p.AskSwarm
	db     *bolt.DB
	router *Router
}

func NewIndexer(db *bolt.DB, swarm p2p.AskSwarm, router *Router) (*Index, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(indexBucketName)
	})
	if err != nil {
		return nil, err
	}
	return &Index{
		swarm:  swarm,
		router: router,
		db:     db,
	}, nil
}

func (in *Indexer) Run(ctx context.Context) error {
	const period = 10 * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ctx, cf := context.WithTimeout(ctx, period)
			in.search
			cf()
		}
	}
}

func (in *Indexer) queryPeer(ctx context.Context, peerID p2p.PeerID) error {
	localID := in.swarm.LocalID()
	for i := 0; i < len(localID); i++ {
		prefix = localID[:i]
		path := in.router.PathTo(peerId)
		req := &ListBlobReq{
			DstId:  peerID,
			Path:   path,
			Prefix: prefix,
		}
		res, err := ListBlobs(req)
		if err != nil {
			return err
		}
		if !res.TooMany {
			break
		}
	}
	return nil
}

func (in *Indexer) ListBlobs(ctx context.Context, req *ListBlobReq) (res *ListBlobRes, err error) {
	var peerID p2p.PeerID
	if len(path) > 1 {
		peerID = in.router.AtIndex(req.Path[0])
		req.Path = req.Path[1:]
	} else {
		peerID = in.router.AtIndex(req.Path[0])
		req.Path = nil
	}
	reqData, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	resData, err := in.swarm.Ask(ctx, peerID, reqData)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (in *Indexer) Add(bid blobs.ID, pid p2p.PeerID) error {
	err := in.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		currentPid := b.Get(bid[:])
		distCur := kademlia.XORBytes(currentPid, bid)
		distNew := kademlia.XORBytes(pid, bid)
		if bytes.Compare(distNew, distCur) < 0 {
			tx.Put(bid[:], pid[:])
		}
	})
	return err
}

func (in *Indexer) Remove(bid blobs.ID, pid p2p.PeerID) error {
	err := in.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		currentPid := b.Get(bid[:])
		if currentPid.Equals(pid) {
			b.Delete(bid[:])
		}
	})
	return err
}

func (in *Indexer) WhoHas(bid blobs.ID) (p2p.PeerID, error) {
	var pid p2p.PeerID
	err := in.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		v := b.Get(bid[:])
		copy(pid[:], v)
	})
	if err != nil {
		return p2p.ZeroPeerID(), err
	}

	return pid, nil
}
