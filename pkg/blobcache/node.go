package blobcache

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobnet"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

type Params struct {
	MetadataDB *bolt.DB
	Cache      Cache

	Mux        simplemux.Muxer
	PrivateKey p2p.PrivateKey
	PeerStore  blobnet.PeerStore

	ExternalSources []blobs.Getter
}

type Node struct {
	metadataDB *bolt.DB
	dataDB     *bolt.DB

	localCache Cache
	pinSets    *PinSetStore

	readChain  blobs.ReadChain
	extSources []blobs.Getter

	bn *blobnet.Blobnet
}

func NewNode(params Params) (*Node, error) {
	pinSetStore, err := NewPinSetStore(params.MetadataDB)
	if err != nil {
		return nil, err
	}

	readChain := blobs.ReadChain{
		newLocalStore(params.Cache),
	}
	for _, extSource := range params.ExternalSources {
		readChain = append(readChain, extSource)
	}

	n := &Node{
		metadataDB: params.MetadataDB,

		localCache: params.Cache,
		pinSets:    pinSetStore,
		readChain:  readChain,
		extSources: params.ExternalSources,

		bn: blobnet.NewBlobNet(blobnet.Params{
			Mux:       params.Mux,
			Local:     readChain,
			PeerStore: params.PeerStore,
		}),
	}

	return n, nil
}

func (n *Node) Shutdown() error {
	return n.bn.Close()
}

func (n *Node) CreatePinSet(ctx context.Context, name string) error {
	return n.pinSets.Create(ctx, name)
}

func (n *Node) Pin(ctx context.Context, name string, id blobs.ID) error {
	return n.pinSets.Pin(ctx, name, id)
}

func (n *Node) Unpin(ctx context.Context, name string, id blobs.ID) error {
	return n.pinSets.Unpin(ctx, name, id)
}

func (n *Node) Get(ctx context.Context, id blobs.ID) (blobs.Blob, error) {
	readChain := append(n.readChain, n.bn)
	return readChain.Get(ctx, id)
}

func (n *Node) Post(ctx context.Context, name string, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	if err := n.pinSets.Pin(ctx, name, id); err != nil {
		return blobs.ZeroID(), err
	}

	// don't persist data if it is in an external source
	for _, s := range n.extSources {
		exists, err := s.Exists(ctx, id)
		if err != nil {
			log.Error(err)
			continue
		}
		if exists {
			return id, nil
		}
	}

	// persist that data to our cache
	err := n.localCache.Put(ctx, id[:], data)
	if err != nil {
		return blobs.ZeroID(), err
	}
	// TODO: fire and forget to network
	// TODO: depending on persistance config, ensure replication
	return id, nil
}

func (n *Node) GetPinSet(ctx context.Context, name string) (*PinSet, error) {
	return n.pinSets.Get(ctx, name)
}

func (n *Node) MaxBlobSize() int {
	return blobs.MaxSize
}
