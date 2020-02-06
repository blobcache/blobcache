package blobcache

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p/p/simplemux"

	bolt "go.etcd.io/bbolt"
)

type Node struct {
	mux simplemux.Muxer

	metadataDB *bolt.DB
	dataDB     *bolt.DB

	localCache Cache
	pinSets    *PinSetStore
}

func NewNode(params Params) (*Node, error) {
	pinSetStore, err := NewPinSetStore(params.MetadataDB)
	if err != nil {
		return nil, err
	}

	n := &Node{
		mux:        params.Mux,
		metadataDB: params.MetadataDB,

		localCache: params.Cache,
		pinSets:    pinSetStore,
	}

	return n, nil
}

func (n *Node) Shutdown() error {
	return nil
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
	return n.localCache.Get(ctx, id[:])
}

func (n *Node) Post(ctx context.Context, name string, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	if err := n.pinSets.Pin(ctx, name, id); err != nil {
		return blobs.ZeroID(), err
	}
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
	return 1 << 16
}
