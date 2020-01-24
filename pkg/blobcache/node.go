package blobcache

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"

	bolt "go.etcd.io/bbolt"
)

type Node struct {
	swarm p2p.Swarm

	metadataDB *bolt.DB
	dataDB     *bolt.DB

	localStore *LocalStore
	pinSets    *PinSetStore
}

func NewNode(params Params) (*Node, error) {
	pinSetStore, err := NewPinSetStore(params.MetadataDB)
	if err != nil {
		return nil, err
	}

	n := &Node{
		swarm: params.Swarm,

		metadataDB: params.MetadataDB,
		dataDB:     params.DataDB,

		localStore: &LocalStore{
			capacity: params.Capacity,
			db:       params.DataDB,
		},
		pinSets: pinSetStore,
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
	return n.localStore.Get(ctx, id)
}

func (n *Node) Post(ctx context.Context, name string, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	if err := n.pinSets.Pin(ctx, name, id); err != nil {
		return blobs.ZeroID(), err
	}
	_, err := n.localStore.Post(ctx, data)
	if err != nil {
		return blobs.ZeroID(), err
	}
	// TODO: fire and forget to network
	// TODO: depending on persistance config, ensure replication
	return id, nil
}

func (n *Node) MaxBlobSize() int {
	return 1 << 16
}

func (n *Node) LocalAddr() string {
	data, _ := n.swarm.LocalAddr().MarshalText()
	return string(data)
}
