package blobcache

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/dgraph-io/badger/v2"
	"github.com/inet256/inet256/pkg/inet256"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/calock"
	"github.com/blobcache/blobcache/pkg/stores"
)

var _ Service = &Node{}

type PeerInfo struct {
	ID         inet256.ID
	QuotaCount uint64
}

type Params struct {
	DB         badger.DB
	Store      cadata.Store
	PrivateKey p2p.PrivateKey
	Peers      []PeerInfo
	INET256    inet256.Service
}

type Node struct {
	db      *badger.DB
	pinSets *PinSetStore

	locker *calock.Locker
	store  Store
}

func NewNode(params Params) *Node {
	pinSetStore := NewPinSetStore(bcdb.PrefixedTxDB{
		TxDB:   params.DB,
		Prefix: "pinsets",
	})
	n := &Node{
		db:      params.DB,
		pinSets: pinSetStore,
		locker:  calock.NewLocker(),
		store:   params.Store,
	}
	return n
}

func (n *Node) CreatePinSet(ctx context.Context, opts PinSetOptions) (PinSetID, error) {
	return n.pinSets.Create(ctx, name)
}

func (n *Node) DeletePinSet(ctx context.Context, psh PinSetHandle) error {
	ps, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	return n.pinSets.Delete(ctx, pinset)
}

func (n *Node) Add(ctx context.Context, psh PinSetHandle, id cadata.ID) error {
	ps, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	if err := n.locker.LockAdd(ctx, id); err != nil {
		return err
	}
	return n.pinSets.Pin(ctx, pinset, id)
}

func (n *Node) Delete(ctx context.Context, psh PinSetHandle, id cadata.ID) error {
	return n.pinSets.Unpin(ctx, pinset, id)
}

func (n *Node) Post(ctx context.Context, psh PinSetHandle, data []byte) (cadata.ID, error) {
	return n.pinSets.Post(ctx, psh, data)
}

func (n *Node) Get(ctx context.Context, psh PinSetHandle, id cadata.ID, buf []byte) (int, error) {
	readChain := append(n.readChain, n.bn)
	return readChain.Get(ctx, id, buf)
}

func (node *Node) List(ctx context.Context, psh PinSetHandle, prefix []byte, ids []cadata.ID) (n int, err error) {
	return node.pinSets.List(ctx, psID, prefix, ids)
}

func (n *Node) Exists(ctx context.Context, psh PinSetHandle, id cadata.ID) (bool, error) {
	return n.pinSets.Exists(ctx, psID, id)
}

func (n *Node) GetPinSet(ctx context.Context, psh PinSetHandle) (*PinSet, error) {
	return n.pinSets.Get(ctx, pinset)
}

func (n *Node) MaxBlobSize() int {
	return stores.MaxSize
}
