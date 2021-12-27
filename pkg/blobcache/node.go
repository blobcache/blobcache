package blobcache

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobnet"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jonboulle/clockwork"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ Service = &Node{}

type Params struct {
	DB              bcstate.TxDB
	Mux             p2pmux.StringSecureAskMux
	PrivateKey      p2p.PrivateKey
	PeerStore       peers.PeerStore
	Stores          []stores.Store
	ExternalSources []Source
}

type Node struct {
	db      bcstate.TxDB
	pinSets *PinSetStore

	readChain  stores.ReadChain
	stores     []stores.Store
	extSources []Source

	bn *blobnet.Blobnet
}

func NewNode(params Params) *Node {
	pinSetStore := NewPinSetStore(bcstate.PrefixedTxDB{
		TxDB:   params.DB,
		Prefix: "pinsets",
	})

	readChain := stores.ReadChain{}
	for _, s := range params.Stores {
		readChain = append(readChain, s)
	}
	for _, extSource := range params.ExternalSources {
		readChain = append(readChain, extSource)
	}

	log.WithFields(log.Fields{
		"local_id": p2p.NewPeerID(params.PrivateKey.Public()),
	}).Info("starting node")
	n := &Node{
		db:         params.DB,
		pinSets:    pinSetStore,
		readChain:  readChain,
		stores:     params.Stores,
		extSources: params.ExternalSources,

		bn: blobnet.NewBlobNet(blobnet.Params{
			Mux:       params.Mux,
			Local:     readChain,
			PeerStore: params.PeerStore,
			DB:        bcstate.PrefixedDB{DB: params.DB, Prefix: "blobnet"},
			Clock:     clockwork.NewRealClock(),
		}),
	}
	return n
}

func (n *Node) Shutdown() error {
	return n.bn.Close()
}

func (n *Node) CreatePinSet(ctx context.Context, name string) (PinSetID, error) {
	return n.pinSets.Create(ctx, name)
}

func (n *Node) DeletePinSet(ctx context.Context, pinset PinSetID) error {
	return n.pinSets.Delete(ctx, pinset)
}

func (n *Node) Pin(ctx context.Context, pinset PinSetID, id cadata.ID) error {
	return n.pinSets.Pin(ctx, pinset, id)
}

func (n *Node) Unpin(ctx context.Context, pinset PinSetID, id cadata.ID) error {
	return n.pinSets.Unpin(ctx, pinset, id)
}

func (n *Node) Get(ctx context.Context, pinsetID PinSetID, id cadata.ID, buf []byte) (int, error) {
	readChain := append(n.readChain, n.bn)
	return readChain.Get(ctx, id, buf)
}

func (n *Node) Post(ctx context.Context, pinset PinSetID, data []byte) (cadata.ID, error) {
	id := cadata.DefaultHash(data)
	if err := n.pinSets.Pin(ctx, pinset, id); err != nil {
		return cadata.ID{}, err
	}

	// don't persist data if it is in an external source
	for _, s := range n.extSources {
		if exists, err := s.Exists(ctx, id); err != nil {
			return cadata.ID{}, err
		} else if exists {
			return id, nil
		}
	}

	if len(n.stores) < 1 {
		return cadata.ID{}, errors.Errorf("cannot post data, no stores")
	}
	store := n.stores[0]
	id, err := store.Post(ctx, data)
	if err != nil {
		return cadata.ID{}, nil
	}

	// TODO: fire and forget to network
	// TODO: depending on persistance config, ensure replication
	return id, nil
}

func (node *Node) List(ctx context.Context, psID PinSetID, prefix []byte, ids []cadata.ID) (n int, err error) {
	return node.pinSets.List(ctx, psID, prefix, ids)
}

func (n *Node) Exists(ctx context.Context, psID PinSetID, id cadata.ID) (bool, error) {
	return n.pinSets.Exists(ctx, psID, id)
}

func (n *Node) GetPinSet(ctx context.Context, pinset PinSetID) (*PinSet, error) {
	return n.pinSets.Get(ctx, pinset)
}

func (n *Node) MaxBlobSize() int {
	return stores.MaxSize
}
