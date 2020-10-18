package blobcache

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobnet"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

type Params struct {
	Ephemeral  bcstate.TxDB
	Persistent bcstate.TxDB

	Mux        simplemux.Muxer
	PrivateKey p2p.PrivateKey
	PeerStore  peers.PeerStore

	ExternalSources []Source
}

var _ API = &Node{}

type Node struct {
	ephemeral  bcstate.DB
	persistent bcstate.DB
	pinSets    *PinSetStore

	readChain  blobs.ReadChain
	extSources []Source

	bn *blobnet.Blobnet
}

func NewNode(params Params) *Node {
	pinSetStore := NewPinSetStore(bcstate.PrefixedTxDB{
		TxDB:   params.Persistent,
		Prefix: "pinsets",
	})

	ephemeralBlobs := params.Ephemeral.Bucket("blobs")
	persistentBlobs := params.Persistent.Bucket("blobs")

	readChain := blobs.ReadChain{
		bcstate.BlobAdapter(ephemeralBlobs),
		bcstate.BlobAdapter(persistentBlobs),
	}
	for _, extSource := range params.ExternalSources {
		readChain = append(readChain, extSource)
	}

	log.WithFields(log.Fields{
		"local_id": p2p.NewPeerID(params.PrivateKey.Public()),
	}).Info("starting node")
	n := &Node{
		ephemeral:  params.Ephemeral,
		persistent: params.Persistent,

		pinSets:    pinSetStore,
		readChain:  readChain,
		extSources: params.ExternalSources,

		bn: blobnet.NewBlobNet(blobnet.Params{
			Mux:       params.Mux,
			Local:     readChain,
			PeerStore: params.PeerStore,
			DB:        bcstate.PrefixedDB{DB: params.Ephemeral, Prefix: "blobnet"},
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

func (n *Node) Pin(ctx context.Context, pinset PinSetID, id blobs.ID) error {
	return n.pinSets.Pin(ctx, pinset, id)
}

func (n *Node) Unpin(ctx context.Context, pinset PinSetID, id blobs.ID) error {
	return n.pinSets.Unpin(ctx, pinset, id)
}

func (n *Node) GetF(ctx context.Context, id blobs.ID, fn func([]byte) error) error {
	readChain := append(n.readChain, n.bn)
	return readChain.GetF(ctx, id, fn)
}

func (n *Node) Post(ctx context.Context, pinset PinSetID, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	if err := n.pinSets.Pin(ctx, pinset, id); err != nil {
		return blobs.ID{}, err
	}

	// don't persist data if it is in an external source
	for _, s := range n.extSources {
		exists, err := s.Exists(ctx, id)
		if err != nil {
			return blobs.ID{}, err
		}
		if exists {
			return id, nil
		}
	}

	// persist that data to local storage
	err := n.persistent.Bucket("blobs").Put(id[:], data)
	if err == bcstate.ErrFull {
		// TODO: must be on the network
		return blobs.ID{}, err
	} else if err != nil {
		return blobs.ID{}, err
	}

	// TODO: fire and forget to network
	// TODO: depending on persistance config, ensure replication
	return id, nil
}

func (n *Node) GetPinSet(ctx context.Context, pinset PinSetID) (*PinSet, error) {
	return n.pinSets.Get(ctx, pinset)
}

func (n *Node) MaxBlobSize() int {
	return blobs.MaxSize
}
