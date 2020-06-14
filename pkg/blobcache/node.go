package blobcache

import (
	"context"
	"errors"

	"github.com/brendoncarroll/blobcache/pkg/bcstate"
	"github.com/brendoncarroll/blobcache/pkg/blobnet"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/jonboulle/clockwork"
	"github.com/multiformats/go-multihash"
	log "github.com/sirupsen/logrus"
)

type API interface {
	// PinSets
	// CreatePinSet creates a new pinset. if err == nil PinSetID will be > 0
	CreatePinSet(ctx context.Context, name string) (PinSetID, error)
	DeletePinSet(ctx context.Context, pinset PinSetID) error
	GetPinSet(ctx context.Context, pinset PinSetID) (*PinSet, error)
	// Pin adds mh to the pinset.  If mh is already in the pinset err == nil
	Pin(ctx context.Context, pinset PinSetID, mh []byte) error
	// Unpin removes mh from the pinset. if mh is not in the pinset err == nil
	Unpin(ctx context.Context, pinset PinSetID, mh []byte) error

	// Blobs
	// Post calculates the hash of data, adds it to the pinset and persists the data.
	// if pinset == 0 the data will be posted to the cache, and there are no persistence gaurentees.
	// len(data) <= MaxBlobSize()
	Post(ctx context.Context, pinset PinSetID, data []byte) ([]byte, error)
	// GetF calls f with data that hashes to mh or returns an error. Errors from f will be returned.
	GetF(ctx context.Context, mh []byte, f func([]byte) error) error
	// MaxBlobSize is the maximum size of a single blob.
	MaxBlobSize() int
}

var _ API = &Node{}

type Params struct {
	Ephemeral  bcstate.TxDB
	Persistent bcstate.TxDB

	Mux        simplemux.Muxer
	PrivateKey p2p.PrivateKey
	PeerStore  peers.PeerStore

	ExternalSources []blobs.Getter
}

type Node struct {
	ephemeral  bcstate.DB
	persistent bcstate.DB
	pinSets    *PinSetStore

	readChain  blobs.ReadChain
	extSources []blobs.Getter

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

func (n *Node) Pin(ctx context.Context, pinset PinSetID, mh []byte) error {
	id, err := decodeMH(mh)
	if err != nil {
		return err
	}
	return n.pinSets.Pin(ctx, pinset, id)
}

func (n *Node) Unpin(ctx context.Context, pinset PinSetID, mh []byte) error {
	id, err := decodeMH(mh)
	if err != nil {
		return err
	}
	return n.pinSets.Unpin(ctx, pinset, id)
}

func (n *Node) GetF(ctx context.Context, mh []byte, fn func([]byte) error) error {
	id, err := decodeMH(mh)
	if err != nil {
		return err
	}
	readChain := append(n.readChain, n.bn)
	return readChain.GetF(ctx, id, fn)
}

func (n *Node) Post(ctx context.Context, pinset PinSetID, data []byte) ([]byte, error) {
	id := blobs.Hash(data)
	mh := encodeMH(id)
	if pinset == 0 {
		if err := n.ephemeral.Bucket("blobs").Put(id[:], data); err != nil {
			return nil, err
		}
		return mh, nil
	}

	if err := n.pinSets.Pin(ctx, pinset, id); err != nil {
		return nil, err
	}
	// don't persist data if it is in an external source
	for _, s := range n.extSources {
		exists, err := s.Exists(ctx, id)
		if err != nil {
			log.Error(err)
			continue
		}
		if exists {
			return mh, nil
		}
	}

	// persist that data to local storage
	err := n.persistent.Bucket("blobs").Put(id[:], data)
	if err == bcstate.ErrFull {
		// TODO: must be on the network
		return nil, err
	} else if err != nil {
		return nil, err
	}

	// TODO: fire and forget to network
	// TODO: depending on persistance config, ensure replication
	return mh, nil
}

func (n *Node) GetPinSet(ctx context.Context, pinset PinSetID) (*PinSet, error) {
	return n.pinSets.Get(ctx, pinset)
}

func (n *Node) MaxBlobSize() int {
	return blobs.MaxSize
}

// https://github.com/multiformats/multicodec/blob/master/table.csv
const mhBLAKE3 = 0x1e

func encodeMH(id blobs.ID) []byte {
	mh, err := multihash.Encode(id[:], mhBLAKE3)
	if err != nil {
		panic(err)
	}
	return mh
}

func decodeMH(mh []byte) (blobs.ID, error) {
	id := blobs.ZeroID()
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return id, err
	}
	if dmh.Code != mhBLAKE3 {
		return id, errors.New("unsupported hash function")
	}
	if dmh.Length != 32 {
		return id, errors.New("unsupported hash length")
	}
	copy(id[:], dmh.Digest)
	return id, nil
}
