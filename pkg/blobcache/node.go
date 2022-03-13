package blobcache

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/sirupsen/logrus"
	"lukechampine.com/blake3"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/bcnet"
	"github.com/blobcache/blobcache/pkg/blobcache/control"
	"github.com/blobcache/blobcache/pkg/stores"
)

var _ Service = &Node{}

type PeerInfo struct {
	ID         inet256.ID
	QuotaCount uint64
}

type Params struct {
	DB      bcdb.DB
	Primary cadata.Store

	INET256    inet256.Service
	PrivateKey p2p.PrivateKey
	Peers      []PeerInfo

	Logger   *logrus.Logger
	MaxCount int64
}

func NewMemParams() Params {
	pk := ed25519.NewKeyFromSeed(make([]byte, 32))
	return Params{
		DB:         bcdb.NewBadgerMemory(),
		Primary:    stores.NewMem(),
		PrivateKey: pk,
	}
}

type Node struct {
	db      bcdb.DB
	localID bcnet.PeerID

	storeMux *storeMux    // manages all access to the local store
	setMan   *setManager  // default set manager
	psSetMan *setManager  // set manager for pinset blobs
	pinSets  *pinSetStore // pinset metadata store
	ctrl     *control.Controller

	log *logrus.Logger
}

func NewNode(params Params) *Node {
	pinSetStore := newPinSetStore(bcdb.NewPrefixed(params.DB, "pinset-meta\x00"))
	psSetMan := newSetManager(bcdb.NewPrefixed(params.DB, "pinset-sets\x00"))
	setMan := newSetManager(bcdb.NewPrefixed(params.DB, "sets\x00"))
	storeMux := newStoreMux(bcdb.NewPrefixed(params.DB, "stores\x00"), params.Primary)
	n := &Node{
		db:      params.DB,
		localID: inet256.NewAddr(params.PrivateKey.Public()),

		storeMux: storeMux,
		setMan:   setMan,
		psSetMan: psSetMan,
		pinSets:  pinSetStore,
		ctrl:     control.New(),
		log:      params.Logger,
	}
	n.ctrl.AttachSource("local", control.Source{
		Set:              psSetMan.union(),
		ExpectedReplicas: 2.0,
	})
	n.ctrl.AttachSink("local", control.Sink{
		Locus:   n.localID,
		Desired: setMan.open("desired/local"),
		// TODO
		Notify: func(cadata.ID) {},
		Flush:  func(context.Context, map[cadata.ID]struct{}) error { return nil },
	})
	for _, peer := range params.Peers {
		sourceName := "peer-source/" + peer.ID.String()
		n.ctrl.AttachSource(sourceName, control.Source{
			Set:              setMan.open(sourceName),
			ExpectedReplicas: 1,
		})
		sinkName := "peer-sink/" + peer.ID.String()
		n.ctrl.AttachSink(sinkName, control.Sink{
			Locus:   peer.ID,
			Desired: setMan.open(sinkName),
			// TODO
			Notify: func(cadata.ID) {},
			Flush:  func(context.Context, map[cadata.ID]struct{}) error { return nil },
		})
	}
	return n
}

// CreatePinSet implementes Service
func (n *Node) CreatePinSet(ctx context.Context, opts PinSetOptions) (*PinSetHandle, error) {
	id, err := n.pinSets.Create(ctx, PinSetOptions{})
	if err != nil {
		return nil, err
	}
	return &PinSetHandle{ID: *id}, nil
}

// GetPinSet implements Service
func (n *Node) GetPinSet(ctx context.Context, psh PinSetHandle) (*PinSet, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return nil, err
	}
	_, err = n.pinSets.Get(ctx, psID)
	if err != nil {
		return nil, err
	}
	return &PinSet{
		Status: StatusOK,
	}, nil
}

// DeletePinSet implements Service
func (n *Node) DeletePinSet(ctx context.Context, psh PinSetHandle) error {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	if err := n.pinSets.Delete(ctx, psID); err != nil {
		return err
	}
	return n.psSetMan.drop(ctx, psID.HexString())
}

// Add implements Service
func (node *Node) Add(ctx context.Context, psh PinSetHandle, id cadata.ID) error {
	psID, err := node.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	store := node.mainStore()
	set := node.getPSSet(psID)
	if exists, err := cadata.Exists(ctx, store, id); err != nil {
		return err
	} else if exists {
		return set.Add(ctx, id)
	}
	return ErrDataNotFound
}

func (n *Node) Delete(ctx context.Context, psh PinSetHandle, id cadata.ID) error {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	set := n.getPSSet(psID)
	if err := set.Delete(ctx, id); err != nil {
		return err
	}
	// n.ctrl.Notify(ctx, id)
	return nil
}

func (n *Node) Post(ctx context.Context, psh PinSetHandle, data []byte) (cadata.ID, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return cadata.ID{}, err
	}
	if len(data) > n.MaxSize() {
		return cadata.ID{}, cadata.ErrTooLarge
	}
	store := n.mainStore()
	id, err := store.Post(ctx, data)
	if err != nil {
		return cadata.ID{}, err
	}
	set := n.getPSSet(psID)
	if err := set.Add(ctx, id); err != nil {
		return cadata.ID{}, err
	}
	// n.ctrl.Notify(ctx, id)
	return id, nil
}

func (n *Node) Get(ctx context.Context, psh PinSetHandle, id cadata.ID, buf []byte) (int, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return 0, err
	}
	set := n.getPSSet(psID)
	if exists, err := set.Exists(ctx, id); err != nil {
		return 0, err
	} else if !exists {
		return 0, cadata.ErrNotFound
	}
	store := n.mainStore()
	// TODO: need to handle cases where the blob is not in the local store.
	return store.Get(ctx, id, buf)
}

func (node *Node) List(ctx context.Context, psh PinSetHandle, prefix []byte, ids []cadata.ID) (n int, err error) {
	psID, err := node.resolvePinSet(ctx, psh)
	if err != nil {
		return 0, err
	}
	set := node.getPSSet(psID)
	return set.List(ctx, prefix, ids)
}

func (n *Node) Exists(ctx context.Context, psh PinSetHandle, id cadata.ID) (bool, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return false, err
	}
	set := n.getPSSet(psID)
	return cadata.Exists(ctx, set, id)
}

func (n *Node) WaitOK(ctx context.Context, psh PinSetHandle) error {
	_, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	return n.ctrl.Flush(ctx, "local")
}

func (n *Node) MaxSize() int {
	return MaxSize
}

func (n *Node) resolvePinSet(ctx context.Context, psh PinSetHandle) (PinSetID, error) {
	key := handleKey(psh)
	logrus.Info("resolved handle ", key)
	// TODO: lookup in db
	return psh.ID, nil
}

func (n *Node) mainStore() cadata.Store {
	return n.storeMux.open("main")
}

func (n *Node) getPSSet(psID PinSetID) cadata.Set {
	return n.psSetMan.open(psID.HexString())
}

func handleKey(psh PinSetHandle) (ret [16]byte) {
	h := blake3.New(32, nil)
	binary.Write(h, binary.BigEndian, psh.ID)
	h.Write(psh.Secret[:])
	sum := h.Sum(nil)
	copy(ret[:], sum)
	return ret
}
