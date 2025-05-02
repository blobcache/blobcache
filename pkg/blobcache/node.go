package blobcache

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/sirupsen/logrus"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/kv"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/bcnet"
	"github.com/blobcache/blobcache/pkg/blobcache/control"
	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/blobcache/blobcache/pkg/stores"
)

const (
	systemDirName = "__system"
)

var _ Service = &Node{}

type PeerInfo struct {
	ID         inet256.ID
	QuotaCount uint64
}

type StoreSpec struct {
	Name       string
	Cost       control.Cost
	BatchSize  int
	BatchDelay int
	Target     control.Target
}

type Params struct {
	// DB is the database to use for Directories, PinSets and other metadata. (REQUIRED)
	DB bcdb.DB
	// Primary is the Primary store (REQUIRED)
	Primary cadata.Store
	// PrivateKey is a private key used by the node for secure connections and for deriving a unique identifer. (REQUIRED)
	PrivateKey inet256.PrivateKey

	// AuxStores are auxillary stores.
	AuxStores []StoreSpec

	// INET256 is the INET256 service to use. (REQUIRED if len(Peers) > 0 )
	INET256 inet256.Service
	// Peers contains information about other Blobcache nodes to connect to.
	Peers []PeerInfo

	Logger *logrus.Logger

	// The maximum number of blobs to store in the Primary if any.
	MaxCount int64
}

func NewMemParams() Params {
	pk, _ := inet256.PrivateKeyFromBuiltIn(ed25519.NewKeyFromSeed(make([]byte, 32)))
	return Params{
		DB:         bcdb.NewBadgerMemory(),
		Primary:    stores.NewMem(),
		PrivateKey: pk,
	}
}

type Node struct {
	db      bcdb.DB
	localID bcnet.PeerID

	dirServ  *dirserv.DirServer
	storeMux *storeMux   // manages all access to the local store
	setMan   *setManager // default set manager
	psSetMan *setManager // set manager for pinset blobs
	ctrl     *control.Controller

	log *logrus.Logger
}

func NewNode(params Params) *Node {
	if params.Logger == nil {
		params.Logger = logrus.StandardLogger()
	}
	psSetMan := newSetManager(bcdb.NewPrefixed(params.DB, "pinset-sets\x00"))
	setMan := newSetManager(bcdb.NewPrefixed(params.DB, "sets\x00"))
	storeMux := newStoreMux(bcdb.NewPrefixed(params.DB, "stores\x00"), params.Primary)
	dirServ := dirserv.New(bcdb.NewPrefixed(params.DB, "dirs\x00"))
	n := &Node{
		db:      params.DB,
		localID: inet256.NewAddr(params.PrivateKey.Public()),

		dirServ:  dirServ,
		storeMux: storeMux,
		setMan:   setMan,
		psSetMan: psSetMan,
		log:      params.Logger,
	}
	n.ctrl = control.New(params.Logger, func(name string) cadata.Set {
		return n.getSystemSet([]string{"sink", name})
	})
	n.ctrl.AttachSource("self", control.Source{
		Set:              psSetMan.union(),
		ExpectedReplicas: 2.0,
	})
	n.ctrl.AttachSink("self-main", control.Sink{
		Locus:     n.localID,
		BatchSize: 0, // Disable batching
		Target:    control.BasicTarget{Store: n.mainStore()},
	})
	// aux stores
	for _, auxSpec := range params.AuxStores {
		sinkName := "self-aux-" + auxSpec.Name
		n.ctrl.AttachSink(sinkName, control.Sink{
			Locus:     n.localID,
			BatchSize: auxSpec.BatchSize,
			Target:    auxSpec.Target,
		})
	}
	// peers
	for _, peer := range params.Peers {
		sourceName := "peer-" + peer.ID.String()
		n.ctrl.AttachSource(sourceName, control.Source{
			Set:              n.getSystemSet([]string{"source", sourceName}),
			ExpectedReplicas: 1,
		})
		sinkName := "peer-" + peer.ID.String()
		n.ctrl.AttachSink(sinkName, control.Sink{
			Locus:      peer.ID,
			BatchSize:  50,
			BatchDelay: 30 * time.Second,
			Target:     control.BasicTarget{Store: cadata.NewVoid(Hash, MaxSize)},
		})
	}
	go n.ctrl.Refresh(context.Background())
	return n
}

// CreateDir implements Service
func (n *Node) CreateDir(ctx context.Context, h Handle, name string) (*Handle, error) {
	if err := n.validateModify(h, name); err != nil {
		return nil, err
	}
	return n.dirServ.Create(ctx, h, name, nil)
}

// ListEntries
func (n *Node) ListEntries(ctx context.Context, h Handle) ([]Entry, error) {
	ents, err := n.dirServ.List(ctx, h)
	if err != nil {
		return nil, err
	}
	// filter systemDirName
	if h.ID == dirserv.RootOID {
		var ents2 []dirserv.Entry
		for _, ent := range ents {
			if ent.Name != systemDirName {
				ents2 = append(ents2, ent)
			}
		}
		ents = ents2
	}
	return ents, nil
}

// DeleteEntry implements Service
func (n *Node) DeleteEntry(ctx context.Context, h Handle, name string) error {
	if err := n.validateModify(h, name); err != nil {
		return err
	}
	_, err := n.dirServ.Remove(ctx, h, name)
	return err
}

// Open implements Service
func (n *Node) Open(ctx context.Context, h Handle, p []string) (*Handle, error) {
	if h.ID == dirserv.RootOID && len(p) > 0 && p[0] == systemDirName {
		return nil, errors.New("cannot open " + systemDirName)
	}
	return n.dirServ.Open(ctx, h, p)
}

// CreatePinSet implements Service
func (n *Node) CreatePinSet(ctx context.Context, h Handle, name string, opts PinSetOptions) (*Handle, error) {
	if err := n.validateModify(h, name); err != nil {
		return nil, err
	}
	data, err := json.Marshal(struct{}{})
	if err != nil {
		return nil, err
	}
	return n.dirServ.Create(ctx, h, name, data)
}

// GetPinSet implements Service
func (n *Node) GetPinSet(ctx context.Context, psh Handle) (*PinSet, error) {
	oid, data, err := n.dirServ.Get(ctx, psh, nil)
	if err != nil {
		return nil, err
	}
	if oid == dirserv.NullOID {
		return nil, errors.New("pinset not found")
	}
	if !bytes.Equal(data, []byte("{}")) {
		return nil, errors.New("object is not a pinset")
	}
	return &PinSet{Status: StatusOK}, nil
}

// Add implements Service
func (node *Node) Add(ctx context.Context, psh Handle, id cadata.ID) error {
	psID, err := node.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	store := node.mainStore()
	set := node.getPSSet(psID)
	if exists, err := kv.ExistsUsingList(ctx, store, id); err != nil {
		return err
	} else if exists {
		return set.Add(ctx, id)
	}
	return cadata.ErrNotFound{Key: id}
}

func (n *Node) Delete(ctx context.Context, psh Handle, id cadata.ID) error {
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

func (n *Node) Post(ctx context.Context, psh Handle, data []byte) (cadata.ID, error) {
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

func (n *Node) Get(ctx context.Context, psh Handle, id cadata.ID, buf []byte) (int, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return 0, err
	}
	set := n.getPSSet(psID)
	if exists, err := set.Exists(ctx, id); err != nil {
		return 0, err
	} else if !exists {
		return 0, cadata.ErrNotFound{Key: id}
	}
	store := n.mainStore()
	// TODO: need to handle cases where the blob is not in the local store.
	return store.Get(ctx, id, buf)
}

func (node *Node) List(ctx context.Context, psh Handle, span cadata.Span, ids []cadata.ID) (n int, err error) {
	psID, err := node.resolvePinSet(ctx, psh)
	if err != nil {
		return 0, err
	}
	set := node.getPSSet(psID)
	return set.List(ctx, span, ids)
}

func (n *Node) Exists(ctx context.Context, psh Handle, id cadata.ID) (bool, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return false, err
	}
	set := n.getPSSet(psID)
	return kv.ExistsUsingList(ctx, set, id)
}

func (n *Node) WaitOK(ctx context.Context, psh Handle) error {
	_, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	return n.ctrl.Flush(ctx, "local")
}

func (n *Node) MaxSize() int {
	return MaxSize
}

func (n *Node) Root() Handle {
	return n.dirServ.Root()
}

func (n *Node) mainStore() cadata.Store {
	return n.storeMux.open(n.getSystemSet([]string{"local-stores", "main"}))
}

func (n *Node) resolvePinSet(ctx context.Context, psh Handle) (dirserv.OID, error) {
	oid, data, err := n.dirServ.Get(ctx, psh, nil)
	if err != nil {
		return dirserv.NullOID, err
	}
	if !bytes.Equal(data, []byte("{}")) {
		return dirserv.NullOID, fmt.Errorf("object at %v is not s PinSet", oid)
	}
	return oid, nil
}

func (n *Node) getSystemSet(p []string) cadata.Set {
	return newSystemSet(n.dirServ, n.setMan, p)
}

func (n *Node) getPSSet(psID dirserv.OID) cadata.Set {
	return n.psSetMan.open(psID)
}

func (n *Node) peerActualStore(peerID bcnet.PeerID) cadata.Store {
	// TODO
	return cadata.NewVoid(Hash, MaxSize)
}

func (n *Node) validateModify(h Handle, name string) error {
	if strings.Contains(name, "/") {
		return errors.New("name cannot contain '/' or null characters")
	}
	if h.ID == dirserv.RootOID && name == "__system" {
		return errors.New("cannot modify " + name)
	}
	return nil
}
