package blobcache

import (
	"context"
	"encoding/binary"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/sirupsen/logrus"
	"lukechampine.com/blake3"

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
	DB              bcdb.DB
	Store           cadata.Store
	PrivateKey      p2p.PrivateKey
	Peers           []PeerInfo
	INET256         inet256.Service
	ExternalSources []Source
}

func NewMemParams() Params {
	return Params{
		DB:    bcdb.NewBadgerMemory(),
		Store: stores.NewMem(),
	}
}

type Node struct {
	db      bcdb.DB
	pinSets *pinSetStore

	locker  *calock.Locker
	store   Store
	sources []Source
}

func NewNode(params Params) *Node {
	pinSetStore := newPinSetStore(bcdb.NewPrefixed(params.DB, "pinsets\x00"))
	n := &Node{
		db:      params.DB,
		pinSets: pinSetStore,
		locker:  calock.NewLocker(),
		store:   params.Store,
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
	info, err := n.pinSets.Get(ctx, psID)
	if err != nil {
		return nil, err
	}
	return &PinSet{
		Status: StatusOK,
		Count:  info.Count,
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
	return n.cleanupStore(ctx)
}

// Add implements Service
func (node *Node) Add(ctx context.Context, psh PinSetHandle, id cadata.ID) error {
	psID, err := node.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	if uf, err := node.locker.LockAdd(ctx, id); err != nil {
		return err
	} else {
		defer uf()
	}
	if exists, err := node.store.Exists(ctx, id); err != nil {
		return err
	} else if exists {
		return node.pinSets.Pin(ctx, psID, id)
	}
	buf := make([]byte, MaxSize)
	for _, source := range node.sources {
		n, err := source.Get(ctx, id, buf)
		if cadata.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err := node.pinSets.Pin(ctx, psID, id); err != nil {
			return err
		}
		_, err = node.store.Post(ctx, buf[:n])
		return err
	}
	return ErrDataNotFound
}

func (n *Node) Delete(ctx context.Context, psh PinSetHandle, id cadata.ID) error {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return err
	}
	if uf, err := n.locker.LockDelete(ctx, id); err != nil {
		return err
	} else {
		defer uf()
	}
	count, err := n.pinSets.Unpin(ctx, psID, id)
	if err != nil {
		return err
	}
	if count == 0 {
		return n.store.Delete(ctx, id)
	}
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
	id := n.store.Hash(data)
	if uf, err := n.locker.LockAdd(ctx, id); err != nil {
		return cadata.ID{}, err
	} else {
		defer uf()
	}
	if err := n.pinSets.Pin(ctx, psID, id); err != nil {
		return cadata.ID{}, err
	}
	_, err = n.store.Post(ctx, data)
	return id, err
}

func (n *Node) Get(ctx context.Context, psh PinSetHandle, id cadata.ID, buf []byte) (int, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return 0, err
	}
	if uf, err := n.locker.LockAdd(ctx, id); err != nil {
		return 0, err
	} else {
		defer uf()
	}
	if exists, err := n.pinSets.Exists(ctx, psID, id); err != nil {
		return 0, err
	} else if !exists {
		return 0, cadata.ErrNotFound
	}
	return n.store.Get(ctx, id, buf)
}

func (node *Node) List(ctx context.Context, psh PinSetHandle, prefix []byte, ids []cadata.ID) (n int, err error) {
	psID, err := node.resolvePinSet(ctx, psh)
	if err != nil {
		return 0, err
	}
	return node.pinSets.List(ctx, psID, prefix, ids)
}

func (n *Node) Exists(ctx context.Context, psh PinSetHandle, id cadata.ID) (bool, error) {
	psID, err := n.resolvePinSet(ctx, psh)
	if err != nil {
		return false, err
	}
	return n.pinSets.Exists(ctx, psID, id)
}

func (n *Node) WaitOK(ctx context.Context, psh PinSetHandle) error {
	return nil
}

func (n *Node) MaxSize() int {
	return MaxSize
}

// cleanupStore deletes unreferenced blobs in the store
func (n *Node) cleanupStore(ctx context.Context) error {
	return cadata.ForEach(ctx, n.store, func(id cadata.ID) error {
		count, err := n.pinSets.GetRefCount(ctx, id)
		if err != nil {
			return err
		}
		if count == 0 {
			if uf, err := n.locker.LockDelete(ctx, id); err != nil {
				return err
			} else {
				defer uf()
			}
			return n.store.Delete(ctx, id)
		}
		return nil
	})
}

func (n *Node) resolvePinSet(ctx context.Context, psh PinSetHandle) (PinSetID, error) {
	key := handleKey(psh)
	logrus.Info("resolved handle ", key)
	// TODO: lookup in db
	return psh.ID, nil
}

func handleKey(psh PinSetHandle) (ret [16]byte) {
	h := blake3.New(32, nil)
	binary.Write(h, binary.BigEndian, psh.ID)
	h.Write(psh.Secret[:])
	sum := h.Sum(nil)
	copy(ret[:], sum)
	return ret
}
