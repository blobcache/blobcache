package blobrouting

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
)

var ErrShouldEvictThis = errors.New("no better entry to evict than this one")

type RouteTable interface {
	Put(context.Context, blobs.ID, p2p.PeerID) error
	Lookup(context.Context, blobs.ID) ([]p2p.PeerID, error)
	Query(context.Context, []byte) (tries.Trie, error)
}

// KadRT - Kademlia Route Table
type KadRT struct {
	store blobs.GetPostDelete
	locus []byte

	mu   sync.RWMutex
	trie tries.Trie
}

func NewKadRT(store blobs.GetPostDelete, locus []byte) *KadRT {
	return &KadRT{
		store: store,
		locus: locus,
		trie:  tries.New(store),
	}
}

func (rt *KadRT) Put(ctx context.Context, blobID blobs.ID, peerID p2p.PeerID) error {
	d := kademlia.XORBytes(rt.locus, blobID[:])
	lz := kademlia.Leading0s(d)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if err := rt.evict(lz); err != nil {
		return err
	}
	key := makeKey(blobID, peerID)
	return rt.trie.Put(ctx, key, nil)
}

func (rt *KadRT) Lookup(ctx context.Context, blobID blobs.ID) ([]p2p.PeerID, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	peerIDs := []p2p.PeerID{}
	err := tries.ForEach(ctx, rt.trie, blobID[:], func(k, v []byte) error {
		_, peerID := splitKey(k)
		peerIDs = append(peerIDs, peerID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return peerIDs, nil
}

func (rt *KadRT) Query(ctx context.Context, prefix []byte) (tries.Trie, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	t := rt.trie
	for bytes.HasPrefix(prefix, t.GetPrefix()) {
		if len(prefix) == len(t.GetPrefix()) {
			return t, nil
		}
		child, err := t.GetChild(ctx, prefix[len(t.GetPrefix())])
		if err != nil {
			return nil, err
		}
		t = child
	}

	return nil, errors.New("no trie found for that prefix")
}

func (rt *KadRT) WouldAccept() bitstrings.BitString {
	return bitstrings.FromBytes(len(rt.locus)*8, rt.locus)
}

func (rt *KadRT) evict(lz int) error {
	for i := 0; i < lz; i++ {
		// TODO:
		return nil
	}
	return ErrShouldEvictThis
}
