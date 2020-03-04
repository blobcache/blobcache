package blobrouting

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
	log "github.com/sirupsen/logrus"
)

var ErrShouldEvictThis = errors.New("no better entry to evict than this one")

type RouteTable interface {
	Put(context.Context, blobs.ID, p2p.PeerID) error
	Lookup(context.Context, blobs.ID) ([]p2p.PeerID, error)
	Query(context.Context, []byte) (tries.Trie, error)
}

// KadRT - Kademlia Route Table
type KadRT struct {
	kv          bckv.KV
	store       blobs.Store
	locus       []byte
	lastEvicted int

	mu   sync.RWMutex
	trie tries.Trie
}

func NewKadRT(kv bckv.KV, locus []byte) *KadRT {
	// save 1 spot for the root
	store := bckv.BlobAdapter(&bckv.FixedQuota{
		Store:    kv.Bucket("blobs"),
		Capacity: kv.SizeTotal() - 1,
	})
	rt := &KadRT{
		kv:    kv,
		store: store,
		locus: locus,
	}
	trie, err := rt.loadRoot()
	if err != nil {
		log.Error("error loading route table", err)
		log.Error("clearing route table")
		trie = tries.New(store)
	}
	rt.trie = trie
	return rt
}

func (rt *KadRT) Put(ctx context.Context, blobID blobs.ID, peerID p2p.PeerID) error {
	d := kademlia.XORBytes(rt.locus, blobID[:])
	lz := kademlia.Leading0s(d)
	key := makeKey(blobID, peerID)
	ctx = tries.CtxDeleteBlobs(ctx)

	rt.mu.Lock()
	defer rt.mu.Unlock()

	for i := 0; i < 10; i++ {
		err := rt.trie.Put(ctx, key, nil)
		if err == bckv.ErrFull {
			err2 := rt.evict(ctx, lz)
			if err2 == ErrShouldEvictThis {
				return nil
			}
			if err2 != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if err == nil {
			return rt.storeRoot(rt.trie)
		}
	}

	panic("could not insert")
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
	return getSubTrie(ctx, rt.trie, prefix)
}

func (rt *KadRT) WouldAccept() bitstrings.BitString {
	x := bitstrings.FromBytes(rt.lastEvicted, rt.locus)
	return x
}

func (rt *KadRT) evict(ctx context.Context, lz int) error {
	for i := 0; i < lz; i++ {
		locus := bitstrings.FromBytes(i, rt.locus)
		for _, c := range allBytes() {
			p := append(rt.locus[:len(rt.locus)-1], c)
			x := bitstrings.FromBytes(i, p)
			if locus.HasPrefix(&x) {
				continue
			}

			log.Println("deleting branch", x)
			err := rt.trie.DeleteBranch(ctx, p)
			log.Println("delete branch error", err)
			if err == tries.ErrBranchEmpty {
				continue
			} else {
				return err
			}
		}
	}
	return ErrShouldEvictThis
}

var routetableRoot = []byte("root")

func (rt *KadRT) loadRoot() (tries.Trie, error) {
	data, err := rt.kv.Get(routetableRoot)
	if err != nil {
		return nil, err
	}
	if len(data) < 1 {
		return tries.New(rt.store), nil
	}
	return tries.FromBytes(rt.store, data)
}

func (rt *KadRT) storeRoot(x tries.Trie) error {
	err := rt.kv.Put(routetableRoot, x.Marshal())
	if err == bckv.ErrFull {
		panic(err)
	}
	return err
}

func getSubTrie(ctx context.Context, t tries.Trie, prefix []byte) (tries.Trie, error) {
	log.Println("getSubTrie")
	for bytes.HasPrefix(prefix, t.GetPrefix()) {
		if len(prefix) == len(t.GetPrefix()) {
			return t, nil
		}
		child, err := t.GetChild(ctx, prefix[len(t.GetPrefix())])
		if err != nil {
			return nil, err
		}
		log.Printf("%x -> %v", child.GetPrefix(), child)
		t = child
	}
	return nil, errors.New("no trie found for that prefix")
}

func allBytes() (set []uint8) {
	for i := 0; i < 256; i++ {
		set = append(set, uint8(i))
	}
	return set
}
