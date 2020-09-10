package blobrouting

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/bitstrings"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
	log "github.com/sirupsen/logrus"
)

var ErrShouldEvictThis = errors.New("no better entry to evict than this one")

// KadRT - Kademlia Route Table
type KadRT struct {
	locus []byte

	cell        bcstate.Cell
	store       blobs.Store
	lastEvicted int

	mu   sync.RWMutex
	trie tries.Trie
}

func NewKadRT(root bcstate.Cell, store blobs.Store, locus []byte) *KadRT {
	rt := &KadRT{
		cell:  root,
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
	if err := rt.GC(context.Background()); err != nil {
		log.Error(err)
	}
	return rt
}

func (rt *KadRT) Put(ctx context.Context, blobID blobs.ID, peerID p2p.PeerID, createdAt time.Time) error {
	d := kademlia.XORBytes(rt.locus, blobID[:])
	lz := kademlia.Leading0s(d)
	key := makeKey(blobID, peerID)
	ctx = tries.CtxDeleteBlobs(ctx)

	rt.mu.Lock()
	defer rt.mu.Unlock()

	for i := 0; i < 10; i++ {
		var timeBytes [8]byte
		binary.BigEndian.PutUint64(timeBytes[:], uint64(createdAt.Unix()))
		err := rt.trie.Put(ctx, key, timeBytes[:])
		if err == bcstate.ErrFull {
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

func (rt *KadRT) Lookup(ctx context.Context, blobID blobs.ID) ([]RTEntry, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	entries := []RTEntry{}
	err := tries.ForEach(ctx, rt.trie, blobID[:], func(k, v []byte) error {
		_, peerID := splitKey(k)
		sightedAt, err := parseTime(v)
		if err != nil {
			return err
		}
		entry := RTEntry{
			PeerID:    peerID,
			SightedAt: *sightedAt,
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (rt *KadRT) GetTrie(ctx context.Context, prefix []byte) (tries.Trie, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return getSubTrie(ctx, rt.trie, prefix)
}

func (rt *KadRT) WouldAccept() bitstrings.BitString {
	x := bitstrings.FromBytes(rt.lastEvicted, rt.locus)
	return x
}

func (rt *KadRT) GC(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	t, err := rt.loadRoot()
	if err != nil {
		return err
	}
	return tries.GCStore(ctx, rt.store, t)
}

func (rt *KadRT) PruneExpired(ctx context.Context, createdBefore time.Time) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	t, err := rt.loadRoot()
	if err != nil {
		return err
	}
	return tries.ForEach(ctx, t, nil, func(k, v []byte) error {
		createdAt := time.Unix(int64(binary.BigEndian.Uint64(v)), 0)
		if createdAt.Before(createdBefore) {
			return t.Delete(ctx, k)
		}
		return nil
	})
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

			err := rt.trie.DeleteBranch(ctx, p)
			if err == tries.ErrBranchEmpty {
				continue
			} else {
				return err
			}
		}
	}
	return ErrShouldEvictThis
}

func (rt *KadRT) loadRoot() (tries.Trie, error) {
	var data []byte
	err := rt.cell.LoadF(func(v []byte) error {
		data = append([]byte{}, v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(data) < 1 {
		return tries.New(rt.store), nil
	}
	return tries.FromBytes(rt.store, data)
}

func (rt *KadRT) storeRoot(x tries.Trie) error {
	err := rt.cell.Store(x.Marshal())
	if err == bcstate.ErrFull {
		panic(err)
	}
	return err
}

func getSubTrie(ctx context.Context, t tries.Trie, prefix []byte) (tries.Trie, error) {
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
