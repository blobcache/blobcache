package blobrouting

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/bitstrings"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
	log "github.com/sirupsen/logrus"
)

var ErrShouldEvictThis = errors.New("no better entry to evict than this one")

// KadRT - Kademlia Route Table
type KadRT struct {
	locus []byte

	mu          sync.RWMutex
	kv          bcstate.KV
	lastEvicted int
}

func NewKadRT(kv bcstate.KV, locus []byte) *KadRT {
	rt := &KadRT{
		kv:    kv,
		locus: locus,
	}
	return rt
}

func (rt *KadRT) Put(ctx context.Context, blobID blobs.ID, peerID p2p.PeerID, createdAt time.Time) error {
	d := kademlia.XORBytes(rt.locus, blobID[:])
	lz := kademlia.Leading0s(d)
	key := makeKey(blobID, peerID)

	rt.mu.Lock()
	defer rt.mu.Unlock()

	for i := 0; i < lz+1; i++ {
		var timeBytes [8]byte
		binary.BigEndian.PutUint64(timeBytes[:], uint64(createdAt.Unix()))
		err := rt.kv.Put(key, timeBytes[:])
		switch {
		case err == nil:
			return nil
		case err == bcstate.ErrFull:
			err2 := rt.evict(ctx, lz)
			if err2 == ErrShouldEvictThis {
				return nil
			}
			if err2 != nil {
				return err
			}
		default:
			return err
		}
	}
	panic("could not insert")
}

func (rt *KadRT) Lookup(ctx context.Context, blobID blobs.ID) ([]RTEntry, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	entries := []RTEntry{}
	err := rt.kv.ForEach(blobID[:], bcstate.PrefixEnd(blobID[:]), func(k, v []byte) error {
		blobID, peerID := splitKey(k)
		sightedAt, err := parseTime(v)
		if err != nil {
			return err
		}
		entry := RTEntry{
			BlobID:    blobID,
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

func (rt *KadRT) List(ctx context.Context, prefix []byte, ents []RTEntry) (int, error) {
	n := 0
	err := rt.kv.ForEach(prefix, bcstate.PrefixEnd(prefix), func(k, v []byte) error {
		sightedAt, err := parseTime(v)
		if err != nil {
			log.Errorf("invalid time. error: (%v). deleting entry", err)
			return rt.kv.Delete(k)
		}
		blobID, peerID := splitKey(k)
		ents[n] = RTEntry{
			BlobID:    blobID,
			PeerID:    peerID,
			SightedAt: *sightedAt,
		}
		n++
		if n >= len(ents) {
			return blobs.ErrTooMany
		}
		return nil
	})
	if err != nil {
		return -1, err
	}
	return n, nil
}

func (rt *KadRT) WouldAccept() bitstrings.BitString {
	x := bitstrings.FromBytes(rt.lastEvicted, rt.locus)
	return x
}

func (rt *KadRT) PruneExpired(ctx context.Context, prefix []byte, createdBefore time.Time) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.kv.ForEach(prefix, bcstate.PrefixEnd(prefix), func(k, v []byte) error {
		createdAt := time.Unix(int64(binary.BigEndian.Uint64(v)), 0)
		if createdAt.Before(createdBefore) {
			return rt.kv.Delete(k)
		}
		return nil
	})
}

func (rt *KadRT) evict(ctx context.Context, lz int) error {
	var stopIter = errors.New("stop iteration")
	for i := 0; i < lz; i++ {
		prefix := rt.locus[:i/8]
		locus := bitstrings.FromBytes(i, rt.locus)
		if err := rt.kv.ForEach(prefix, bcstate.PrefixEnd(prefix), func(k, v []byte) error {
			keybs := bitstrings.FromBytes(len(k)*8, k)
			if !bitstrings.HasPrefix(keybs, locus) {
				if err := rt.kv.Delete(k); err != nil {
					return err
				}
				return stopIter
			}
			return nil
		}); err != nil {
			if err == stopIter {
				return nil
			}
			return err
		}
		if i > rt.lastEvicted {
			rt.lastEvicted = i
		}
	}
	return ErrShouldEvictThis
}
