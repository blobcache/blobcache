package blobnet

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/kademlia"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

var ErrShouldEvictThis = errors.New("no better entry to evict than this one")

type BlobLocStore struct {
	locus []byte
	kv    bckv.KV

	lastEvictLz int32

	mu      sync.RWMutex
	buckets []*BLBucket
}

func newBlobLocStore(kv bckv.KV, locus []byte) *BlobLocStore {
	err := kv.ForEach([]byte{}, nil, func(k, v []byte) error {
		if v == nil {
			log.Debug("BlobLocStore: loaded bucket", string(k))
		} else {
			log.Warnf("BlobLocStore: non bucket key found %x", k)
		}
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	return &BlobLocStore{
		locus:       locus,
		kv:          kv,
		lastEvictLz: -1,
	}
}

func (s *BlobLocStore) Get(id blobs.ID) *BlobLoc {
	d := kademlia.XORBytes(s.locus, id[:])
	lz := kademlia.Leading0s(d)

	s.mu.RLock()
	if len(s.buckets) <= lz {
		s.mu.RUnlock()
		return nil
	}
	b := s.buckets[lz]
	s.mu.RUnlock()

	bl, err := b.Get(id)
	if err != nil {
		log.Error(err)
		return nil
	}
	return bl
}

func (s *BlobLocStore) Put(blobID blobs.ID, peerID p2p.PeerID) error {
	bl := &BlobLoc{
		BlobId: blobID[:],
		PeerId: peerID[:],
	}
	d := kademlia.XORBytes(s.locus, bl.BlobId)
	lz := kademlia.Leading0s(d)

	var b *BLBucket
	// check if we have the bucket with a read lock
	// upgrade to a write lock if we need to, and add the bucket
	s.mu.RLock()
	if len(s.buckets) <= lz {
		s.mu.RUnlock()
		s.mu.Lock()
		for len(s.buckets) <= lz {
			bitstr := bitstrings.FromBytes(lz, s.locus)
			b := &BLBucket{
				prefix: bitstr,
				kv:     s.kv.Bucket("bucket_" + bitstr.String()),
			}
			s.buckets = append(s.buckets, b)
		}
		b = s.buckets[lz]
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}

	err := b.Put(bl)
	switch {
	case err == bckv.ErrFull:
		if err := s.evict(lz); err != nil {
			return err
		}
		return s.Put(blobID, peerID)
	case err != nil:
		return err
	}

	return nil
}

func (s *BlobLocStore) WouldAccept() bitstrings.BitString {
	lz := atomic.LoadInt32(&s.lastEvictLz)
	if lz < 0 {
		return bitstrings.FromBytes(0, nil)
	}
	b := s.buckets[int(lz)]
	return b.prefix
}

func (s *BlobLocStore) evict(lz int) error {
	evictLz := -1
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, b := range s.buckets[:lz] {
		bloc, err := b.Evict()
		if err != nil {
			return err
		}
		if bloc != nil {
			evictLz = i
			break
		}
	}
	if evictLz == -1 {
		return ErrShouldEvictThis
	}

	atomic.StoreInt32(&s.lastEvictLz, int32(evictLz))
	return nil
}

type BLBucket struct {
	prefix bitstrings.BitString
	kv     bckv.KV
}

func (b *BLBucket) Put(bl *BlobLoc) error {
	// clear the blobID since it will be the key
	bl2 := *bl
	key := bl2.BlobId
	bl2.BlobId = nil

	data, err := proto.Marshal(bl)
	if err != nil {
		panic(err)
	}
	return b.kv.Put(key, data)
}

func (b *BLBucket) Get(id blobs.ID) (*BlobLoc, error) {
	data, err := b.kv.Get(id[:])
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	bl := &BlobLoc{}
	if err := proto.Unmarshal(data, bl); err != nil {
		return nil, err
	}
	bl.BlobId = id[:]
	return bl, nil
}

func (b *BLBucket) Evict() (*BlobLoc, error) {
	stopIter := errors.New("stop iteration")

	var key []byte
	var value []byte
	err := b.kv.ForEach([]byte{}, nil, func(k, v []byte) error {
		key = append([]byte{}, k...)
		value = append([]byte{}, v...)
		return stopIter
	})
	if err != nil && err != stopIter {
		return nil, err
	}
	if key == nil {
		// nothing to evict
		return nil, nil
	}

	bl := &BlobLoc{}
	if err := proto.Unmarshal(value, bl); err != nil {
		return nil, err
	}
	bl.BlobId = key

	if err := b.kv.Delete(key); err != nil {
		return nil, err
	}
	return bl, nil
}
