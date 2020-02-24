package blobrouting

import (
	"context"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/trieevents"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	log "github.com/sirupsen/logrus"
)

type Indexable interface {
	blobs.Lister
	blobs.Getter
}

type Shard struct {
	ID        blobs.ID
	TrieBytes []byte
}

type LocalIndexer struct {
	store   Indexable
	eb      *trieevents.EventBus
	localID p2p.PeerID

	cf context.CancelFunc
	mu sync.RWMutex
	m  map[string]Shard
}

func New(store Indexable, eb *trieevents.EventBus, localID p2p.PeerID) *LocalIndexer {
	ctx, cf := context.WithCancel(context.Background())
	in := &LocalIndexer{
		store: store,
		eb:    eb,
		cf:    cf,
	}
	go in.run(ctx)
	return in
}

func (in *LocalIndexer) GetShard(prefix []byte) *Shard {
	in.mu.RLock()
	defer in.mu.RUnlock()

	shard, exists := in.m[string(prefix)]
	if !exists {
		return nil
	}
	return &shard
}

func (in *LocalIndexer) putShard(prefix []byte, shard Shard) {
	in.mu.Lock()
	in.m[string(prefix)] = shard
	in.mu.Unlock()
}

func (in *LocalIndexer) Close() error {
	in.cf()
	return nil
}

func (in *LocalIndexer) run(ctx context.Context) {
	const N = 256
	wg := sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < N; i++ {
		prefix := []byte{byte(i)}
		w := worker{
			store:  in.store,
			eb:     in.eb,
			prefix: prefix,
			li:     in,
		}
		go func() {
			if err := w.run(ctx); err != nil {
				log.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

type worker struct {
	store  Indexable
	eb     *trieevents.EventBus
	prefix []byte

	li *LocalIndexer
}

func (w *worker) run(ctx context.Context) error {
	ch := make(chan blobs.ID)
	w.eb.Subscribe(w.prefix, ch)
	defer w.eb.Unsubscribe(ch)

RESYNC:
	scratch := blobs.NewMem()
	trie := tries.NewWithPrefix(scratch, w.prefix)
	if err := w.build(ctx, trie, w.prefix); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case id := <-ch:
			exists, err := w.store.Exists(ctx, id)
			if err != nil {
				log.Error(err)
			}

			key := makeKey(id, w.li.localID)
			if exists {
				if err := trie.Put(ctx, key, nil); err != nil {
					log.Error(err)
					goto RESYNC
				}
			} else {
				if err := trie.Delete(ctx, key); err != nil {
					log.Error(err)
					goto RESYNC
				}
			}
			data := trie.Marshal()
			shard := Shard{
				ID:        blobs.Hash(data),
				TrieBytes: data,
			}
			w.li.putShard(w.prefix, shard)
		}
	}
}

func (w *worker) build(ctx context.Context, t *tries.Trie, prefix []byte) error {
	ids := make([]blobs.ID, 2<<15)
	n, err := w.store.List(ctx, w.prefix, ids)
	switch {
	case err == blobs.ErrTooMany:
		for i := 0; i < 256; i++ {
			c := byte(i)
			prefix2 := append(prefix, c)
			if err := w.build(ctx, t, prefix2); err != nil {
				return err
			}
		}
	case err != nil:
		return err
	}

	for _, blobID := range ids[:n] {
		peerID := w.li.localID
		key := append(blobID[:], peerID[:]...)
		if err := t.Put(ctx, key, nil); err != nil {
			return err
		}
	}
	return nil
}

func (w *worker) putAllShards(ctx context.Context, t *tries.Trie) error {
	if t.IsParent() {
		for i := range t.Children {
			child, err := t.GetChild(ctx, byte(i))
			if err != nil {
				return err
			}
			if err := w.putAllShards(ctx, child); err != nil {
				return err
			}
		}
	} else {
		data := t.Marshal()
		shard := Shard{
			ID:        blobs.Hash(data),
			TrieBytes: data,
		}
		w.li.putShard(t.GetPrefix(), shard)
	}
	return nil
}
