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

type LocalIndexer struct {
	store   Indexable
	eb      *trieevents.EventBus
	localID p2p.PeerID

	cf      context.CancelFunc
	scratch blobs.GetPostDelete

	mu     sync.RWMutex
	shards map[string]blobs.ID
}

type IndexerParams struct {
	Target   Indexable
	EventBus *trieevents.EventBus
	LocalID  p2p.PeerID
}

func NewLocalIndexer(params IndexerParams) *LocalIndexer {
	ctx, cf := context.WithCancel(context.Background())
	in := &LocalIndexer{
		store:   params.Target,
		eb:      params.EventBus,
		cf:      cf,
		scratch: blobs.NewMem(),
		shards:  map[string]blobs.ID{},
	}
	go in.run(ctx)
	return in
}

func (in *LocalIndexer) GetShard(prefix []byte) tries.Trie {
	in.mu.RLock()
	defer in.mu.RUnlock()

	// TODO: try longer and longer prefixes until the shard is found,
	// then get the trie root and traverse.
	for i := range prefix {
		id, exists := in.shards[string(prefix[:i+1])]
		if exists {
			trieBytes, err := in.scratch.Get(context.TODO(), id)
			if err != nil {
				log.Error(err)
				return nil
			}
			t, err := tries.FromBytes(in.scratch, trieBytes)
			if err != nil {
				log.Error(err)
				return nil
			}
			return t
		}
	}
	return nil
}

func (in *LocalIndexer) putShard(prefix []byte, id blobs.ID) {
	in.mu.Lock()
	in.shards[string(prefix)] = id
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
	scratch := w.li.scratch
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

			if err := w.putShard(ctx, trie); err != nil {
				log.Error(err)
				goto RESYNC
			}
		}
	}
}

func (w *worker) build(ctx context.Context, t tries.Trie, prefix []byte) error {
	ids := make([]blobs.ID, 1<<15)
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
		return nil
	case err != nil:
		return err
	}

	for _, blobID := range ids[:n] {
		peerID := w.li.localID
		key := makeKey(blobID, peerID)
		if err := t.Put(ctx, key, nil); err != nil {
			return err
		}
	}
	return w.putShard(ctx, t)
}

func (w *worker) putShard(ctx context.Context, t tries.Trie) error {
	data := t.Marshal()
	id, err := w.li.scratch.Post(ctx, data)
	if err != nil {
		return err
	}
	w.li.putShard(w.prefix, id)
	return nil
}
