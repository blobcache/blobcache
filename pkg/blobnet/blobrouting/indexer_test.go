package blobrouting

import (
	"context"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/trieevents"
	"github.com/brendoncarroll/go-p2p"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexer(t *testing.T) {
	store := blobs.NewMem()
	addRandom(store)
	li := NewLocalIndexer(IndexerParams{
		Target:   store,
		EventBus: trieevents.New(),
		LocalID:  p2p.ZeroPeerID(),
	})

	numShards := pollShardCount(li, 256)
	trie := li.GetShard([]byte{'\x00'})
	require.NotNil(t, trie)
	assert.Equal(t, []byte{'\x00'}, trie.GetPrefix())
	assert.Equal(t, numShards, 256)
}

func addRandom(store blobs.Poster) {
	for i := 0; i < 1<<16; i++ {
		data := make([]byte, 64)
		mrand.Read(data)
		store.Post(context.TODO(), data)
	}
}

func pollShardCount(li *LocalIndexer, numShards int) int {
	deadline := time.Now().Add(10 * time.Second)
	for {
		li.mu.RLock()
		l := len(li.shards)
		li.mu.RUnlock()
		if l >= numShards {
			return l
		}
		if time.Now().After(deadline) {
			return l
		}
		time.Sleep(100 * time.Millisecond)
	}
}
