package blobrouting

import (
	"context"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/trieevents"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexer(t *testing.T) {
	store := blobs.NewMem()
	addRandom(store)
	li := NewLocalIndexer(store, trieevents.New(), p2p.ZeroPeerID())
	time.Sleep(time.Second * 3)

	assert.Nil(t, li.GetShard(nil))

	shard := li.GetShard([]byte{'\x00'})
	require.NotNil(t, shard)
	trie, err := tries.Parse(store, shard.TrieBytes)
	require.Nil(t, err)
	assert.Equal(t, []byte{'\x00'}, trie.GetPrefix())
	t.Log("len(li.m)", len(li.m))
	assert.True(t, len(li.m) >= 256)
}

func addRandom(store blobs.Poster) {
	for i := 0; i < 1<<17; i++ {
		data := make([]byte, 64)
		mrand.Read(data)
		store.Post(context.TODO(), data)
	}
}
