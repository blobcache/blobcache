package blobrouting

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	kv := &bcstate.MemKV{Capacity: 15}
	locus := make([]byte, 32)
	rt := NewKadRT(kv, locus)
	ctx := context.TODO()

	const N = 1000
	for i := 0; i < N; i++ {
		blobID := blobs.ID{}
		peerID := p2p.PeerID{}
		binary.BigEndian.PutUint64(blobID[:], uint64(i))
		binary.BigEndian.PutUint64(peerID[:], uint64(i))

		err := rt.Put(ctx, blobID, peerID, time.Now())
		require.Nil(t, err)

		ents, err := rt.Lookup(ctx, blobs.ID{})
		require.Nil(t, err)
		require.Len(t, ents, 1, "should still have locus after %d", i)
	}

	// check that we have entries close to us in keyspace
	for i := 0; i < 10; i++ {
		blobID := blobs.ID{}
		binary.BigEndian.PutUint64(blobID[:], uint64(i))
		ents, err := rt.Lookup(ctx, blobID)
		require.Nil(t, err)
		require.Len(t, ents, 1, "%v should have an entry", blobID)
	}

	// check that we evicted entries far away
	for i := 0; i < 10; i++ {
		j := N - i
		blobID := blobs.ID{}
		binary.BigEndian.PutUint64(blobID[:], uint64(j))
		peerIDs, err := rt.Lookup(ctx, blobID)
		require.Nil(t, err)
		require.Len(t, peerIDs, 0, "%v should not have an entry", blobID)
	}
}
