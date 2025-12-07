package tries

import (
	"context"
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestPutGet(t *testing.T) {
	ctx := context.TODO()
	s := schema.NewTestStore(t)
	op := NewMachine(nil, blobcache.HashAlgo_BLAKE3_256.HashFunc())
	const N = 1000

	x, err := op.PostSlice(ctx, s, nil)
	require.NoError(t, err)
	// put
	for i := range N {
		buf := fmt.Appendf(nil, "test-value-%d", i)
		key := cadata.DefaultHash(buf)
		x, err = op.Put(ctx, s, *x, key[:], buf)
		require.NoError(t, err)
	}
	t.Logf("put %d blobs", s.Len())
	// get
	for i := range N {
		expected := fmt.Appendf(nil, "test-value-%d", i)
		key := cadata.DefaultHash(expected)
		var actual []byte
		found, err := op.Get(ctx, s, *x, key[:], &actual)
		assert.NoError(t, err, "while fetching key %q", key[:])
		assert.True(t, found)
		assert.Equal(t, expected, actual)
	}
}
