package tries

import (
	"context"
	"fmt"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	const N = 1000

	ref, err := PostNode(ctx, s, New())
	require.Nil(t, err)
	// put
	for i := 0; i < N; i++ {
		buf := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(buf)
		ref, err = Put(ctx, s, *ref, id[:], buf)
		require.Nil(t, err)
	}
	t.Logf("put %d blobs", s.Len())
	// get
	for i := 0; i < N; i++ {
		expected := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(expected)
		actual, err := Get(ctx, s, *ref, id[:])
		assert.Nil(t, err)
		assert.Equal(t, expected, actual)
	}
}
