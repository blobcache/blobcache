package tries

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestPutGet(t *testing.T) {
	ctx := context.TODO()
	s := cadata.NewMem(cadata.DefaultHash, 1<<20)
	op := NewOperator()
	const N = 1000

	x, err := op.PostSlice(ctx, s, nil)
	require.NoError(t, err)
	// put
	for i := 0; i < N; i++ {
		buf := []byte(fmt.Sprintf("test-value-%d", i))
		key := cadata.DefaultHash(buf)
		x, err = op.Put(ctx, s, *x, key[:], buf)
		require.NoError(t, err)
	}
	t.Logf("put %d blobs", s.Len())
	// get
	for i := 0; i < N; i++ {
		expected := []byte(fmt.Sprintf("test-value-%d", i))
		key := cadata.DefaultHash(expected)
		actual, err := op.Get(ctx, s, *x, key[:])
		assert.NoError(t, err, "while fetching key %q", key[:])
		assert.Equal(t, expected, actual)
	}
}
