package tries

import (
	"context"
	"fmt"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForEach(t *testing.T) {
	s := blobs.NewMem()
	ctx := context.TODO()

	x := New(s)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%02d", i)
		value := key
		err := x.Put(ctx, []byte(key), []byte(value))
		require.Nil(t, err)
	}

	visited := map[string]bool{}
	err := ForEach(ctx, x, []byte("5"), func(k, v []byte) error {
		visited[string(k)] = true
		return nil
	})
	assert.Nil(t, err)
	assert.Len(t, visited, 10)
}
