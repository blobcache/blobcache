package tries

import (
	"context"
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
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

func TestIterate(t *testing.T) {
	ctx := context.TODO()
	mach := NewMachine(nil, blobcache.HashAlgo_BLAKE3_256.HashFunc())
	const N = 1000

	type testCase struct {
		Ents []Entry
	}
	tcs := []testCase{
		{}, // empty
		{
			Ents: []Entry{
				{
					Key:   []byte("key-0"),
					Value: []byte("test-value-0"),
				},
				{
					Key:   []byte("key-1"),
					Value: []byte("test-value-1"),
				},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			s := schema.NewTestStore(t)
			x, err := mach.NewEmpty(ctx, s)
			require.NoError(t, err)
			x, err = mach.BatchEdit(ctx, s, *x, func(yield func(Op) bool) {
				for _, ent := range tc.Ents {
					if !yield(OpPut(ent.Key, ent.Value)) {
						return
					}
				}
			})
			require.NoError(t, err)

			it := mach.NewIterator(s, *x, Span{})
			var count int
			require.NoError(t, streams.ForEach(ctx, it, func(ent Entry) error {
				assert.Equal(t, tc.Ents[count], ent, "at entry %d", count)
				count++
				return nil
			}))
			assert.Equal(t, len(tc.Ents), count)
		})
	}
}
