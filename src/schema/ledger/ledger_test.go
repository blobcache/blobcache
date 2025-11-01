package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/merklelog"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	type state = autoMarshal[int]
	x := Root[state]{
		History: merklelog.State{Levels: []merklelog.CID{}},
		State:   autoMarshal[int]{X: 1},
	}
	parseState := func(data []byte) (state, error) {
		var x state
		if err := x.Unmarshal(data); err != nil {
			return state{}, err
		}
		return x, nil
	}
	y, err := Parse(x.Marshal(nil), parseState)
	require.NoError(t, err)
	require.Equal(t, x, y)
}

func TestCountUp(t *testing.T) {
	ctx := testutil.Context(t)
	s := schema.NewMem(blobcache.HashAlgo_BLAKE3_256.HashFunc(), 1<<21)
	mach := Machine[autoMarshal[int]]{
		HashAlgo:   blobcache.HashAlgo_BLAKE3_256,
		ParseState: autoParse[int],
		Verify: func(ctx context.Context, s schema.RO, prev, next autoMarshal[int]) error {
			if prev.X+1 != next.X {
				return fmt.Errorf("%d cannot follow %d", next.X, prev.X)
			}
			return nil
		},
	}
	initRoot := mach.Initial(autoMarshal[int]{X: 0})
	root := initRoot
	for i := 1; i < 10; i++ {
		root2, err := mach.AndThen(ctx, s, root, autoMarshal[int]{X: i})
		require.NoError(t, err)
		root = root2
	}

	require.NoError(t, mach.Validate(ctx, s, initRoot, root))
}

type autoMarshal[T any] struct {
	X T
}

func (s autoMarshal[T]) Marshal(out []byte) []byte {
	data, err := json.Marshal(s.X)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (s *autoMarshal[T]) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &s.X)
}

func autoParse[T any](data []byte) (autoMarshal[T], error) {
	var auto autoMarshal[T]
	return auto, auto.Unmarshal(data)
}
