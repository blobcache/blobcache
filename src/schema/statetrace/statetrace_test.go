package statetrace

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/merklelog"
	"blobcache.io/blobcache/src/schema/schematests"
	"github.com/stretchr/testify/require"
)

func TestTx(t *testing.T) {
	vspec := blobcache.VolumeBackend_Local{
		Schema: blobcache.SchemaSpec{
			Name: SchemaName,
			Params: jsonMarshal(Spec{
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			}),
		},
		HashAlgo: blobcache.HashAlgo_BLAKE3_256,
		MaxSize:  1 << 20,
	}
	schs := map[blobcache.SchemaName]schema.Constructor{
		blobcache.Schema_NONE: schema.NoneConstructor,
		SchemaName:            Constructor,
	}
	svc, volh := schematests.Setup(t, schs, vspec)
	txh := blobcachetests.BeginTx(t, svc, volh, blobcache.TxParams{Modify: true})
	ctx := testutil.Context(t)
	defer svc.Abort(ctx, txh)
	blobcachetests.Save(t, svc, txh, Root[autoMarshal[int]]{}.Marshal(nil))
	blobcachetests.Commit(t, svc, txh)
}

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

func jsonMarshal(x any) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}
