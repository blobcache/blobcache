package e2etest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"blobcache.io/glfs"
	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	_ "blobcache.io/blobcache/src/schema/basicns"
	bcglfs "blobcache.io/blobcache/src/schema/glfs"
)

// TestGLFS tests that blobcache integrates correctly with the Git-Like File System.
func TestGLFS(t *testing.T) {
	ctx := testutil.Context(t)
	svc := newTestService(t)
	volh, err := svc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	blobcachetests.Modify(t, svc, *volh, func(tx *blobcache.Tx) ([]byte, error) {
		ref, err := glfs.PostBlob(ctx, tx, strings.NewReader("hello"))
		require.NoError(t, err)
		pea := &bcglfs.PostExistAdapter{WO: tx}
		ref, err = glfs.PostTreeSlice(ctx, pea, []glfs.TreeEntry{
			{Name: "hello.txt", Ref: *ref},
		})
		require.NoError(t, err)
		return jsonMarshal(ref), nil
	})

	tx, err := blobcache.BeginTx(ctx, svc, *volh, blobcache.TxParams{})
	require.NoError(t, err)
	var root []byte
	require.NoError(t, tx.Load(ctx, &root))
	ref := new(glfs.Ref)
	require.NoError(t, json.Unmarshal(root, ref))
	ref, err = glfs.GetAtPath(ctx, tx, *ref, "hello.txt")
	require.NoError(t, err)
	data, err := glfs.GetBlobBytes(ctx, tx, *ref, 100)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
}

func jsonMarshal(x any) []byte {
	buf, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return buf
}

func newTestService(t testing.TB) blobcache.Service {
	s := bclocal.NewTestService(t)
	lis := testutil.Listen(t)
	go func() {
		http.Serve(lis, &bchttp.Server{Service: s})
	}()
	return bchttp.NewClient(nil, fmt.Sprintf("http://%s", lis.Addr().String()))
}
