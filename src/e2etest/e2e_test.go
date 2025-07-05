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
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
)

// TestGLFS tests that blobcache integrates correctly with the Git-Like File System.
func TestGLFS(t *testing.T) {
	ctx := testutil.Context(t)
	svc := newTestService(t)
	volh, err := svc.CreateVolume(ctx, blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	blobcachetests.Modify(t, svc, *volh, func(tx *blobcache.Tx) ([]byte, error) {
		ref, err := glfs.PostBlob(ctx, tx, strings.NewReader("hello"))
		require.NoError(t, err)
		ref, err = glfs.PostTreeSlice(ctx, tx, []glfs.TreeEntry{
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
	ctx := testutil.Context(t)
	db := dbutil.OpenMemory()
	if err := bclocal.SetupDB(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := bclocal.New(bclocal.Env{DB: db})
	lis := testutil.Listen(t)
	go func() {
		if err := http.Serve(lis, &bchttp.Server{Service: s}); err != nil {
			t.Log(err)
		}
	}()
	return bchttp.NewClient(nil, fmt.Sprintf("http://%s", lis.Addr().String()))
}
