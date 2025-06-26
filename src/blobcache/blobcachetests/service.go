// package blobcachetests provides a test suite for blobcache.Service.
package blobcachetests

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

// ServiceAPI tests an implementation of blobcache.Service.
func ServiceAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("Endpoint", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		_, err := s.Endpoint(ctx)
		require.NoError(t, err)
	})
	t.Run("CreateVolume", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		h, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, h)
	})
	t.Run("VolumeEmpty", func(t *testing.T) {
		// Check that an initial volume is empty.
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Mutate: false})
		require.NoError(t, err)
		require.NotNil(t, txh)
		buf := []byte{1, 2, 3} // arbitrary data
		err = s.Load(ctx, *txh, &buf)
		require.NoError(t, err)
		require.Equal(t, 0, len(buf))
	})
	t.Run("RootAEAD", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		volh2, err := s.CreateVolume(ctx, blobcache.VolumeSpec{
			HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			MaxSize:  1 << 21,
			Backend: blobcache.VolumeBackend[blobcache.Handle]{
				RootAEAD: &blobcache.VolumeBackend_RootAEAD[blobcache.Handle]{
					Inner:  *volh,
					Algo:   blobcache.AEAD_CHACHA20POLY1305,
					Secret: [32]byte{},
				},
			},
		})
		require.NoError(t, err)
		require.NoError(t, s.PutEntry(ctx, blobcache.RootHandle(), "test-name", *volh2))
		_, err = s.OpenAt(ctx, blobcache.RootHandle(), "test-name")
		require.NoError(t, err)
	})
	t.Run("Namespace", func(t *testing.T) {
		NamespaceAPI(t, mk)
	})
	t.Run("Tx", func(t *testing.T) {
		TxAPI(t, mk)
	})
}

func NamespaceAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("PutEntryOpen", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		err = s.PutEntry(ctx, blobcache.RootHandle(), "test-name", *volh)
		require.NoError(t, err)
		err = s.Drop(ctx, *volh)
		require.NoError(t, err)

		volh2, err := s.OpenAt(ctx, blobcache.RootHandle(), "test-name")
		require.NoError(t, err)
		require.Equal(t, volh.OID, volh2.OID)
	})
	t.Run("ListEmpty", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		names, err := s.ListNames(ctx, blobcache.RootHandle())
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("ListPutList", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		for i := 0; i < 10; i++ {
			err = s.PutEntry(ctx, blobcache.RootHandle(), fmt.Sprintf("test-name-%d", i), *volh)
			require.NoError(t, err)
		}
		names, err := s.ListNames(ctx, blobcache.RootHandle())
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			require.Contains(t, names, fmt.Sprintf("test-name-%d", i))
		}
	})
	t.Run("PutDelete", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		err = s.PutEntry(ctx, blobcache.RootHandle(), "test-name", *volh)
		require.NoError(t, err)
		names, err := s.ListNames(ctx, blobcache.RootHandle())
		require.NoError(t, err)
		require.Contains(t, names, "test-name")
		err = s.DeleteEntry(ctx, blobcache.RootHandle(), "test-name")
		require.NoError(t, err)
		names, err = s.ListNames(ctx, blobcache.RootHandle())
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("DeleteNonExistent", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		// Delets are idempotent, should not get an error.
		err = s.DeleteEntry(ctx, blobcache.RootHandle(), "test-name")
		require.NoError(t, err)
	})
}

func TxAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("TxAbortNoOp", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Mutate: false})
		require.NoError(t, err)
		require.NotNil(t, txh)
		err = s.Abort(ctx, *txh)
		require.NoError(t, err)
	})
	t.Run("TxCommit", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		require.NotNil(t, txh)
		err = s.Commit(ctx, *txh, []byte{1, 2, 3})
		require.NoError(t, err)
		require.NotNil(t, txh)
	})
	t.Run("PostExists", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		require.NotNil(t, txh)

		data := []byte{1, 2, 3}
		require.False(t, exists(t, s, *txh, blake3.Sum256(data)), "should not exist before post")
		cid := post(t, s, *txh, nil, data)
		require.True(t, exists(t, s, *txh, cid), "should exist after post")
	})
	t.Run("PostGet", func(t *testing.T) {
		s := mk(t)
		volh := createVolume(t, s)
		txh := beginTx(t, s, volh)

		data1 := []byte("hello world")
		cid := post(t, s, txh, nil, data1)
		data2 := get(t, s, txh, cid, nil, 100)
		require.Equal(t, data1, data2)
	})
}

func defaultVolumeSpec() blobcache.VolumeSpec {
	return blobcache.DefaultLocalSpec()
}

func createVolume(t testing.TB, s blobcache.Service) blobcache.Handle {
	ctx := testutil.Context(t)
	volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
	require.NoError(t, err)
	require.NotNil(t, volh)
	return *volh
}

func beginTx(t testing.TB, s blobcache.Service, volh blobcache.Handle) blobcache.Handle {
	ctx := testutil.Context(t)
	txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: true})
	require.NoError(t, err)
	require.NotNil(t, txh)
	return *txh
}

func post(t testing.TB, s blobcache.Service, txh blobcache.Handle, salt *blobcache.CID, data []byte) blobcache.CID {
	ctx := testutil.Context(t)
	cid, err := s.Post(ctx, txh, salt, data)
	require.NoError(t, err)
	return cid
}

func get(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID, salt *blobcache.CID, maxLen int) []byte {
	ctx := testutil.Context(t)
	buf := make([]byte, maxLen)
	n, err := s.Get(ctx, txh, cid, salt, buf)
	require.NoError(t, err)
	return buf[:n]
}

func exists(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID) bool {
	ctx := testutil.Context(t)
	yes, err := s.Exists(ctx, txh, cid)
	require.NoError(t, err)
	return yes
}

func Modify(t testing.TB, s blobcache.Service, volh blobcache.Handle, mutate bool, f func(tx *blobcache.Tx) ([]byte, error)) {
	ctx := testutil.Context(t)
	tx, err := blobcache.BeginTx(ctx, s, volh, blobcache.TxParams{Mutate: mutate})
	require.NoError(t, err)
	data, err := f(tx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx, data))
}
