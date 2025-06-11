// package blobcachetests provides a test suite  for blobcache.Service.
package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

// ServiceAPI tests an implementation of blobcache.Service.
func ServiceAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
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
		txh, err := s.BeginTx(ctx, *volh, false)
		require.NoError(t, err)
		require.NotNil(t, txh)
		buf := []byte{1, 2, 3} // arbitrary data
		err = s.Load(ctx, *txh, &buf)
		require.NoError(t, err)
		require.Equal(t, 0, len(buf))
	})
	t.Run("Anchor", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		err = s.Anchor(ctx, *volh)
		require.NoError(t, err)
		err = s.Drop(ctx, *volh)
		require.NoError(t, err)
	})
	t.Run("Tx", func(t *testing.T) {
		TxAPI(t, mk)
	})
}

func TxAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("TxAbortNoOp", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		txh, err := s.BeginTx(ctx, *volh, false)
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
		txh, err := s.BeginTx(ctx, *volh, true)
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
		txh, err := s.BeginTx(ctx, *volh, true)
		require.NoError(t, err)
		require.NotNil(t, txh)

		data := []byte{1, 2, 3}
		require.False(t, exists(t, s, *txh, blake3.Sum256(data)), "should not exist before post")
		cid := post(t, s, *txh, data)
		require.True(t, exists(t, s, *txh, cid), "should exist after post")
	})
}

func defaultVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{HashAlgo: blobcache.HashAlgo_BLAKE3_256}
}

func post(t testing.TB, s blobcache.Service, txh blobcache.Handle, data []byte) blobcache.CID {
	ctx := testutil.Context(t)
	cid, err := s.Post(ctx, txh, data)
	require.NoError(t, err)
	return cid
}

func exists(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID) bool {
	ctx := testutil.Context(t)
	yes, err := s.Exists(ctx, txh, cid)
	require.NoError(t, err)
	return yes
}
