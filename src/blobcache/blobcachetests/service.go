// package blobcachetests provides a test suite for blobcache.Service.
package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
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
		h, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, h)
	})
	t.Run("VolumeEmpty", func(t *testing.T) {
		// Check that an initial volume is empty.
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
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
	t.Run("HashAlgo", func(t *testing.T) {
		ctx := testutil.Context(t)
		s := mk(t)
		for _, algo := range []blobcache.HashAlgo{
			blobcache.HashAlgo_BLAKE3_256,
			blobcache.HashAlgo_BLAKE2b_256,
			blobcache.HashAlgo_SHA2_256,
			blobcache.HashAlgo_SHA3_256,
		} {
			t.Run(string(algo), func(t *testing.T) {
				spec := defaultLocalSpec()
				spec.Local.HashAlgo = algo
				hf := algo.HashFunc()
				volh, err := s.CreateVolume(ctx, nil, spec)
				require.NoError(t, err)
				require.NotNil(t, volh)
				txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Mutate: true})
				require.NoError(t, err)
				require.NotNil(t, txh)
				defer s.Abort(ctx, *txh)

				data := []byte("hello world")
				expected := hf(nil, data)
				cid, err := s.Post(ctx, *txh, nil, data)
				require.NoError(t, err)
				require.Equal(t, expected, cid)
			})
		}
	})
	t.Run("SimpleNS", func(t *testing.T) {
		SimpleNS(t, mk)
	})
	// Run Tx test suite on local volume.
	t.Run("Local/Tx", func(t *testing.T) {
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			s := mk(t)
			volh, err := s.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Local: &blobcache.VolumeBackend_Local{
					VolumeParams: blobcache.VolumeParams{
						HashAlgo: blobcache.HashAlgo_BLAKE3_256,
						MaxSize:  1 << 21,
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh)
			return s, *volh
		})
	})
	t.Run("Vault/Tx", func(t *testing.T) {
		t.SkipNow() // TODO: re-enable after GC transactions are working.
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			s := mk(t)
			volh1, err := s.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Local: &blobcache.VolumeBackend_Local{
					VolumeParams: blobcache.VolumeParams{
						HashAlgo: blobcache.HashAlgo_BLAKE3_256,
						MaxSize:  1 << 21,
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh1)

			volh, err := s.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Vault: &blobcache.VolumeBackend_Vault[blobcache.Handle]{
					Inner:  *volh1,
					Secret: [32]byte{},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh)
			return s, *volh
		})
	})
}
