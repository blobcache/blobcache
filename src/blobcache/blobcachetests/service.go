// package blobcachetests provides a test suite for blobcache.Service.
package blobcachetests

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

// ServiceAPI tests an implementation of blobcache.Service.
func ServiceAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("Endpoint", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		_, err := s.Endpoint(ctx)
		require.NoError(t, err)
	})
	t.Run("CreateVolume", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		h, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, h)
	})
	t.Run("VolumeEmpty", func(t *testing.T) {
		t.Parallel()
		// Check that an initial volume is empty.
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Modify: false})
		require.NoError(t, err)
		require.NotNil(t, txh)
		buf := []byte{1, 2, 3} // arbitrary data
		err = s.Load(ctx, *txh, &buf)
		require.NoError(t, err)
		require.Equal(t, 0, len(buf))
	})
	t.Run("HashAlgo", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		for _, algo := range []blobcache.HashAlgo{
			blobcache.HashAlgo_BLAKE3_256,
			blobcache.HashAlgo_BLAKE2b_256,
			blobcache.HashAlgo_SHA2_256,
			blobcache.HashAlgo_SHA3_256,
			blobcache.HashAlgo_CSHAKE256,
		} {
			t.Run(string(algo), func(t *testing.T) {
				spec := defaultLocalSpec()
				spec.Local.HashAlgo = algo
				hf := algo.HashFunc()
				volh, err := s.CreateVolume(ctx, nil, spec)
				require.NoError(t, err)
				require.NotNil(t, volh)
				txh, err := s.BeginTx(ctx, *volh, blobcache.TxParams{Modify: true})
				require.NoError(t, err)
				require.NotNil(t, txh)
				defer s.Abort(ctx, *txh)

				data := []byte("hello world")
				expected := hf(nil, data)
				cid, err := s.Post(ctx, *txh, data, blobcache.PostOpts{})
				require.NoError(t, err)
				require.Equal(t, expected, cid)
			})
		}
	})
	t.Run("PostTooLarge", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		spec := defaultLocalSpec()
		spec.Local.MaxSize = 1024
		volh, err := s.CreateVolume(ctx, nil, spec)
		require.NoError(t, err)

		txh := BeginTx(t, s, *volh, blobcache.TxParams{Modify: true})
		defer s.Abort(ctx, txh)
		data := make([]byte, 1025)
		_, err = s.Post(ctx, txh, data, blobcache.PostOpts{})
		require.Error(t, err)
	})
	// Run Tx test suite on local volume.
	t.Run("Local/Tx", func(t *testing.T) {
		t.Parallel()
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			s := mk(t)
			volh, err := s.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Local: &blobcache.VolumeBackend_Local{
					HashAlgo: blobcache.HashAlgo_BLAKE3_256,
					MaxSize:  1 << 21,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh)
			return s, *volh
		})
	})
	t.Run("Vault/Tx", func(t *testing.T) {
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			s := mk(t)
			volh1, err := s.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Local: &blobcache.VolumeBackend_Local{
					HashAlgo: blobcache.HashAlgo_BLAKE3_256,
					MaxSize:  1 << 21,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh1)

			volh, err := s.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Vault: &blobcache.VolumeBackend_Vault[blobcache.Handle]{
					X:        *volh1,
					HashAlgo: blobcache.HashAlgo_BLAKE3_256,
					Secret:   [32]byte{},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh)
			return s, *volh
		})
	})
	t.Run("Queue/Memory", func(t *testing.T) {
		QueueAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			svc := mk(t)
			qh, err := svc.CreateQueue(ctx, nil, blobcache.QueueSpec{
				Memory: &blobcache.QueueBackend_Memory{
					MaxDepth: 16,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, qh)
			return svc, *qh
		})
	})
}

func TestManyBlobs(t *testing.T, singleTx bool, mk func(t testing.TB) blobcache.Service) {
	ctx := testutil.Context(t)
	s := mk(t)
	volh := CreateVolume(t, s, nil, defaultLocalSpec())
	require.NotNil(t, volh)

	const N = 1e4
	const dataSize = 128
	numWorkers := runtime.GOMAXPROCS(0)
	cids := make(chan blobcache.CID, N)

	// post all the blobs
	txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
	defer s.Abort(ctx, txh)
	wg := sync.WaitGroup{}
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, dataSize)
			for i := 0; i < N/numWorkers; i++ {
				binary.LittleEndian.PutUint64(data, uint64(i))
				cid := Post(t, s, txh, data, blobcache.PostOpts{})
				cids <- cid
			}
		}()
	}
	wg.Wait()
	close(cids)
	if !singleTx {
		Commit(t, s, txh)
	}

	t.Logf("posted %d blobs", int(N))

	// check that all the blobs exist
	if !singleTx {
		txh = BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
		defer s.Abort(ctx, txh)
	}
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			buf := make([]byte, dataSize)
			for i := 0; i < N/numWorkers; i++ {
				for cid := range cids {
					require.True(t, Exists(t, s, txh, cid))
					n := Get(t, s, txh, cid, buf, blobcache.GetOpts{})
					require.Equal(t, dataSize, n)
				}
			}
		}()
	}
	wg.Wait()
}
