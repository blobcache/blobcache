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
		h, err := s.CreateVolume(ctx, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, h)
	})
	t.Run("VolumeEmpty", func(t *testing.T) {
		// Check that an initial volume is empty.
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultLocalSpec())
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
		volh, err := s.CreateVolume(ctx, defaultLocalSpec())
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

	// Run Tx test suit on local volume.
	t.Run("Local/Tx", func(t *testing.T) {
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			s := mk(t)
			volh, err := s.CreateVolume(ctx, blobcache.VolumeSpec{
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 21,
				Backend: blobcache.VolumeBackend[blobcache.Handle]{
					Local: &blobcache.VolumeBackend_Local{},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh)
			return s, *volh
		})
	})
	t.Run("RootAEAD/Tx", func(t *testing.T) {
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			ctx := testutil.Context(t)
			s := mk(t)
			volh1, err := s.CreateVolume(ctx, blobcache.VolumeSpec{
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 21,
				Backend: blobcache.VolumeBackend[blobcache.Handle]{
					Local: &blobcache.VolumeBackend_Local{},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh1)

			volh, err := s.CreateVolume(ctx, blobcache.VolumeSpec{
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 21,
				Backend: blobcache.VolumeBackend[blobcache.Handle]{
					RootAEAD: &blobcache.VolumeBackend_RootAEAD[blobcache.Handle]{
						Inner:  *volh1,
						Algo:   blobcache.AEAD_CHACHA20POLY1305,
						Secret: [32]byte{},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, volh)
			return s, *volh
		})
	})
}

func NamespaceAPI(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("PutEntryOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultLocalSpec())
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
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		names, err := s.ListNames(ctx, blobcache.RootHandle())
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("ListPutList", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultLocalSpec())
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
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultLocalSpec())
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
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		// Delets are idempotent, should not get an error.
		err = s.DeleteEntry(ctx, blobcache.RootHandle(), "test-name")
		require.NoError(t, err)
	})
}

func TxAPI(t *testing.T, mk func(t testing.TB) (blobcache.Service, blobcache.Handle)) {
	t.Run("TxAbortNoOp", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: false})
		require.NoError(t, err)
		require.NotNil(t, txh)
		err = s.Abort(ctx, *txh)
		require.NoError(t, err)
	})
	t.Run("TxCommit", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		require.NotNil(t, txh)
		err = s.Commit(ctx, *txh, []byte{1, 2, 3})
		require.NoError(t, err)
		require.NotNil(t, txh)
	})
	t.Run("PostExists", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		require.NotNil(t, txh)

		data := []byte{1, 2, 3}
		require.False(t, Exists(t, s, *txh, blake3.Sum256(data)), "should not exist before post")
		cid := Post(t, s, *txh, nil, data)
		require.True(t, Exists(t, s, *txh, cid), "should exist after post")
	})
	t.Run("PostGet", func(t *testing.T) {
		s, volh := mk(t)
		txh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})

		data1 := []byte("hello world")
		cid := Post(t, s, txh, nil, data1)
		data2 := Get(t, s, txh, cid, nil, 100)
		require.Equal(t, data1, data2)
	})
	t.Run("1WriterNReaders", func(t *testing.T) {
		s, volh := mk(t)

		wtxh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
		buf := Load(t, s, wtxh)
		require.Equal(t, []byte{}, buf)

		// Open 10 readers.
		rtxhs := make([]blobcache.Handle, 10)
		for i := range rtxhs {
			rtxhs[i] = BeginTx(t, s, volh, blobcache.TxParams{Mutate: false})
			defer Abort(t, s, rtxhs[i])
		}
		// commit the write transaction.
		root2 := []byte{1, 2, 3}
		Commit(t, s, wtxh, root2)

		// all of the readers should still see the empty blob.
		for _, rtxh := range rtxhs {
			buf := Load(t, s, rtxh)
			require.Equal(t, buf, []byte{})
		}

		// this reader should see the new root.
		rtxh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
		defer Abort(t, s, rtxh)
		buf2 := Load(t, s, rtxh)
		require.Equal(t, buf2, root2)
	})
	t.Run("WriteN", func(t *testing.T) {
		s, volh := mk(t)
		const N = 10
		for i := 0; i < N; i++ {
			func() {
				wtxh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
				Commit(t, s, wtxh, []byte{byte(i)})
			}()
		}
		txh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: false})
		defer Abort(t, s, txh)
		buf := Load(t, s, txh)
		require.Equal(t, []byte{9}, buf)
	})
}
