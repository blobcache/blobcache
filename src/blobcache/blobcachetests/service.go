// package blobcachetests provides a test suite for blobcache.Service.
package blobcachetests

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/simplens"
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
		t.SkipNow()
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

func SimpleNS(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("PutEntryOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := simplens.Client{Service: s}
		nsh := blobcache.Handle{}
		err = nsc.PutEntry(ctx, nsh, "test-name", *volh)
		require.NoError(t, err)
		err = s.Drop(ctx, *volh)
		require.NoError(t, err)

		volh2, err := nsc.OpenAt(ctx, nsh, "test-name", blobcache.Action_ALL)
		require.NoError(t, err)
		require.Equal(t, volh.OID, volh2.OID)
	})
	t.Run("ListEmpty", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		nsc := simplens.Client{Service: s}

		names, err := nsc.ListNames(ctx, blobcache.Handle{})
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("ListPutList", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := simplens.Client{Service: s}
		nsh := blobcache.Handle{}
		for i := 0; i < 10; i++ {
			err = nsc.PutEntry(ctx, nsh, fmt.Sprintf("test-name-%d", i), *volh)
			require.NoError(t, err)
		}
		names, err := nsc.ListNames(ctx, nsh)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			require.Contains(t, names, fmt.Sprintf("test-name-%d", i))
		}
	})
	t.Run("PutDelete", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := simplens.Client{Service: s}
		nsh, err := s.OpenAs(ctx, nil, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)
		err = nsc.PutEntry(ctx, *nsh, "test-name", *volh)
		require.NoError(t, err)
		names, err := nsc.ListNames(ctx, *nsh)
		require.NoError(t, err)
		require.Contains(t, names, "test-name")
		err = nsc.DeleteEntry(ctx, *nsh, "test-name")
		require.NoError(t, err)
		names, err = nsc.ListNames(ctx, *nsh)
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("DeleteNonExistent", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsh, err := s.OpenAs(ctx, nil, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)
		nsc := simplens.Client{Service: s}
		// Delets are idempotent, should not get an error.
		err = nsc.DeleteEntry(ctx, *nsh, "test-name")
		require.NoError(t, err)
	})
	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := simplens.Client{Service: s}
		require.NoError(t, nsc.PutEntry(ctx, blobcache.Handle{}, "vol1", *volh))

		nsh, err := s.OpenAs(ctx, nil, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)
		txh, err := s.BeginTx(ctx, *nsh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		data := []byte("this is not a valid CID")
		require.False(t, len(data) == len(blobcache.CID{}))
		require.Error(t, s.Save(ctx, *txh, data))
		require.NoError(t, s.Commit(ctx, *txh)) // could also abort here. Save failed so volume should be unchanged.
	})
	t.Run("Nested", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		// Open the root namespace
		nsc := simplens.Client{Service: s}
		rootNSh, err := s.OpenAs(ctx, nil, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)

		// Create 10 nested namespaces.
		ns1h := *rootNSh
		for i := 0; i < 10; i++ {
			subNSSpec := defaultLocalSpec()
			subNSSpec.Local.Schema = blobcache.Schema_SimpleNS
			ns2h, err := nsc.CreateAt(ctx, ns1h, "nested", subNSSpec)
			require.NoError(t, err)
			ns1h = *ns2h
		}

		ns1h = *rootNSh
		for i := 0; i < 10; i++ {
			ns2h, err := nsc.OpenAt(ctx, ns1h, "nested", blobcache.Action_ALL)
			require.NoError(t, err)
			ns1h = *ns2h
		}
	})
}
