package blobcachetests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/basicns"
)

// BasicNS tests that the BasicNS schema works on the Service.
func BasicNS(t *testing.T, mk func(t testing.TB) blobcache.Service) {
	t.Run("PutEntryOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		volh, err := s.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := basicns.Client{Service: s}
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
		nsc := basicns.Client{Service: s}

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
		nsc := basicns.Client{Service: s}
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
		nsc := basicns.Client{Service: s}
		nsh, err := s.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
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
		nsh, err := s.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)
		nsc := basicns.Client{Service: s}
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
		nsc := basicns.Client{Service: s}
		require.NoError(t, nsc.PutEntry(ctx, blobcache.Handle{}, "vol1", *volh))

		nsh, err := s.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
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
		nsc := basicns.Client{Service: s}
		rootNSh, err := s.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)

		// Create 10 nested namespaces.
		ns1h := *rootNSh
		for i := 0; i < 10; i++ {
			subNSSpec := defaultLocalSpec()
			subNSSpec.Local.Schema = blobcache.Schema_BasicNS
			ns2h, err := nsc.CreateAt(ctx, ns1h, "nested", subNSSpec)
			require.NoError(t, err)
			ns1h = *ns2h
		}

		ns1h = *rootNSh
		for i := 0; i < 10; i++ {
			require.NotZero(t, ns1h.Secret) // This would cause basicns to call OpenFiat instead of OpenFrom.
			ns2h, err := nsc.OpenAt(ctx, ns1h, "nested", blobcache.Action_ALL)
			require.NoError(t, err)
			ns1h = *ns2h
		}
	})
	t.Run("MultiOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s := mk(t)
		nsc := basicns.Client{Service: s}
		rootNSh, err := s.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err := nsc.CreateAt(ctx, *rootNSh, fmt.Sprintf("subvol-%d", i), defaultLocalSpec())
			require.NoError(t, err)
		}

		for i := 0; i < 10; i++ {
			_, err := nsc.OpenAt(ctx, *rootNSh, fmt.Sprintf("subvol-%d", i), blobcache.Action_ALL)
			require.NoError(t, err)
		}
	})
}
