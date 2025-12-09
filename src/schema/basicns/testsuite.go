package basicns

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
)

// tests that the schema works on the Service.
func TestSuite(t *testing.T, mk func(t testing.TB) (svc blobcache.Service, nsh blobcache.Handle)) {
	t.Run("PutEntryOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		volh := blobcachetests.CreateVolume(t, s, nil, blobcache.DefaultLocalSpec())
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		err := nsc.Put(ctx, nsh, "test-name", volh, blobcache.Action_ALL)
		require.NoError(t, err)
		err = s.Drop(ctx, volh)
		require.NoError(t, err)

		volh2, err := nsc.OpenAt(ctx, nsh, "test-name", blobcache.Action_ALL)
		require.NoError(t, err)
		require.Equal(t, volh.OID, volh2.OID)
	})
	t.Run("ListEmpty", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := schema.NSClient{Service: s, Schema: Schema{}}

		names, err := nsc.ListNames(ctx, nsh)
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("ListPutList", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		volh, err := s.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		for i := 0; i < 10; i++ {
			err = nsc.Put(ctx, nsh, fmt.Sprintf("test-name-%d", i), *volh, blobcache.Action_ALL)
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
		s, nsh := mk(t)
		volh := blobcachetests.CreateVolume(t, s, nil, blobcache.DefaultLocalSpec())
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		err := nsc.Put(ctx, nsh, "test-name", volh, blobcache.Action_ALL)
		require.NoError(t, err)
		names, err := nsc.ListNames(ctx, nsh)
		require.NoError(t, err)
		require.Contains(t, names, "test-name")
		err = nsc.Delete(ctx, nsh, "test-name")
		require.NoError(t, err)
		names, err = nsc.ListNames(ctx, nsh)
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("DeleteNonExistent", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		// Delets are idempotent, should not get an error.
		err := nsc.Delete(ctx, nsh, "test-name")
		require.NoError(t, err)
	})
	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		volh, err := s.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		require.NoError(t, nsc.Put(ctx, blobcache.Handle{}, "vol1", *volh, blobcache.Action_ALL))

		txh, err := s.BeginTx(ctx, nsh, blobcache.TxParams{Modify: true})
		require.NoError(t, err)
		data := []byte("this is not a valid CID")
		require.False(t, len(data) == len(blobcache.CID{}))
		require.Error(t, s.Save(ctx, *txh, data))
		require.NoError(t, s.Commit(ctx, *txh)) // could also abort here. Save failed so volume should be unchanged.
	})
	t.Run("Nested", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		// Open the root namespace
		nsc := schema.NSClient{Service: s, Schema: Schema{}}

		// Create 10 nested namespaces.
		ns1h := nsh
		for i := 0; i < 10; i++ {
			subNSSpec := blobcache.DefaultLocalSpec()
			subNSSpec.Local.Schema = blobcache.SchemaSpec{Name: SchemaName}
			ns2h, err := nsc.CreateAt(ctx, ns1h, "nested", subNSSpec)
			require.NoError(t, err)
			ns1h = *ns2h
		}

		ns1h = nsh
		for i := 0; i < 10; i++ {
			require.NotZero(t, ns1h.Secret) // This would cause to call OpenFiat instead of OpenFrom.
			ns2h, err := nsc.OpenAt(ctx, ns1h, "nested", blobcache.Action_ALL)
			require.NoError(t, err)
			ns1h = *ns2h
		}
	})
	t.Run("MultiOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		for i := 0; i < 10; i++ {
			_, err := nsc.CreateAt(ctx, nsh, fmt.Sprintf("subvol-%d", i), blobcache.DefaultLocalSpec())
			require.NoError(t, err)
		}

		for i := 0; i < 10; i++ {
			_, err := nsc.OpenAt(ctx, nsh, fmt.Sprintf("subvol-%d", i), blobcache.Action_ALL)
			require.NoError(t, err)
		}
	})
	t.Run("GC", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := schema.NSClient{Service: s, Schema: Schema{}}
		mkName := func(x int) string {
			return fmt.Sprintf("name-%d", x)
		}
		// add some subvolumes
		for i := range 10 {
			name := mkName(i)
			_, err := nsc.CreateAt(ctx, nsh, name, blobcache.DefaultLocalSpec())
			require.NoError(t, err)
		}
		// run GC
		require.NoError(t, GC(ctx, s, nsh))
		// open subvolumes
		for i := range 10 {
			name := mkName(i)
			_, err := nsc.OpenAt(ctx, nsh, name, blobcache.Action_ALL)
			require.NoError(t, err)
		}
		// delete even entries
		for i := 0; i < 10; i += 2 {
			name := mkName(i)
			require.NoError(t, nsc.Delete(ctx, nsh, name))
		}
		// GC
		require.NoError(t, GC(ctx, s, nsh))
		for i := range 10 {
			name := mkName(i)
			_, err := nsc.OpenAt(ctx, nsh, name, blobcache.Action_ALL)
			if i%2 == 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		}
	})
}
