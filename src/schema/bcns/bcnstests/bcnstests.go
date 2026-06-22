// Package bcnstests implements a test suite for bcns
package bcnstests

import (
	"context"
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/stretchr/testify/require"
)

type (
	Scene       = blobcachetests.Scene
	Volume      = blobcachetests.Volume
	LocalVolume = blobcachetests.LocalVolume
)

// Entry is an namespace entry
type Entry struct {
	Name   string
	Rights blobcache.ActionSet
	// Target refers to a Volume on the same node by index
	Target int
}

var _ blobcachetests.Contents = &NS{}

// NS is namespace contents for a volume
type NS struct {
	Schema  bcns.Namespace
	Entries []Entry
}

func (n *NS) Fill(ctx context.Context, tx *bcsdk.Tx, vols []blobcache.Handle) error {
	tx2, err := bcns.NewFromTx(ctx, n.Schema, tx)
	if err != nil {
		return err
	}
	for _, ent := range n.Entries {
		if ent.Target < 0 || ent.Target >= len(vols) {
			return fmt.Errorf("entry target out of range: %d", ent.Target)
		}
		if err := tx2.Put(ctx, ent.Name, vols[ent.Target], ent.Rights); err != nil {
			return err
		}
	}
	return tx.Save(ctx, tx2.AppendCell(nil))
}

// TestSuite chekcs that the schema works on the
func TestSuite(t *testing.T, sch bcns.Namespace, spec blobcache.SchemaSpec, setup func(t testing.TB, svcs []blobcache.Service)) {
	mk := func(t testing.TB) (blobcache.Service, blobcache.Handle) {
		var svcs [1]blobcache.Service
		setup(t, svcs[:])
		vspec := blobcache.DefaultLocalSpec()
		vspec.Local.Schema = spec
		nsh := blobcachetests.CreateVolume(t, svcs[0], nil, vspec)
		return svcs[0], nsh
	}

	t.Run("PutEntryOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		volh := blobcachetests.CreateVolume(t, s, nil, blobcache.DefaultLocalSpec())
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)
		err := nsc.Put(ctx, "test-name", volh, blobcache.Action_ALL)
		require.NoError(t, err)
		err = s.Drop(ctx, volh)
		require.NoError(t, err)

		volh2, err := nsc.Open(ctx, "test-name", blobcache.Action_ALL)
		require.NoError(t, err)
		require.Equal(t, volh.OID, volh2.OID)
	})
	t.Run("ListEmpty", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)

		names, err := nsc.ListNames(ctx, "")
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
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)
		for i := 0; i < 10; i++ {
			err = nsc.Put(ctx, fmt.Sprintf("test-name-%d", i), *volh, blobcache.Action_ALL)
			require.NoError(t, err)
		}
		names, err := nsc.ListNames(ctx, "")
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
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)
		err := nsc.Put(ctx, "test-name", volh, blobcache.Action_ALL)
		require.NoError(t, err)
		names, err := nsc.ListNames(ctx, "")
		require.NoError(t, err)
		require.Contains(t, names, "test-name")
		err = nsc.Delete(ctx, "test-name")
		require.NoError(t, err)
		names, err = nsc.ListNames(ctx, "")
		require.NoError(t, err)
		require.Equal(t, []string{}, names)
	})
	t.Run("DeleteNonExistent", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)
		// Delets are idempotent, should not get an error.
		err := nsc.Delete(ctx, "test-name")
		require.NoError(t, err)
	})
	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		volh, err := s.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
		require.NoError(t, err)
		require.NotNil(t, volh)
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)
		require.NoError(t, nsc.Put(ctx, "vol1", *volh, blobcache.Action_ALL))

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
		// Create 10 nested namespaces.
		ns1h := nsh
		for i := 0; i < 10; i++ {
			subNSSpec := blobcache.DefaultLocalSpec()
			subNSSpec.Local.Schema = spec
			nsc2 := bcns.NewClient(s, ns1h.OID)
			nsc2.SetDefaultSchema(sch)
			ns2h, err := nsc2.CreateVolume(ctx, "nested", subNSSpec)
			require.NoError(t, err)
			ns1h = ns2h
		}

		ns1h = nsh
		for i := 0; i < 10; i++ {
			require.NotZero(t, ns1h.Secret) // This would cause to call OpenFiat instead of OpenFrom.
			nsc2 := bcns.NewClient(s, ns1h.OID)
			nsc2.SetDefaultSchema(sch)
			ns2h, err := nsc2.Open(ctx, "nested", blobcache.Action_ALL)
			require.NoError(t, err)
			ns1h = ns2h
		}
	})
	t.Run("MultiOpen", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, nsh := mk(t)
		nsc := bcns.NewClient(s, nsh.OID)
		nsc.SetDefaultSchema(sch)
		for i := 0; i < 10; i++ {
			_, err := nsc.CreateVolume(ctx, fmt.Sprintf("subvol-%d", i), blobcache.DefaultLocalSpec())
			require.NoError(t, err)
		}

		for i := 0; i < 10; i++ {
			_, err := nsc.Open(ctx, fmt.Sprintf("subvol-%d", i), blobcache.Action_ALL)
			require.NoError(t, err)
		}
	})
	t.Run("Open", func(t *testing.T) {
		TestOpen(t, mk, spec)
	})
	t.Run("CreateVolume", func(t *testing.T) {
		TestCreateVolume(t, sch, spec, setup)
	})
}
