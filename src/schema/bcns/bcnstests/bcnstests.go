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

func (n *NS) Fill(ctx context.Context, tx *bcsdk.Tx, nodes []blobcache.NodeID, vols [][]blobcache.OID) error {
	if n.Schema == nil {
		return fmt.Errorf("namespace schema is nil")
	}
	if vols == nil || len(vols) < 1 {
		return fmt.Errorf("no volumes provided")
	}
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	for _, ent := range n.Entries {
		if ent.Target < 0 || ent.Target >= len(vols[0]) {
			return fmt.Errorf("entry target out of range: %d", ent.Target)
		}
		lt, err := tx.Link(ctx, blobcache.Handle{OID: vols[0][ent.Target]}, ent.Rights)
		if err != nil {
			return err
		}
		root, err = n.Schema.NSPut(ctx, tx, root, bcns.Entry{
			Name:   ent.Name,
			Target: lt.Target,
			Rights: lt.Rights,
			Secret: lt.Secret,
		})
		if err != nil {
			return err
		}
	}
	return tx.Save(ctx, root)
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
		nsc := bcns.Client{Service: s, Schema: sch}
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
		nsc := bcns.Client{Service: s, Schema: sch}

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
		nsc := bcns.Client{Service: s, Schema: sch}
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
		nsc := bcns.Client{Service: s, Schema: sch}
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
		nsc := bcns.Client{Service: s, Schema: sch}
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
		nsc := bcns.Client{Service: s, Schema: sch}
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
		nsc := bcns.Client{Service: s, Schema: sch}

		// Create 10 nested namespaces.
		ns1h := nsh
		for i := 0; i < 10; i++ {
			subNSSpec := blobcache.DefaultLocalSpec()
			subNSSpec.Local.Schema = spec
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
		nsc := bcns.Client{Service: s, Schema: sch}
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
	})
}

func TestGC(t *testing.T, setup func(svcs []blobcache.Service)) {
	t.Parallel()
	ctx := testutil.Context(t)
	s, nsh := mk(t)
	nsc := bcns.Client{Service: s, Schema: sch}
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
}
