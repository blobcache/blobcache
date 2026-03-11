package schematests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/bcns"
)

func TestLookup(t testing.TB, mk func(t testing.TB) (svc blobcache.Service, nsh blobcache.Handle), mkClient func(svc blobcache.Service) *bcns.Client, schemaSpec blobcache.SchemaSpec) {
	type TestCase struct {
		// Each entry in Volumes is a Volume containing a namespace.
		Volumes [][]bcns.Entry
		// Ok are the strings which should be able to be successfully looked up, and what volume they belong to.
		Ok map[string]int
		// Fail are strings which should return some error when looked up.
		Fail []string
	}
	tcs := []TestCase{
		// Single volume, exact match
		{
			Volumes: [][]bcns.Entry{
				{
					{Name: "a"},
				},
			},
			Ok:   map[string]int{"a": 0},
			Fail: []string{"b"},
		},
		// Single volume, multiple entries
		{
			Volumes: [][]bcns.Entry{
				{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Ok:   map[string]int{"a": 0, "b": 0},
			Fail: []string{"c"},
		},
		// Two volumes chained: vol0 has "a" -> vol1, vol1 has "b" (leaf)
		{
			Volumes: [][]bcns.Entry{
				{
					{Name: "a"},
				},
				{
					{Name: "b"},
				},
			},
			Ok:   map[string]int{"a": 0, "a/b": 1},
			Fail: []string{"b", "a/c"},
		},
		// Three volumes chained
		{
			Volumes: [][]bcns.Entry{
				{
					{Name: "a"},
				},
				{
					{Name: "b"},
				},
				{
					{Name: "c"},
				},
			},
			Ok:   map[string]int{"a": 0, "a/b": 1, "a/b/c": 2},
			Fail: []string{"b", "a/c", "a/b/d"},
		},
		// Empty namespace
		{
			Volumes: [][]bcns.Entry{
				{},
			},
			Fail: []string{"a", "a/b"},
		},
	}
	for i, tc := range tcs {
		t.(*testing.T).Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			ctx := testutil.Context(t)
			svc, nsh := mk(t)
			nsc := mkClient(svc)

			// Build the chain of namespace volumes.
			// Volume 0 is nsh from mk. Volumes 1..N are created as sub-namespaces
			// linked from the first entry of the previous volume.
			volHandles := make([]blobcache.Handle, len(tc.Volumes))
			volHandles[0] = nsh

			// Create chained namespace volumes (1..N) by linking first entry of vol[i-1] to vol[i].
			for vi := 1; vi < len(tc.Volumes); vi++ {
				subSpec := blobcache.DefaultLocalSpec()
				subSpec.Local.Schema = schemaSpec
				// The first entry of the previous volume points to this sub-namespace.
				name := tc.Volumes[vi-1][0].Name
				subH, err := nsc.CreateAt(ctx, volHandles[vi-1], name, subSpec)
				require.NoError(t, err)
				volHandles[vi] = *subH
			}

			// Populate leaf entries (entries that don't chain to the next volume).
			for vi, entries := range tc.Volumes {
				start := 0
				if vi < len(tc.Volumes)-1 {
					// First entry already points to the next namespace volume.
					start = 1
				}
				for _, ent := range entries[start:] {
					targetH := blobcachetests.CreateVolume(t, svc, nil, blobcache.DefaultLocalSpec())
					err := nsc.Put(ctx, volHandles[vi], ent.Name, targetH, blobcache.Action_ALL)
					require.NoError(t, err)
				}
			}

			// Test Ok cases.
			root := volHandles[0]
			for name := range tc.Ok {
				ent, err := bcns.Lookup(ctx, nsc, root, name)
				require.NoError(t, err, "expected lookup of %q to succeed", name)
				require.NotZero(t, ent.Target, "expected non-zero target for %q", name)
			}

			// Test Fail cases.
			for _, name := range tc.Fail {
				_, err := bcns.Lookup(ctx, nsc, root, name)
				require.Error(t, err, "expected lookup of %q to fail", name)
			}
		})
	}
}
