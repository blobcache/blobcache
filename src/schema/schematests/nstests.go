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
		// The map describes the names to be created and which volumes they should link to.
		Volumes []map[string]int
		// Ok are the strings which should be able to be successfully looked up, and what volume they belong to.
		Ok map[string]int
		// Fail are strings which should return some error when looked up.
		Fail []string
	}
	tcs := []TestCase{
		// Single volume, exact match
		{
			Volumes: []map[string]int{
				{"a": 1},
			},
			Ok:   map[string]int{"a": 0},
			Fail: []string{"b"},
		},
		// Single volume, multiple entries
		{
			Volumes: []map[string]int{
				{"a": 1, "b": 2},
			},
			Ok:   map[string]int{"a": 0, "b": 0},
			Fail: []string{"c"},
		},
		// Two volumes chained: vol0 has "a" -> vol1, vol1 has "b" -> vol2
		{
			Volumes: []map[string]int{
				{"a": 1},
				{"b": 2},
			},
			Ok:   map[string]int{"a": 0, "a/b": 1},
			Fail: []string{"b", "a/c"},
		},
		// Three volumes chained
		{
			Volumes: []map[string]int{
				{"a": 1},
				{"b": 2},
				{"c": 3},
			},
			Ok:   map[string]int{"a": 0, "a/b": 1, "a/b/c": 2},
			Fail: []string{"b", "a/c", "a/b/d"},
		},
		// Entry with slash in name: vol0 has "a/b" -> vol1, vol1 has "c" -> vol2
		{
			Volumes: []map[string]int{
				{"a/b": 1},
				{"c": 2},
			},
			Ok:   map[string]int{"a/b": 0, "a/b/c": 1},
			Fail: []string{"a", "c", "a/c"},
		},
		// Three volumes with slashes: vol0 has "a/b" -> vol1, vol1 has "c/d" -> vol2, vol2 has "e" -> vol3
		{
			Volumes: []map[string]int{
				{"a/b": 1},
				{"c/d": 2},
				{"e": 3},
			},
			Ok:   map[string]int{"a/b": 0, "a/b/c/d": 1, "a/b/c/d/e": 2},
			Fail: []string{"a", "c", "a/b/c", "a/b/c/d/f"},
		},
		// Mixed: vol0 has "a/b" -> vol1, vol1 has "c" -> vol2, vol2 has "d" -> vol3
		{
			Volumes: []map[string]int{
				{"a/b": 1},
				{"c": 2},
				{"d": 3},
			},
			Ok:   map[string]int{"a/b": 0, "a/b/c": 1, "a/b/c/d": 2},
			Fail: []string{"a", "c", "a/c"},
		},
		// Empty namespace
		{
			Volumes: []map[string]int{
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

			// Determine how many volumes we need: the namespace volumes plus any
			// additional target volumes referenced by map values.
			maxVol := len(tc.Volumes) - 1
			for _, m := range tc.Volumes {
				for _, target := range m {
					if target > maxVol {
						maxVol = target
					}
				}
			}

			// Create all volumes upfront. Volume 0 is nsh from mk.
			volumes := make([]blobcache.Handle, maxVol+1)
			volumes[0] = nsh
			for vi := 1; vi <= maxVol; vi++ {
				spec := blobcache.DefaultLocalSpec()
				spec.Local.Schema = schemaSpec
				volumes[vi] = blobcachetests.CreateVolume(t, svc, nil, spec)
			}

			// Populate entries: for each namespace volume, create links from name -> target volume.
			for vi, m := range tc.Volumes {
				for name, target := range m {
					err := nsc.Put(ctx, volumes[vi], name, volumes[target], blobcache.Action_ALL)
					require.NoError(t, err)
				}
			}

			// Test Ok cases.
			root := volumes[0]
			for name := range tc.Ok {
				volh, err := bcns.Lookup(ctx, nsc, root, name)
				require.NoError(t, err, "expected lookup of %q to succeed", name)
				require.NotNil(t, volh, "expected non-nil handle for %q", name)
			}

			// Test Fail cases.
			for _, name := range tc.Fail {
				_, err := bcns.Lookup(ctx, nsc, root, name)
				require.Error(t, err, "expected lookup of %q to fail", name)
			}
		})
	}
}
