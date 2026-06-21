package bcnstests

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/stretchr/testify/require"
)

// TestCreateVolumeAt runs a test suite for bcns.CreateVolumeAt.
// setup must fill svcs with working services, which can communicate with one another through peer Volumes.
func TestCreateVolumeAt(t *testing.T, sch bcns.Namespace, setup func(t testing.TB, svcs []blobcache.Service)) {
	type TestCase struct {
		// Name is the display name for the test
		Name string
		// Scenario is actualized before the test starts
		blobcachetests.Scene
		// Err is the expected error
		Err error
		// VolumePath
		VolumePath bcns.FQP
	}
	tcs := []TestCase{
		{
			Name: "single node",
			Scene: blobcachetests.Scene{
				Nodes: [][]blobcachetests.Volume{{}},
			},
			VolumePath: bcns.FQP{Path: "create-at-single-node"},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d-%s", i, tc.Name), func(t *testing.T) {
			ctx := testutil.Context(t)
			svcs := make([]blobcache.Service, len(tc.Scene.Nodes))
			setup(t, svcs)
			for i := range svcs {
				require.NotNil(t, svcs[i], "setup must provide service at index %d", i)
			}

			blobcachetests.SetupScene(t, tc.Scene, svcs)

			spec := blobcache.DefaultLocalSpec()
			got, err := bcns.CreateVolumeAt(ctx, svcs[0], tc.VolumePath, spec)
			if tc.Err != nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, blobcache.OID{}, got.OID)

			opened, err := bcns.Open(ctx, svcs[0], tc.VolumePath)
			require.NoError(t, err)
			require.Equal(t, got.OID, opened.OID)

			sch2, err := bcns.SchemaForVolume(ctx, svcs[0], opened)
			require.NoError(t, err)
			require.Equal(t, sch, sch2)
		})
	}
}
