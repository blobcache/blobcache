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
func TestCreateVolumeAt(t *testing.T, setup func(t testing.TB, svcs []blobcache.Service)) {
	type TestCase struct {
		// Name is the display name for the test
		Name string
		// Scenario is actualized before the test starts
		blobcachetests.Scene

		// Path is the path to create the volume at.
		Path string
		// Err is the expected error
		Err error
		// Host is expected host of the Volume once created.
		Host blobcache.NodeID
	}
	tcs := []TestCase{
		{
			Name: "single node",
			Scene: blobcachetests.Scene{
				Nodes: [][]blobcachetests.Volume{{}},
			},
			Path: "create-at-single-node",
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d-%s", i, tc.Name), func(t *testing.T) {
			ctx := testutil.Context(t)
			svcs := make([]blobcache.Service, len(tc.Scene.Nodes))
			setup(t, svcs)
			blobcachetests.SetupScene(t, tc.Scene, svcs)

			spec := blobcache.DefaultLocalSpec()
			nsc := bcns.NewClient(svcs[0], blobcache.OID{})
			got, err := nsc.CreateVolumeAt(ctx, tc.Path, spec)
			if tc.Err != nil {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			require.NotEqual(t, blobcache.OID{}, got.OID)

			opened, err := nsc.Open(ctx, tc.Path)
			require.NoError(t, err)
			require.Equal(t, got.OID, opened.OID)
		})
	}
}
