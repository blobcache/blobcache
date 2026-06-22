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

// TestCreateVolume runs a test suite for bcns.Client.CreateVolume.
// setup must fill svcs with working services, which can communicate with one another through peer Volumes.
func TestCreateVolume(t *testing.T, sch bcns.Namespace, schemaSpec blobcache.SchemaSpec, setup func(t testing.TB, svcs []blobcache.Service)) {
	type TestCase struct {
		Name string
		blobcachetests.Scene
		Path string
		Err  error
		// Host is the expected host of the Volume
		Host int
	}

	tcs := []TestCase{
		{
			Name: "single node",
			Scene: blobcachetests.Scene{
				Nodes: []blobcachetests.Node{{}},
			},
			Path: "create-at-single-node",
		},
		{
			Name: "remote namespace on node 1",
			Scene: blobcachetests.Scene{
				Nodes: []blobcachetests.Node{
					{
						Root: blobcachetests.LocalVolume{
							Schema: schemaSpec,
							Contents: &NS{
								Schema: sch,
								Entries: []Entry{
									{Name: "remote", Target: 0, Rights: blobcache.Action_ALL},
								},
							},
						},
						Volumes: []blobcachetests.Volume{
							&blobcachetests.PeerVolume{Node: 1, Volume: 0},
						},
					},
					{
						Root: blobcachetests.LocalVolume{
							Schema:   schemaSpec,
							Contents: &NS{Schema: sch},
						},
						Volumes: []blobcachetests.Volume{
							&blobcachetests.LocalVolume{
								Schema:   schemaSpec,
								Contents: &NS{Schema: sch},
							},
						},
					},
				},
			},
			Path: "remote/myvol",
			Host: 1,
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d-%s", i, tc.Name), func(t *testing.T) {
			ctx := testutil.Context(t)
			svcs := make([]blobcache.Service, len(tc.Scene.Nodes))
			setup(t, svcs)
			blobcachetests.SetupScene(t, tc.Scene, svcs)

			// create a LocalVolume at the path
			spec := blobcache.DefaultLocalSpec()
			nsc := bcns.NewClient(svcs[0], blobcache.OID{})
			volh, err := nsc.CreateVolume(ctx, tc.Path, spec)
			if tc.Err != nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, blobcache.OID{}, volh.OID)

			// Inspect the Volume
			vinfo, err := svcs[0].InspectVolume(ctx, volh)
			require.NoError(t, err)
			if tc.Host != 0 {
				fqoid := vinfo.GetRemoteFQOID()
				nodeID := blobcachetests.Endpoint(t, svcs[tc.Host]).Node
				require.Equal(t, nodeID, fqoid.Node)
			}
		})
	}
}
