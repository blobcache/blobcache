package bcnstests

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/schema/bcns"
)

// TestCreateVolumeAt runs a test suite for bcns.CreateVolumeAt.
func TestCreateVolumeAt(t *testing.T, sch bcns.Namespace, setup func(t testing.TB, svcs []blobcache.Service)) {
	type TestCase struct {
		// Name is the display name for the test
		Name string
		// Scenario is actualized before the test starts
		blobcachetests.Scene
		// Err is the expected error
		Err error

		VolumePath bcns.FQP
	}
	tcs := []TestCase{
		{
			Name: "single node",
			Scene: Scene{
				Nodes: [][]Volume{
					[]Volume{&LocalVolume{Contents: &NS{Schema: sch}}},
				},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d-%s", i, tc.Name), func(t *testing.T) {

		})
	}
}
