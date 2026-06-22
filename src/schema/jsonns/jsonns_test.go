package jsonns

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/bcns/bcnstests"
	"blobcache.io/blobcache/src/schema/schematests"
)

// TestNS runs the bcns test suite
func TestNS(t *testing.T) {
	spec := blobcache.SchemaSpec{Name: SchemaName}
	bcnstests.TestSuite(t, Schema{}, spec, setup)
}

func setup(t testing.TB, svcs []blobcache.Service) {
	schs := map[blobcache.SchemaName]schema.Constructor{
		"":         schema.NoneConstructor,
		SchemaName: Constructor,
	}
	schematests.InitNodes(t, schs, svcs)
}
