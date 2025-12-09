package basicns

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/schematests"
)

func TestBasicNS(t *testing.T) {
	// Test cases for BasicNS
	TestSuite(t, func(t testing.TB) (svc blobcache.Service, nsh blobcache.Handle) {
		schs := map[blobcache.SchemaName]schema.Constructor{
			"":         schema.NoneConstructor,
			SchemaName: Constructor,
		}
		spec := blobcache.DefaultLocalSpec()
		spec.Local.Schema = blobcache.SchemaSpec{
			Name: SchemaName,
		}
		return schematests.Setup(t, schs, *spec.Local)
	})
}
