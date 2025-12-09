package jsonns

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/schematests"
)

func TestJSONNS(t *testing.T) {
	// Test cases for BasicNS
	TestSuite(t, func(t testing.TB) (svc blobcache.Service, nsh blobcache.Handle) {
		spec := blobcache.DefaultLocalSpec()
		spec.Local.Schema = blobcache.SchemaSpec{
			Name: SchemaName,
		}
		schs := map[blobcache.SchemaName]schema.Constructor{
			"":         schema.NoneConstructor,
			SchemaName: Constructor,
		}
		return schematests.Setup(t, schs, *spec.Local)
	})
}
