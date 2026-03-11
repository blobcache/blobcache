package jsonns

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/schematests"
)

func setup(t testing.TB) (blobcache.Service, blobcache.Handle) {
	spec := blobcache.DefaultLocalSpec()
	spec.Local.Schema = blobcache.SchemaSpec{
		Name: SchemaName,
	}
	schs := map[blobcache.SchemaName]schema.Constructor{
		"":         schema.NoneConstructor,
		SchemaName: Constructor,
	}
	return schematests.Setup(t, schs, *spec.Local)
}

func TestJSONNS(t *testing.T) {
	TestSuite(t, setup)
}

func TestLookup(t *testing.T) {
	schematests.TestLookup(t, setup, func(svc blobcache.Service) *bcns.Client {
		return &bcns.Client{Service: svc, Schema: Schema{}}
	}, blobcache.SchemaSpec{Name: SchemaName})
}
