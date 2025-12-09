package schematests

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/schema"
)

// Setup performs generic setup to prepare a Volume with the desired schema.
func Setup(t testing.TB, schs map[blobcache.SchemaName]schema.Constructor, spec blobcache.VolumeBackend_Local) (blobcache.Service, blobcache.Handle) {
	t.Helper()
	env := bclocal.NewTestEnv(t)
	var mkSchema func(spec blobcache.SchemaSpec) (schema.Schema, error)
	mkSchema = func(spec blobcache.SchemaSpec) (schema.Schema, error) {
		cons, exists := schs[spec.Name]
		if !exists {
			return nil, fmt.Errorf("schema %s not found", spec.Name)
		}
		return cons(spec.Params, mkSchema)
	}
	env.MkSchema = mkSchema
	svc := bclocal.NewTestServiceFromEnv(t, env)
	vspec := blobcache.VolumeSpec{Local: &spec}
	h := blobcachetests.CreateVolume(t, svc, nil, vspec)
	return svc, h
}
