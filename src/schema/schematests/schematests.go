package schematests

import (
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
	env.Schemas = schs
	svc := bclocal.NewTestServiceFromEnv(t, env)
	vspec := blobcache.VolumeSpec{Local: &spec}
	h := blobcachetests.CreateVolume(t, svc, nil, vspec)
	return svc, h
}
