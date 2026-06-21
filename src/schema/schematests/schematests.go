package schematests

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/schema"
)

func Factory(schs Schemas) schema.Factory {
	var mkSchema func(spec blobcache.SchemaSpec) (schema.Schema, error)
	mkSchema = func(spec blobcache.SchemaSpec) (schema.Schema, error) {
		cons, exists := schs[spec.Name]
		if !exists {
			return nil, fmt.Errorf("schema %s not found", spec.Name)
		}
		return cons(spec.Params, mkSchema)
	}
	return mkSchema
}

type Schemas = map[blobcache.SchemaName]schema.Constructor

// InitNodes initializes svcs
func InitNodes(t testing.TB, schs Schemas, svcs []blobcache.Service) {
	t.Helper()
	for i := range svcs {
		env := bclocal.NewTestEnv(t)
		env.MkSchema = Factory(schs)
		svc := bclocal.NewTestServiceFromEnv(t, env)
		svcs[i] = svc
	}
}

// Setup performs generic setup to prepare a Volume with the desired schema.
func Setup(t testing.TB, schs Schemas, vspec blobcache.VolumeBackend_Local) (blobcache.Service, blobcache.Handle) {
	t.Helper()
	env := bclocal.NewTestEnv(t)
	env.MkSchema = Factory(schs)
	svc := bclocal.NewTestServiceFromEnv(t, env)
	volh := blobcachetests.CreateVolume(t, svc, nil, blobcache.VolumeSpec{Local: &vspec})
	return svc, volh
}
