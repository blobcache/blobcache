// Package schemareg provides a registry for Schemas.
package schemareg

import (
	"fmt"
	"maps"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

// TODO: move defaultSchemas to blobcached package
var defaultSchemas = map[blobcache.SchemaName]schema.Constructor{
	blobcache.Schema_NONE: schema.NoneConstructor,
}

func AddDefaultSchema(name blobcache.SchemaName, constructor schema.Constructor) {
	if _, exists := defaultSchemas[name]; exists {
		panic(fmt.Errorf("schema %s already exists", name))
	}
	defaultSchemas[name] = constructor
}

func DefaultSchemas() map[blobcache.SchemaName]schema.Constructor {
	return maps.Clone(defaultSchemas)
}

// DefaultRoot returns the default root volume spec.
// It uses the jsonns schema and a 2MB byte max size.
func DefaultRoot() blobcache.VolumeSpec {
	const rootSchemaName = "blobcache/jsonns"
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			Schema:   blobcache.SchemaSpec{Name: rootSchemaName},
			HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			MaxSize:  1 << 22,
		},
	}
}

func Factory(spec blobcache.SchemaSpec) (schema.Schema, error) {
	if constructor, ok := defaultSchemas[spec.Name]; ok {
		return constructor(spec.Params, Factory)
	}
	return nil, fmt.Errorf("schema %s not found", spec.Name)
}
