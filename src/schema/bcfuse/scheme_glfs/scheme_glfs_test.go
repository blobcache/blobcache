package scheme_glfs

import (
	"testing"

	"blobcache.io/blobcache/src/schema/bcfuse"
	_ "blobcache.io/blobcache/src/schema/jsonns"
)

func TestSchemeGLFS(t *testing.T) {
	bcfuse.TestFS(t, func(tb testing.TB) bcfuse.Scheme[string] {
		return NewScheme()
	})
}
