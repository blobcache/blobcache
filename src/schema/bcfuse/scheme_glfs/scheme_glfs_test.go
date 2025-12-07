package scheme_glfs

import (
	"testing"

	_ "blobcache.io/blobcache/src/schema/basicns"
	"blobcache.io/blobcache/src/schema/bcfuse"
)

func TestSchemeGLFS(t *testing.T) {
	bcfuse.TestFS(t, func(tb testing.TB) bcfuse.Scheme[string] {
		return NewScheme()
	})
}
