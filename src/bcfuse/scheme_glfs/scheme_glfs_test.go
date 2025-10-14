package scheme_glfs

import (
	"testing"

	"blobcache.io/blobcache/src/bcfuse"
	_ "blobcache.io/blobcache/src/schema/basicns"
)

func TestSchemeGLFS(t *testing.T) {
	bcfuse.TestFS(t, func(tb testing.TB) bcfuse.Scheme[string] {
		return NewScheme()
	})
}
