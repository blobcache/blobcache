package scheme_glfs

import (
	"testing"

	"blobcache.io/blobcache/src/bcfuse"
)

func TestSchemeGLFS(t *testing.T) {
	bcfuse.TestFS(t, func(tb testing.TB) bcfuse.Scheme[string] {
		return NewScheme()
	})
}
