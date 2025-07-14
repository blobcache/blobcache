package scheme_glfs

import (
	"testing"

	"blobcache.io/blobcache/src/bcfs"
)

func TestSchemeGLFS(t *testing.T) {
	bcfs.TestFS(t, func(tb testing.TB) bcfs.Scheme[string] {
		return NewScheme()
	})
}
