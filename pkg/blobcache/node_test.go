package blobcache_test

import (
	"testing"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobcachetest"
)

func TestNode(t *testing.T) {
	blobcachetest.TestService(t, func(t testing.TB) blobcache.Service {
		return blobcache.NewNode(blobcache.NewMemParams())
	})
}
