package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
)

func TestVolumeSubscribe(t *testing.T, setup func(testing.TB) (svc blobcache.Service, volh, qh blobcache.Handle)) {
	// Each test calls SubToVol to subscribe the queue to the Volume.

	// Empty checks that empty messages show up whenever there is a commit to the Volume.
	t.Run("Empty", func(t *testing.T) {

	})
}
