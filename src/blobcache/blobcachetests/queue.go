package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
)

func QueueAPI(t *testing.T, setup func(testing.TB) blobcache.Service) {
	// Need tests for:
	// - max bytes
	// - max handles
	// - 0 length buffer returns error
	// - simple Enqueue, Dequeue works
	// - Min works 0, 1, 2
	// - MaxWait works
	// - Concurrent Dequeue | wait 1s then Enqueue, Dequeue should return.
}
