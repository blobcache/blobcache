package blobcache_test

import (
	"testing"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobcachetest"
	"github.com/brendoncarroll/go-state/cadata"
)

func TestNode(t *testing.T) {
	blobcachetest.TestService(t, func(t testing.TB) blobcache.Service {
		return blobcache.NewNode(blobcache.Params{
			DB:    bcdb.NewBadgerMemory(),
			Store: cadata.NewMem(blobcache.Hash),
		})
	})
}
