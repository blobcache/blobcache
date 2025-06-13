package bclocal

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
)

func TestSetupDB(t *testing.T) {
	ctx := testutil.Context(t)
	db := dbutil.OpenMemory()
	if err := SetupDB(ctx, db); err != nil {
		t.Fatal(err)
	}
}

func TestAPI(t *testing.T) {
	blobcachetests.ServiceAPI(t, NewTestService)
}
