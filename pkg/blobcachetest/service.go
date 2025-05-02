package blobcachetest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cadata/storetest"
)

// Factory returns a fresh blobcache.Service for testing
type Factory = func(t testing.TB) (blobcache.Service, blobcache.Handle)

func TestService(t *testing.T, newService func(t testing.TB) (blobcache.Service, blobcache.Handle)) {
	ctx := context.Background()
	t.Run("CreatePinSet", func(t *testing.T) {
		srv, rh := newService(t)
		psh, err := srv.CreatePinSet(ctx, rh, "test", blobcache.PinSetOptions{})
		require.NoError(t, err)
		t.Log(psh)
		_, err = srv.GetPinSet(ctx, *psh)
		require.NoError(t, err)
		ents, err := srv.ListEntries(ctx, rh)
		require.NoError(t, err)
		require.Len(t, ents, 1)
		require.Equal(t, "test", ents[0].Name)
	})
	t.Run("Store", func(t *testing.T) {
		srv, rh := newService(t)
		genName := newNameGenerator("test")
		storetest.TestStore(t, func(t testing.TB) cadata.Store {
			psh, err := srv.CreatePinSet(ctx, rh, genName(), blobcache.PinSetOptions{})
			require.NoError(t, err)
			t.Log("created pinset", psh.ID)
			return blobcache.NewStore(srv, *psh)
		})
	})
	t.Run("CreateDir", func(t *testing.T) {
		srv, rh := newService(t)

		dirs := []string{"test1", "test2", "test3"}
		ents, err := srv.ListEntries(ctx, rh)
		require.NoError(t, err)
		require.Len(t, ents, 0)
		for _, name := range dirs {
			dh, err := srv.CreateDir(ctx, rh, name)
			require.NoError(t, err)
			require.NotNil(t, dh)
		}
		ents, err = srv.ListEntries(ctx, rh)
		require.NoError(t, err)
		require.Len(t, ents, 3)
	})
}

func newNameGenerator(prefix string) func() string {
	var n uint32
	return func() string {
		n := atomic.AddUint32(&n, 1)
		return fmt.Sprintf("%s-%03d", prefix, n)
	}
}
