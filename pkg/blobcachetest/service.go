package blobcachetest

import (
	"context"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/storetest"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T, newService func(t testing.TB) blobcache.Service) {
	t.Run("CreatePinSet", func(t *testing.T) {
		ctx := context.Background()
		srv := newService(t)
		psh, err := srv.CreatePinSet(ctx, blobcache.PinSetOptions{})
		require.NoError(t, err)
		t.Log(psh)
	})
	t.Run("Store", func(t *testing.T) {
		ctx := context.Background()
		srv := newService(t)
		storetest.TestStore(t, func(t testing.TB) cadata.Store {
			psh, err := srv.CreatePinSet(ctx, blobcache.PinSetOptions{})
			require.NoError(t, err)
			return blobcache.NewStore(srv, psh)
		})
	})
}
