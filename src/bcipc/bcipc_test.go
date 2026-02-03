package bcipc

import (
	"context"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestBoth(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	dir := t.TempDir()
	svc := bclocal.NewTestService(t)
	p := filepath.Join(dir, "bcipc-test.sock")
	c := NewClient(p)
	var eg errgroup.Group
	eg.Go(func() error {
		// Dial
		volh, err := c.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
		if err != nil {
			return err
		}
		t.Log("volh", *volh)
		return nil
	})
	eg.Go(func() error {
		// Listen
		return ListenAndServe(ctx, p, &bcp.Server{
			Access: func(blobcache.PeerID) blobcache.Service {
				return svc
			},
		})
	})
	cf()
	require.NoError(t, eg.Wait())
}
