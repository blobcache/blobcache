package bcgrpc

import (
	"net"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobcachetest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestClientServer(t *testing.T) {
	blobcachetest.TestService(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
		n := blobcache.NewNode(blobcache.NewMemParams())
		l, err := net.Listen("tcp", "127.0.0.1:")
		require.NoError(t, err)
		t.Cleanup(func() { l.Close() })
		gs := grpc.NewServer()
		srv := NewServer(n)
		RegisterBlobcacheServer(gs, srv)
		go gs.Serve(l)
		gc, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
		require.NoError(t, err)
		return NewClient(NewBlobcacheClient(gc)), n.Root()
	})
}
