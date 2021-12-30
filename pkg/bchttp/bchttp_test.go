package bchttp

import (
	"net"
	"net/http"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobcachetest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestClientServer(t *testing.T) {
	blobcachetest.TestService(t, func(t testing.TB) blobcache.Service {
		n := blobcache.NewNode(blobcache.NewMemParams())
		l, err := net.Listen("tcp", "127.0.0.1:")
		require.NoError(t, err)
		srv := NewServer(n, logrus.StandardLogger())
		go http.Serve(l, srv)
		return NewClient("http://" + l.Addr().String())
	})
}
