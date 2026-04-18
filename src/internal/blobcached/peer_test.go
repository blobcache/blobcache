package blobcached

import (
	"fmt"
	"net"
	"os"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestPeerLocation(t *testing.T) {
	ctx := testutil.Context(t)

	// Create two daemons' state dirs and packet conns up-front,
	// so we can write PEER_LOC files before starting them.
	type site struct {
		daemon *Daemon
		pc     net.PacketConn
		lis    net.Listener
		peerID blobcache.NodeID
	}
	sites := make([]site, 2)
	for i := range sites {
		dir, err := os.OpenRoot(t.TempDir())
		require.NoError(t, err)
		d := &Daemon{StateDir: dir, ConfigDir: dir}
		require.NoError(t, d.EnsurePolicyFiles())
		peerID, err := d.GetPeerID()
		require.NoError(t, err)
		sites[i] = site{
			daemon: d,
			pc:     testutil.PacketConn(t),
			lis:    testutil.Listen(t),
			peerID: peerID,
		}
	}

	// Write PEER_LOC files: each daemon gets the other's peer ID
	// mapped to localhost:<port> so the DNS lookup for "localhost" is exercised.
	for i := range sites {
		other := &sites[1-i]
		udpPort := other.pc.LocalAddr().(*net.UDPAddr).Port
		// Use "localhost" (not 127.0.0.1) so DNS resolution is exercised.
		peerLocContent := fmt.Sprintf("%s localhost:%d\n", other.peerID, udpPort)
		require.NoError(t, sites[i].daemon.StateDir.WriteFile(peerLocPath, []byte(peerLocContent), 0644))

		// Also add the other peer to the admin identity so it has access.
		require.NoError(t, sites[i].daemon.AddPeerToAdmin(other.peerID))
	}

	// start both daemons
	for i := range sites {
		s := &sites[i]
		bgTestDaemon(t, s.daemon, s.pc, []net.Listener{s.lis}, nil)
	}

	svc1 := bcclient.NewClient(sites[0].lis.Addr().Network() + "://" + sites[0].lis.Addr().String())
	svc2 := bcclient.NewClient(sites[1].lis.Addr().Network() + "://" + sites[1].lis.Addr().String())

	// Create a local volume on daemon 1.
	volh, err := svc1.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	// Create a peer volume on daemon 2 pointing at daemon 1 via PeerID + OID (no endpoint).
	peerVolh, err := svc2.CreateVolume(ctx, nil, blobcache.VolumeSpec{
		Peer: &blobcache.VolumeBackend_Peer{
			Peer:   sites[0].peerID,
			Volume: volh.OID,
		},
	})
	require.NoError(t, err)

	// Use the peer volume: write via daemon 1, read back via daemon 2.
	txh1, err := svc1.BeginTx(ctx, *volh, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	data := []byte("hello from peer location test")
	cid, err := svc1.Post(ctx, *txh1, data, blobcache.PostOpts{})
	require.NoError(t, err)
	require.NoError(t, svc1.Commit(ctx, *txh1))

	txh2, err := svc2.BeginTx(ctx, *peerVolh, blobcache.TxParams{})
	require.NoError(t, err)
	buf := make([]byte, len(data)+64)
	n, err := svc2.Get(ctx, *txh2, cid, buf, blobcache.GetOpts{})
	require.NoError(t, err)
	require.Equal(t, data, buf[:n])
	require.NoError(t, svc2.Abort(ctx, *txh2))
	t.Log("peer volume read-back succeeded")
}
