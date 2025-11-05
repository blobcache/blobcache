package e2etest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blobcache.io/glfs"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/sync/errgroup"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcremote"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/basicns"
)

// TestDaemonAuth checks that the daemon correctly reads the auth policy files.
// And allows access over the network.
func TestDaemonAuth(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	stateDir := t.TempDir()
	d := blobcached.Daemon{StateDir: stateDir}
	clientPub, clientPriv, err := inet256.GenerateKey()
	require.NoError(t, err)
	id := inet256.NewID(clientPub)

	identitiesPath := filepath.Join(stateDir, blobcached.IdentitiesFilename)
	// add the client's ID to the identities file as an admin
	appendToFile(t, identitiesPath,
		[]byte("admin "+id.String()+"\n"),
	)
	t.Logf("identities file: %s", identitiesPath)
	printFile(t, identitiesPath)

	// run the daemon
	pc := testutil.PacketConn(t)
	eg := errgroup.Group{}
	eg.Go(func() error {
		return d.Run(ctx, pc, nil)
	})

	// connect with the client
	peer, err := d.GetPeerID()
	require.NoError(t, err)
	ep := blobcache.Endpoint{
		Peer:   peer,
		IPPort: pc.LocalAddr().(*net.UDPAddr).AddrPort(),
	}

	svc, err := bcremote.Dial(clientPriv.(ed25519.PrivateKey), ep)
	require.NoError(t, err)
	nsc := basicns.Client{Service: svc}
	_, err = nsc.CreateAt(ctx, blobcache.Handle{}, "test-volume", blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	cf()
	eg.Wait()
}

func appendToFile(t *testing.T, p string, data []byte) {
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file %s: %v", p, err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatalf("failed to write to file %s: %v", p, err)
	}
}

func printFile(t testing.TB, p string) {
	data, err := os.ReadFile(p)
	if err != nil {
		require.NoError(t, err)
	}
	t.Logf("file %s: %s", p, string(data))
}

// TestGLFS tests that blobcache integrates correctly with the Git-Like File System.
func TestGLFS(t *testing.T) {
	ctx := testutil.Context(t)
	svc := newTestService(t)
	volh, err := svc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	blobcachetests.Modify(t, svc, *volh, func(tx *blobcache.Tx) ([]byte, error) {
		ref, err := glfs.PostBlob(ctx, tx, strings.NewReader("hello"))
		require.NoError(t, err)
		ref, err = glfs.PostTreeSlice(ctx, tx, []glfs.TreeEntry{
			{Name: "hello.txt", Ref: *ref},
		})
		require.NoError(t, err)
		return jsonMarshal(ref), nil
	})

	tx, err := blobcache.BeginTx(ctx, svc, *volh, blobcache.TxParams{})
	require.NoError(t, err)
	var root []byte
	require.NoError(t, tx.Load(ctx, &root))
	ref := new(glfs.Ref)
	require.NoError(t, json.Unmarshal(root, ref))
	ref, err = glfs.GetAtPath(ctx, tx, *ref, "hello.txt")
	require.NoError(t, err)
	data, err := glfs.GetBlobBytes(ctx, tx, *ref, 100)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
}

func jsonMarshal(x any) []byte {
	buf, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return buf
}

func newTestService(t testing.TB) blobcache.Service {
	s := bclocal.NewTestService(t)
	lis := testutil.Listen(t)
	go func() {
		http.Serve(lis, &bchttp.Server{Service: s})
	}()
	return bchttp.NewClient(nil, fmt.Sprintf("http://%s", lis.Addr().String()))
}
