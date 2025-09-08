package bclocal

import (
	"encoding/binary"
	"math/rand/v2"
	"os"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

func TestNewService(t *testing.T) {
	NewTestService(t)
}

func TestAPI(t *testing.T) {
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		svc := NewTestService(t)
		return svc
	})
}

func TestMultiNode(t *testing.T) {
	blobcachetests.TestMultiNode(t, func(t testing.TB, n int) []blobcache.Service {
		svcs := make([]blobcache.Service, n)
		for i := range svcs {
			svcs[i] = NewTestService(t)
		}
		for i := range svcs {
			for j := range svcs {
				if i == j {
					continue
				}
				peerID := svcs[j].(*Service).node.LocalID()
				svcs[i].(*Service).env.Policy = &AllOrNothingPolicy{Allow: []blobcache.PeerID{peerID}}
			}
		}
		return svcs
	})
}

// TestDefaultNoAccess tests that a remote peer cannot perform any
// actions on the local service by default.
func TestDefaultNoAccess(t *testing.T) {
	ctx := testutil.Context(t)
	svc1 := NewTestService(t)
	svc2 := NewTestService(t)

	// create a volume on svc1 so there is something to try to access.
	volh, err := svc1.CreateVolume(ctx, nil, blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 20,
			},
		},
	})
	require.NoError(t, err)
	nsc1 := simplens.Client{Service: svc1}
	require.NoError(t, err)
	require.NoError(t, nsc1.PutEntry(ctx, blobcache.Handle{}, "name1", *volh))

	nsc2 := simplens.Client{Service: svc2}
	entry, err := nsc2.GetEntry(ctx, blobcache.Handle{}, "name1")
	require.NoError(t, err)
	require.Nil(t, entry)

	err = nsc2.PutEntry(ctx, *volh, "any name", blobcache.Handle{})
	require.Error(t, err)

	names, err := nsc2.ListNames(ctx, *volh)
	require.Error(t, err)
	require.Empty(t, names)
}

func TestUploadDownload(t *testing.T) {
	blobDir, err := os.OpenRoot(t.TempDir())
	require.NoError(t, err)
	defer blobDir.Close()

	const blobSize = 1 << 18
	var cids []blobcache.CID

	var buf []byte
	for i := 0; i < 1024; i++ {
		buf = buf[:0]
		rng := rand.NewPCG(uint64(i), uint64(i))
		for len(buf) < blobSize {
			buf = binary.LittleEndian.AppendUint64(buf, rng.Uint64())
		}

		cid := blake3.Sum256(buf)
		require.NoError(t, uploadBlob(blobDir, cid, buf))
		cids = append(cids, cid)
	}

	for _, cid := range cids {
		n, err := downloadBlob(blobDir, cid, buf)
		require.NoError(t, err)
		data := buf[:n]
		h := blake3.Sum256(data)
		require.Equal(t, cid, h)
	}
}
