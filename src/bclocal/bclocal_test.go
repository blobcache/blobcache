package bclocal

import (
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
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

func TestTxSystem(t *testing.T) {
	db := newTestPebbleDB(t)
	txs := newTxSystem(db)

	// allocate a new transaction ID
	txid1, err := txs.allocateTxID()
	require.NoError(t, err)
	require.Equal(t, pdb.MVTag(1), txid1)
	txid2, err := txs.allocateTxID()
	require.NoError(t, err)
	require.Equal(t, pdb.MVTag(2), txid2)

	// mark the transaction as active
	require.NoError(t, doRWBatch(db, func(ba *pebble.Batch) error {
		return txs.start(ba, txid1)
	}))
	// read the active transactions
	activeTxns := make(map[pdb.MVTag]struct{})
	require.NoError(t, doSnapshot(db, func(sn *pebble.Snapshot) error {
		clear(activeTxns)
		return txs.readActive(sn, activeTxns)
	}))
	require.Equal(t, 1, len(activeTxns))

	// remove the transaction from the active set
	require.NoError(t, doRWBatch(db, func(ba *pebble.Batch) error {
		return txs.success(ba, txid1)
	}))
	// read the active transactions
	require.NoError(t, doSnapshot(db, func(sn *pebble.Snapshot) error {
		clear(activeTxns)
		return txs.readActive(sn, activeTxns)
	}))
	require.Equal(t, 0, len(activeTxns))
}

func newTestPebbleDB(t *testing.T) *pebble.DB {
	dbPath := filepath.Join(t.TempDir(), "pebble")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}
