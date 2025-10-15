package bclocal

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
)

func TestNewService(t *testing.T) {
	NewTestService(t)
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
