package bclocal

import (
	"database/sql"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/state/cadata"
)

// ensureBlob inserts a row into the blobs table, if it doesn't already exist.
func ensureBlob(tx *sqlx.Tx, cid blobcache.CID, salt *blobcache.CID, data []byte) error {
	if data == nil {
		// this is necessary to avoid inserting a null blob into the database (which is not allowed)
		data = []byte{}
	}
	if _, err := tx.Exec(`
		INSERT INTO blobs (cid, salt, data)
		VALUES (?, ?, ?)
		ON CONFLICT DO NOTHING
	`, cid, salt, data); err != nil {
		return err
	}
	return nil
}

// addBlob adds a blob to a transaction.  If the blob did not exist, then it is added.
// If it was previously marked deleted in the transaction, then it is added, and is_deleted is set to false.
func addBlob(tx *sqlx.Tx, volID LocalVolumeID, txid LocalTxnID, cid blobcache.CID) error {
	// if the blob did not exist in this store, increment its refcount.
	if _, err := tx.Exec(`UPDATE blobs SET rc = rc + 1 WHERE cid = ? AND NOT EXISTS (
		SELECT 1 FROM local_vol_blobs WHERE vol_id = ? AND txn_id = ?
	)`, cid, volID, txid); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO local_vol_blobs (vol_id, cid, txn_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`, volID, cid, txid); err != nil {
		return err
	}
	return nil
}

// deleteBlob deletes a blob from a transaction.
func deleteBlob(tx *sqlx.Tx, volID LocalVolumeID, txid LocalTxnID, cid blobcache.CID) error {
	if _, err := tx.Exec(`INSERT INTO local_vol_blobs (vol_id, cid, txn_id, is_delete) VALUES (?, ?, ?, ?)`, volID, cid, txid, true); err != nil {
		return err
	}
	// if the blob exists in this store, decrement its refcount.
	if _, err := tx.Exec(`UPDATE blobs SET rc = rc - 1 WHERE EXISTS (
		SELECT cid FROM local_vol_blobs WHERE vol_id = ? AND cid = ? AND txn_id = ?
	)`, volID, cid, txid); err != nil {
		return err
	}
	return nil
}

// txnContainsBlob returns true if the transaction contains the blob.
// txnContainsBlob considers all transactions below the base transaction for the volume.
func txnContainsBlob(tx *sqlx.Tx, volID LocalVolumeID, base, txnID LocalTxnID, cid blobcache.CID) (bool, error) {
	var exists bool
	if err := tx.Get(&exists, `
		SELECT EXISTS (
			SELECT 1 FROM local_vol_blobs
			WHERE vol_id = ? AND cid = ? AND (txn_id = ? OR txn_id <= ?)
		)
	`, volID, cid, txnID, base); err != nil {
		return false, err
	}
	return exists, nil
}

// readBlob reads a blob from a local volume.
func readBlob(tx *sqlx.Tx, volID LocalVolumeID, base, txnID LocalTxnID, cid blobcache.CID, buf []byte) (int, error) {
	var data []byte
	err := tx.Get(&data, `
		SELECT data FROM blobs
		JOIN local_vol_blobs ON blobs.cid = local_vol_blobs.cid
		WHERE local_vol_blobs.vol_id = ? AND local_vol_blobs.cid = ?
			AND (local_vol_blobs.txn_id = ? OR local_vol_blobs.txn_id <= ?)
	`, volID, cid, txnID, base)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, cadata.ErrNotFound{Key: cid}
		}
		return 0, err
	}
	copy(buf, data)
	return len(data), nil
}
