package bclocal

import (
	"database/sql"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/state/cadata"
)

type StoreID = int64

func createStore(tx *sqlx.Tx) (StoreID, error) {
	res, err := tx.Exec("INSERT INTO stores DEFAULT VALUES")
	if err != nil {
		return 0, err
	}
	storeID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return StoreID(storeID), nil
}

// dropStore deletes all blobs from the store, decrementing their refcounts.
// Then it deletes the row from the stores table.
func dropStore(tx *sqlx.Tx, storeID StoreID) error {
	if _, err := tx.Exec(`
		UPDATE blobs SET rc = rc - 1
		WHERE cid IN (
			SELECT cid FROM store_blobs WHERE store_id = ?
		)
	`, storeID); err != nil {
		return err
	}
	_, err := tx.Exec("DELETE FROM stores WHERE id = ?", storeID)
	return err
}

// ensureBlob inserts a row into the blobs table, if it doesn't already exist.
func ensureBlob(tx *sqlx.Tx, cid blobcache.CID, salt *blobcache.CID, data []byte) error {
	if _, err := tx.Exec(`
		INSERT INTO blobs (cid, salt, data)
		VALUES (?, ?, ?)
		ON CONFLICT DO NOTHING
	`, cid, salt, data); err != nil {
		return err
	}
	return nil
}

// addBlob adds a blob to a store, and increments its refcount if it was not already in the store.
// The blob must already exist in the blobs table.
func addBlob(tx *sqlx.Tx, storeID StoreID, cid blobcache.CID) error {
	// if the blob did not exist in this store, increment its refcount.
	if _, err := tx.Exec(`UPDATE blobs SET rc = rc + 1 WHERE cid = ? AND NOT EXISTS (
		SELECT 1 FROM store_blobs WHERE store_id = ? AND cid = ?
	)`, cid, storeID, cid); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO store_blobs (store_id, cid) VALUES (?, ?) ON CONFLICT DO NOTHING`, storeID, cid); err != nil {
		return err
	}
	return nil
}

// deleteBlob delets a blob from a store.
func deleteBlob(tx *sqlx.Tx, storeID StoreID, cid blobcache.CID) error {
	if _, err := tx.Exec(`INSERT INTO store_blobs (store_id, cid, is_delete) VALUES (?, ?, ?)`, storeID, cid, true); err != nil {
		return err
	}
	// if the blob exists in this store, decrement its refcount.
	if _, err := tx.Exec(`UPDATE blobs SET rc = rc - 1 WHERE EXISTS (
		SELECT cid FROM store_blobs WHERE store_id = ? AND cid = ?
	)`, storeID, cid); err != nil {
		return err
	}
	return nil
}

// storesContainsBlob returns true if any of the stores contain the blob.
func storesContainsBlob(tx *sqlx.Tx, storeIDs []StoreID, cid blobcache.CID) (bool, error) {
	var exists int
	query, args, err := sqlx.In(`
		SELECT EXISTS (
			SELECT 1 FROM store_blobs WHERE store_id IN (?) AND cid = ?
		)
	`, storeIDs, cid)
	if err != nil {
		return false, err
	}
	if err := tx.Get(&exists, query, args...); err != nil {
		return false, err
	}
	return exists > 0, nil
}

// readBlob reads a blob from any of the stores.
func readBlob(tx *sqlx.Tx, storeIDs []StoreID, cid blobcache.CID, buf []byte) (int, error) {
	var data []byte
	query, args, err := sqlx.In(`
		SELECT data FROM blobs
		JOIN store_blobs ON blobs.cid = store_blobs.cid
		WHERE store_blobs.store_id IN (?) AND store_blobs.cid = ?
	`, storeIDs, cid)
	if err != nil {
		return 0, err
	}
	if err := tx.Get(&data, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return 0, cadata.ErrNotFound{Key: cid}
		}
		return 0, err
	}
	copy(buf, data)
	return len(data), nil
}

// mergeStores applies all additions and deletions from srcStoreIDs to dstStoreID.
// mergeStores leaves srcStoreIDs intact.
func mergeStores(tx *sqlx.Tx, dstStoreID StoreID, srcStoreIDs []StoreID) error {
	for _, srcStoreID := range srcStoreIDs {
		if _, err := tx.Exec(`
		INSERT INTO store_blobs (store_id, cid, is_delete)
		SELECT ?, b.cid, b.is_delete
		FROM store_blobs b
		WHERE b.store_id = ?
		ON CONFLICT(store_id, cid) DO UPDATE SET is_delete=excluded.is_delete
	`, dstStoreID, srcStoreID); err != nil {
			return err
		}
	}

	// TODO: use the refcount to delete blobs.
	if _, err := tx.Exec(`DELETE FROM blobs WHERE cid NOT IN (
		SELECT cid FROM store_blobs
	)`, dstStoreID); err != nil {
		return err
	}
	return nil
}
