package bclocal

import (
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/jmoiron/sqlx"
)

func createVolume(tx *sqlx.Tx, info blobcache.VolumeInfo) (*blobcache.OID, error) {
	oid, err := createObject(tx)
	if err != nil {
		return nil, err
	}
	storeID, err := createStore(tx)
	if err != nil {
		return nil, err
	}
	specJSON, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec("INSERT INTO volumes (id, root, spec, store_id) VALUES (?, ?, ?, ?)", oid, []byte{}, specJSON, storeID)
	if err != nil {
		return nil, err
	}
	return oid, nil
}

type volumeRow struct {
	ID      blobcache.OID `db:"id"`
	Root    []byte        `db:"root"`
	StoreID StoreID       `db:"store_id"`
}

func getVolume(tx *sqlx.Tx, volID blobcache.OID) (*volumeRow, error) {
	var v volumeRow
	if err := tx.Get(&v, "SELECT id, root, store_id FROM volumes WHERE id = ?", volID); err != nil {
		return nil, err
	}
	return &v, nil
}

// getVolumeRoot gets the root of a local volume and sets dst to it.
func getVolumeRoot(tx *sqlx.Tx, volID blobcache.OID, dst *[]byte) error {
	if err := tx.Get(dst, "SELECT root FROM volumes WHERE id = ?", volID); err != nil {
		return err
	}
	return nil
}

// setVolumeRoot sets the root of a local volume.
func setVolumeRoot(tx *sqlx.Tx, volID blobcache.OID, root []byte) error {
	_, err := tx.Exec("UPDATE volumes SET root = ? WHERE id = ?", root, volID)
	return err
}

// volumeHasActiveTx returns true if the volume has an in progress transaction.
func volumeHasActiveTx(tx *sqlx.Tx, volID blobcache.OID) (bool, error) {
	var exists bool
	if err := tx.Get(&exists, "SELECT EXISTS (SELECT 1 FROM txns WHERE volume_id = ?)", volID); err != nil {
		return false, err
	}
	return exists, nil
}
