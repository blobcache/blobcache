package bclocal

import (
	"database/sql"
	"encoding/json"
	"iter"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/jmoiron/sqlx"
)

// createVolume creates a new volume, and stores info in the volumes table.
// if the backend is local, then it will also call createLocalVolume.
func createVolume(tx *sqlx.Tx, info blobcache.VolumeInfo) (*blobcache.OID, error) {
	oid, err := createObject(tx)
	if err != nil {
		return nil, err
	}
	backendJSON, err := json.Marshal(info.Backend)
	if err != nil {
		return nil, err
	}
	row := volumeRow{
		OID:      *oid,
		HashAlgo: string(info.HashAlgo),
		MaxSize:  info.MaxSize,
		Backend:  backendJSON,
		Salted:   info.Salted,
	}
	if err := insertVolume(tx, row); err != nil {
		return nil, err
	}
	if err := insertVolumeDeps(tx, *oid, info.Backend.Deps()); err != nil {
		return nil, err
	}
	if info.Backend.Local != nil {
		if _, err := createLocalVolume(tx, *oid); err != nil {
			return nil, err
		}
	}
	return oid, nil
}

// ensureRootVolume creates the root volume if it does not exist.
func ensureRootVolume(tx *sqlx.Tx) error {
	rootOID := blobcache.OID{}
	if _, err := getVolume(tx, rootOID); err == nil {
		return nil
	} else if err != sql.ErrNoRows {
		return err
	}
	if err := insertObject(tx, rootOID, time.Now()); err != nil {
		return err
	}
	info := rootVolumeInfo()
	backendJSON, err := json.Marshal(info.Backend)
	if err != nil {
		return err
	}
	row := volumeRow{
		OID:      rootOID,
		HashAlgo: string(info.HashAlgo),
		MaxSize:  info.MaxSize,
		Backend:  backendJSON,
		Schema:   string(info.Schema),
	}
	if err := insertVolume(tx, row); err != nil {
		return err
	}
	if _, err := createLocalVolume(tx, rootOID); err != nil {
		return err
	}
	return nil
}

// rootVolumeInfo returns the info for the root volume.
func rootVolumeInfo() blobcache.VolumeInfo {
	return blobcache.VolumeInfo{
		ID: blobcache.OID{},
		VolumeParams: blobcache.VolumeParams{
			Schema:   blobcache.SchemaName_SimpleNS,
			HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			MaxSize:  1 << 22,
		},
		Backend: blobcache.VolumeBackend[blobcache.OID]{
			Local: &blobcache.VolumeBackend_Local{},
		},
	}
}

// insertVolume inserts a volume into the volumes table.
func insertVolume(tx *sqlx.Tx, row volumeRow) error {
	_, err := tx.Exec(`INSERT INTO volumes (id, backend, hash_algo, max_size, sch, salted)
	    VALUES (?, ?, ?, ?, ?, ?)`, row.OID, row.Backend, row.HashAlgo, row.MaxSize, row.Schema, row.Salted)
	if err != nil {
		return err
	}
	return nil
}

// volumeRow is a row in the volumes table.
type volumeRow struct {
	OID    blobcache.OID `db:"id"`
	Schema string        `db:"sch"`
	// TODO: use the HashAlgo type, make sure it serializes correctly for the database.
	HashAlgo string          `db:"hash_algo"`
	MaxSize  int64           `db:"max_size"`
	Backend  json.RawMessage `db:"backend"`
	Salted   bool            `db:"salted"`
}

func getVolume(tx *sqlx.Tx, volID blobcache.OID) (*volumeRow, error) {
	var v volumeRow
	if err := tx.Get(&v, "SELECT id, hash_algo, max_size, backend, salted FROM volumes WHERE id = ?", volID); err != nil {
		return nil, err
	}
	return &v, nil
}

func inspectVolume(tx *sqlx.Tx, volID blobcache.OID) (*blobcache.VolumeInfo, error) {
	volRow, err := getVolume(tx, volID)
	if err != nil {
		return nil, err
	}
	var backend blobcache.VolumeBackend[blobcache.OID]
	if err := json.Unmarshal(volRow.Backend, &backend); err != nil {
		return nil, err
	}
	volInfo := blobcache.VolumeInfo{
		ID: volID,
		VolumeParams: blobcache.VolumeParams{
			Schema:   blobcache.SchemaName(volRow.Schema),
			HashAlgo: blobcache.HashAlgo(volRow.HashAlgo),
			MaxSize:  volRow.MaxSize,
			Salted:   volRow.Salted,
		},
		Backend: backend,
	}
	return &volInfo, nil
}

// insertVolumeDeps inserts a dependency to a set of subvols.
// If any of the subvols are not found, an error is returned (this is done with a database constraint).
func insertVolumeDeps(tx *sqlx.Tx, volID blobcache.OID, deps iter.Seq[blobcache.OID]) error {
	for vol2 := range deps {
		if _, err := tx.Exec(`INSERT INTO volumes_deps (from_id, to_id) VALUES (?, ?)`, volID, vol2); err != nil {
			return err
		}
	}
	return nil
}

// setSubVolumes sets the subvolumes for a volume within a transaction.
func setSubVolumes(tx *sqlx.Tx, txh LocalTxnID, subvols []blobcache.OID) error {
	return nil
}

type volumeLink struct {
	OID    blobcache.OID
	Rights blobcache.ActionSet
}
