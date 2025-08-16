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
func ensureRootVolume(tx *sqlx.Tx, spec blobcache.VolumeSpec) (*blobcache.VolumeInfo, error) {
	rootOID := blobcache.OID{}
	if _, err := getVolume(tx, rootOID); err == nil {
		return inspectVolume(tx, rootOID)
	} else if err != sql.ErrNoRows {
		return nil, err
	}
	if err := insertObject(tx, rootOID, time.Now()); err != nil {
		return nil, err
	}
	backendJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	vp := spec.Params()
	row := volumeRow{
		OID:     rootOID,
		Backend: backendJSON,

		HashAlgo: string(vp.HashAlgo),
		MaxSize:  vp.MaxSize,
		Schema:   string(vp.Schema),
		Salted:   vp.Salted,
	}
	if err := insertVolume(tx, row); err != nil {
		return nil, err
	}
	if _, err := createLocalVolume(tx, rootOID); err != nil {
		return nil, err
	}
	return inspectVolume(tx, rootOID)
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
	if err := tx.Get(&v, "SELECT id, sch, hash_algo, max_size, backend, salted FROM volumes WHERE id = ?", volID); err != nil {
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
			Schema:   blobcache.Schema(volRow.Schema),
			HashAlgo: blobcache.HashAlgo(volRow.HashAlgo),
			MaxSize:  volRow.MaxSize,
			Salted:   volRow.Salted,
		},
		Backend: backend,
	}
	return &volInfo, nil
}

// insertVolumeDeps inserts a dependency to a set of volumes.
// If any of the volumes are not found, an error is returned (this is done with a database constraint).
func insertVolumeDeps(tx *sqlx.Tx, volID blobcache.OID, deps iter.Seq[blobcache.OID]) error {
	for vol2 := range deps {
		if _, err := tx.Exec(`INSERT INTO volumes_deps (from_id, to_id) VALUES (?, ?)`, volID, vol2); err != nil {
			return err
		}
	}
	return nil
}

// cleanupVolumes deletes volumes which are not in the keep list,
// and which are not depended on by any other volume,
// and which are not linked to by any other volume,
// and which are not the root volume.
func cleanupVolumes(tx *sqlx.Tx, keep []blobcache.OID) error {
	if len(keep) == 0 {
		keep = []blobcache.OID{blobcache.OID{}} // In doesn't like empty slices.
	}
	// figure out which volumes to delete.
	q, args, err := sqlx.In(`SELECT id FROM volumes
		WHERE id != ?
		AND id NOT IN (?)
		AND NOT EXISTS (SELECT 1 FROM volumes_deps WHERE to_id = id)
		AND NOT EXISTS (SELECT 1 FROM volume_links WHERE to_id = id)
	`, blobcache.OID{}, keep)
	if err != nil {
		return err
	}
	var toDelete []blobcache.OID
	if err := tx.Select(&toDelete, q, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	// delete the volumes.
	for _, volID := range toDelete {
		if err := dropVolume(tx, volID); err != nil {
			return err
		}
	}
	return nil
}

// dropVolume drops a volume from the database.
func dropVolume(tx *sqlx.Tx, volID blobcache.OID) error {
	if _, err := tx.Exec(`DELETE FROM volumes_deps WHERE from_id = ?`, volID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM volume_links WHERE from_id = ?`, volID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM volumes WHERE id = ?`, volID); err != nil {
		return err
	}
	if err := dropObject(tx, volID); err != nil {
		return err
	}
	return nil
}
