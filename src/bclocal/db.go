package bclocal

import (
	"github.com/cockroachdb/pebble"
)

func doSnapshot(db *pebble.DB, fn func(*pebble.Snapshot) error) error {
	sn := db.NewSnapshot()
	defer sn.Close()
	return fn(sn)
}

// doRWBatch creates an indexed batch and calls fn with it.
// if fn returns nil, the batch is committed.
func doRWBatch(db *pebble.DB, fn func(*pebble.Batch) error) error {
	ba := db.NewIndexedBatch()
	//defer ba.Close()
	if err := fn(ba); err != nil {
		return err
	}
	return ba.Commit(nil)
}
