package bclocal

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cockroachdb/pebble"
)

type tableID uint32

// tableKey appends a tableID to out, followed by key.
func tableKey(out []byte, tid tableID, key []byte) []byte {
	out = binary.BigEndian.AppendUint32(out, uint32(tid))
	out = append(out, key...)
	return out
}

func splitTableID(key []byte) (tableID, []byte, error) {
	if len(key) < 4 {
		return 0, nil, fmt.Errorf("key is too short to contain tableID")
	}
	return tableID(binary.BigEndian.Uint32(key[:4])), key[4:], nil
}

const (
	tid_UNKNOWN = tableID(iota)

	tid_BLOB_DATA
	tid_BLOB_META
	tid_VOLUMES
	tid_LOCAL_TXNS
	tid_LOCAL_VOLUME_CELLS
	tid_LOCAL_VOLUME_BLOBS
	tid_VOLUME_LINKS
	tid_VOLUME_LINKS_INV
)

type dbReading interface {
	Get(k []byte) (v []byte, closer io.Closer, err error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

type dbWriting interface {
	Set(k, v []byte, opts *pebble.WriteOptions) error
}

var _ dbReading = (*pebble.Snapshot)(nil)

func doSnapshot(db *pebble.DB, fn func(*pebble.Snapshot) error) error {
	sn := db.NewSnapshot()
	defer sn.Close()
	return fn(sn)
}

// doRWBatch creates an indexed batch and calls fn with it.
// if fn returns nil, the batch is committed.
func doRWBatch(db *pebble.DB, fn func(*pebble.Batch) error) error {
	ba := db.NewIndexedBatch()
	defer ba.Close()
	if err := fn(ba); err != nil {
		return err
	}
	return ba.Commit(pebble.Sync)
}

func volumeBlobKey(volID blobcache.OID, cid blobcache.CID, mvc MVCCID) []byte {
	return tableKey(nil,
		tid_LOCAL_VOLUME_BLOBS,
		slices.Concat(volID[:], cid[:], binary.BigEndian.AppendUint64(nil, uint64(mvc))),
	)
}
