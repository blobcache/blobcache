package localvol

import (
	"encoding/binary"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/cockroachdb/pebble"
)

// blobMeta is a row in the BLOB_META table.
type blobMeta struct {
	cid blobcache.CID

	flags uint8
	salt  *blobcache.CID
	size  uint32
}

func (bi blobMeta) hasSalt() bool {
	return bi.flags&(1<<0) != 0
}

// parseBlobMeta parses a blobMeta from a key and value.
func parseBlobMeta(k []byte, v []byte) (blobMeta, error) {
	if len(k) != blobcache.CIDSize {
		return blobMeta{}, errors.New("blobMeta: key is not a valid cid")
	}
	if len(v) < 1 {
		return blobMeta{}, errors.New("blobMeta: value is too short")
	}
	return blobMeta{
		cid: blobcache.CID(k),

		flags: v[0],
	}, nil
}

func (bi blobMeta) Key(out []byte) []byte {
	return pdb.TKey{
		TableID: dbtab.TID_BLOB_META,
		Key:     bi.cid[:],
	}.Marshal(out)
}

func (bi blobMeta) Value(out []byte) []byte {
	out = append(out, bi.flags)
	if bi.salt != nil {
		out = append(out, 1)
		out = append(out, bi.salt[:]...)
	}
	return out
}

// putBlobMeta puts a blobMeta into the database.
func putBlobMeta(ba pdb.WO, bm blobMeta) error {
	return ba.Set(bm.Key(nil), bm.Value(nil), nil)
}

// getBlobMeta gets a blobMeta from the database.
func getBlobMeta(ba pdb.RO, cid blobcache.CID) (blobMeta, error) {
	k := pdb.TKey{TableID: dbtab.TID_BLOB_META, Key: cid[:]}.Marshal(nil)
	v, closer, err := ba.Get(k)
	if err != nil {
		return blobMeta{}, err
	}
	defer closer.Close()
	return parseBlobMeta(k, v)
}

// existsBlobMeta returns true if an entry for cid exists in the BLOB_META table.
func existsBlobMeta(ba *pebble.Batch, cid blobcache.CID) (bool, error) {
	return pdb.Exists(ba, pdb.TKey{TableID: dbtab.TID_BLOB_META, Key: cid[:]}.Marshal(nil), nil)
}

func mkBlobMetaFlags(hasSalt bool) uint8 {
	flags := uint8(0)
	if hasSalt {
		flags |= 1 << 0
	}
	return flags
}

type RefCount uint32

// blobRefCountIncr must be called with a lock on the blob.
// blob ref counts are the number of times a blob is referenced (excluding delete operations) in the LOCAL_VOLUME_BLOBS table.
func blobRefCountIncr(ba *pebble.Batch, cid blobcache.CID, delta int32) (RefCount, error) {
	k := pdb.TKey{TableID: dbtab.TID_BLOB_REF_COUNT, Key: cid[:16]}.Marshal(nil)
	n, err := pdb.IncrUint32(ba, k, delta, false)
	if err != nil {
		return 0, err
	}
	return RefCount(n), nil
}

// blobRefCountGet gets the reference count for a blob.
func blobRefCountGet(ba pdb.RO, cid blobcache.CID) (RefCount, error) {
	k := pdb.TKey{TableID: dbtab.TID_BLOB_REF_COUNT, Key: cid[:16]}.Marshal(nil)
	v, closer, err := ba.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(v) != 4 {
		return 0, fmt.Errorf("invalid value length: %d", len(v))
	}
	return RefCount(binary.BigEndian.Uint32(v)), nil
}

const (
	// volumeBlobFlag_OBSERVED means the blob was observed during the transaction and needs to stay.
	// This is set by the Exists and Get operations.
	volumeBlobFlag_OBSERVED = 0
	// volumeBlobFlag_ADDED means the blob was added during the transaction.
	// This is set by the Post and AddFrom operations.
	volumeBlobFlag_ADDED = 1 << 0
	// volumeBlobFlag_VISITED means the blob was visited during the transaction.
	// This is set by the Visit operation, and is only valid during a GC transaction.
	volumeBlobFlag_VISITED = 1 << 1
)
