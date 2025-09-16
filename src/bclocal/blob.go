package bclocal

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/cockroachdb/pebble"
)

type cidPrefix [16]byte

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
		TableID: tid_BLOB_META,
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
	k := pdb.TKey{TableID: tid_BLOB_META, Key: cid[:]}.Marshal(nil)
	v, closer, err := ba.Get(k)
	if err != nil {
		return blobMeta{}, err
	}
	defer closer.Close()
	return parseBlobMeta(k, v)
}

// existsBlobMeta returns true if an entry for cid exists in the BLOB_META table.
func existsBlobMeta(ba *pebble.Batch, cid blobcache.CID) (bool, error) {
	return pdb.Exists(ba, pdb.TKey{TableID: tid_BLOB_META, Key: cid[:]}.Marshal(nil), nil)
}

// blobPath returns the path to store the blob at in the filesystem.
func blobPath(cidp cidPrefix) string {
	return filepath.Join(
		hex.EncodeToString(cidp[0:1]),
		hex.EncodeToString(cidp[1:2]),

		hex.EncodeToString(cidp[2:]),
	)
}

// uploadBlob writes a blob to the filesystem.
// cid is not checked, and is assumed to be correct for data, possible with a salt.
func uploadBlob(blobDir *os.Root, cid blobcache.CID, data []byte) error {
	p := blobPath(cid)
	if err := blobDir.Mkdir(filepath.Dir(p), 0o755); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	f, err := blobDir.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// downloadBlob reads a blob from the filesystem.
func downloadBlob(blobDir *os.Root, cid blobcache.CID, buf []byte) (int, error) {
	p := blobPath(cid)
	f, err := blobDir.OpenFile(p, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, err
	}
	return n, nil
}

// dropExternalBlob drops a blob from the filesystem.
func dropBlobData(blobDir *os.Root, cidp cidPrefix) error {
	p := blobPath(cidp)
	return blobDir.Remove(p)
}

type RefCount uint32

// blobRefCountIncr must be called with a lock on the blob.
// blob ref counts are the number of times a blob is referenced (excluding delete operations) in the LOCAL_VOLUME_BLOBS table.
func blobRefCountIncr(ba *pebble.Batch, cidp cidPrefix, delta int32) (RefCount, error) {
	k := pdb.TKey{TableID: tid_BLOB_REF_COUNT, Key: cidp[:]}.Marshal(nil)
	n, err := pdb.IncrUint32(ba, k, delta, false)
	if err != nil {
		return 0, err
	}
	return RefCount(n), nil
}

// blobRefCountGet gets the reference count for a blob.
func blobRefCountGet(ba pdb.RO, cidp cidPrefix) (RefCount, error) {
	k := pdb.TKey{TableID: tid_BLOB_REF_COUNT, Key: cidp[:]}.Marshal(nil)
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
	volumeBlobFlag_ADDED   = 1 << 0
	volumeBlobFlag_VISITED = 1 << 1
)
