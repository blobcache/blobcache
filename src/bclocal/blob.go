package bclocal

import (
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cockroachdb/pebble"
)

// uploadBlob writes a blob to the filesystem.
// cid is not checked, and is assumed to be correct for data, possible with a salt.
func uploadBlob(blobDir *os.Root, cid blobcache.CID, data []byte) error {
	p := blobPath(cid)
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
	for len(buf) > 0 {
		n, err := f.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, nil
			}
			return 0, err
		}
		if n == 0 {
			return n, nil
		}
		buf = buf[n:]
	}
	return 0, io.ErrShortBuffer
}

func dropBlob(db *pebble.DB, blobDir *os.Root, cid blobcache.CID) error {
	p := hex.EncodeToString(cid[:])
	return os.Remove(p)
}

// putBlobData puts a blob into the database.
func putBlobData(ba *pebble.Batch, cid blobcache.CID, data []byte) error {
	return ba.Set(tableKey(nil, tid_BLOB_DATA, cid[:]), data, nil)
}

// deleteBlobData deletes a blob from the database.
func deleteBlobData(ba *pebble.Batch, cid blobcache.CID) error {
	return ba.Delete(tableKey(nil, tid_BLOB_DATA, cid[:]), nil)
}

type blobMeta struct {
	cid blobcache.CID

	flags    uint8
	salt     *blobcache.CID
	size     uint32
	refCount uint32
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
	return tableKey(out, tid_BLOB_META, bi.cid[:])
}

func (bi blobMeta) Value(out []byte) []byte {
	out = append(out, bi.flags)
	if bi.salt != nil {
		out = append(out, 1)
		out = append(out, bi.salt[:]...)
	}
	return out
}

func putBlobMeta(ba *pebble.Batch, bi blobMeta) error {
	return ba.Set(bi.Key(nil), bi.Value(nil), nil)
}

func haveBlobMeta(ba *pebble.Batch, cid blobcache.CID) (bool, error) {
	_, closer, err := ba.Get(tableKey(nil, tid_BLOB_META, cid[:]))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	return true, nil
}

// blobPath returns the path to store the blob at in the filesystem.
func blobPath(cid blobcache.CID) string {
	return filepath.Join(
		hex.EncodeToString(cid[:1]),
		hex.EncodeToString(cid[1:]),
	)
}
