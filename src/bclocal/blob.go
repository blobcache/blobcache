package bclocal

import (
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

// putBlobData puts a blob into the database.
func putBlobData(ba *pebble.Batch, cid blobcache.CID, data []byte) error {
	return ba.Set(pdb.TKey{TableID: tid_BLOB_DATA, Key: cid[:]}.Marshal(nil), data, nil)
}

// deleteBlobData deletes a blob from the database.
func deleteBlobData(ba *pebble.Batch, cid blobcache.CID) error {
	return ba.Delete(pdb.TKey{TableID: tid_BLOB_DATA, Key: cid[:]}.Marshal(nil), nil)
}

// readBlobData reads a blob from the database into buf.
func readBlobData(r pdb.RO, cid blobcache.CID, buf []byte) (int, error) {
	data, closer, err := r.Get(pdb.TKey{TableID: tid_BLOB_DATA, Key: cid[:]}.Marshal(nil))
	if err != nil {
		return 0, fmt.Errorf("readBlobData: %w", err)
	}
	defer closer.Close()
	return copy(buf, data), nil
}

// blobMeta is a row in the BLOB_META table.
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

func putBlobMeta(ba *pebble.Batch, bi blobMeta) error {
	return ba.Set(bi.Key(nil), bi.Value(nil), nil)
}

func getBlobMeta(ba *pebble.Batch, cid blobcache.CID) (blobMeta, error) {
	k := pdb.TKey{TableID: tid_BLOB_META, Key: cid[:]}.Marshal(nil)
	v, closer, err := ba.Get(k)
	if err != nil {
		return blobMeta{}, err
	}
	defer closer.Close()
	return parseBlobMeta(k, v)
}

func existsBlobMeta(ba *pebble.Batch, cid blobcache.CID) (bool, error) {
	k := pdb.TKey{TableID: tid_BLOB_META, Key: cid[:]}.Marshal(nil)
	_, closer, err := ba.Get(k)
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

func dropExternalBlob(blobDir *os.Root, cid blobcache.CID) error {
	p := blobPath(cid)
	return blobDir.Remove(p)
}
