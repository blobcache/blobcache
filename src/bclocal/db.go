package bclocal

import (
	"bytes"
	"crypto/aes"
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/cockroachdb/pebble"
)

const (
	tid_UNKNOWN = pdb.TableID(iota)

	// tid_SYS_TXNS holds the transaction sequence number, and set of active transactions.
	// This is for database-wide transactions, rather than Volume transactions
	// although one is implemented using the other.
	tid_SYS_TXNS
)

const (
	// tid_BLOB_META holds blob metadata.
	tid_BLOB_META = pdb.TableID(1*256 + iota)
	// tid_BLOB_REF_COUNT holds the reference count for a blob.
	tid_BLOB_REF_COUNT
)

const (
	// tid_VOLUMES holds volume information.
	// This includes all volume information, not just local volumes.
	tid_VOLUMES = pdb.TableID(2*256 + iota)
	// tid_VOLUME_DEPS holds the dependencies for a volume.
	// These are not user-defined relationships
	// Volumes that wrap other volumes will reference those volumes as dependencies.
	tid_VOLUME_DEPS
	// tid_VOLUME_DEPS_INV holds the inverse of the VOLUME_DEPS table.
	tid_VOLUME_DEPS_INV
	// tid_VOLUME_LINKS holds links from one volume to another.
	// These are user-defined relationships, provided through the AllowLink method on transactions.
	tid_VOLUME_LINKS
	// tid_VOLUME_LINKS_INV holds the inverse of the VOLUME_LINKS table.
	tid_VOLUME_LINKS_INV
)

const (
	// tid_LOCAL_VOLUME_TXNS holds active transactions on local volumes.
	tid_LOCAL_VOLUME_TXNS = pdb.TableID(3*256 + iota)
	// tid_LOCAL_VOLUME_CELLS holds the root data for a local volume.
	// This table uses the MVCC keys
	tid_LOCAL_VOLUME_CELLS
	// tid_LOCAL_VOLUME_BLOBS holds the blobs for a local volume.
	// This table uses the MVCC keys.
	tid_LOCAL_VOLUME_BLOBS
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

var allOnesOID = blobcache.OID(bytes.Repeat([]byte{0xff}, blobcache.OIDSize))

func aesEncrypt(key *[16]byte, src, dst []byte) {
	ciph, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	ciph.Encrypt(dst, src)
}

func aesDecrypt(key *[16]byte, src, dst []byte) {
	ciph, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	ciph.Decrypt(dst, src)
}

func oidFromLocalID(x LocalVolumeID) blobcache.OID {
	if x == 0 {
		// return the root volume ID.
		return blobcache.OID{}
	}
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(x))
	binary.BigEndian.PutUint64(buf[8:16], uint64(x))

	var key [16]byte
	aesEncrypt(&key, buf[:], buf[:])
	return blobcache.OID(buf)
}

func localVolumeIDFromOID(oid blobcache.OID) (LocalVolumeID, error) {
	if oid == (blobcache.OID{}) {
		return 0, nil
	}
	var buf [16]byte
	var key [16]byte
	aesDecrypt(&key, oid[:], buf[:])
	if !bytes.Equal(buf[:8], buf[8:16]) {
		return 0, fmt.Errorf("OID is not for a local volume")
	}
	return LocalVolumeID(binary.BigEndian.Uint64(buf[0:8])), nil
}

// putLocalVolumeTxn adds an entry to the LOCAL_VOLUME_TXNS table.
func putLocalVolumeTxn(ba pdb.WO, volID LocalVolumeID, txid pdb.MVTag) error {
	return pdb.TablePut(ba, tid_LOCAL_VOLUME_TXNS, volID.Marshal(nil), binary.BigEndian.AppendUint64(nil, uint64(txid)))
}

// deleteLocalVolumeTxn deletes from the LOCAL_VOLUME_TXNS table.
func deleteLocalVolumeTxn(ba pdb.WO, volID LocalVolumeID, txid pdb.MVTag) error {
	return pdb.TableDelete(ba, tid_LOCAL_VOLUME_TXNS, volID.Marshal(nil))
}
