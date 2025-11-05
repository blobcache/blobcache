package localvol

import (
	"bytes"
	"crypto/aes"
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
)

// ID uniquely identifies a local volume
type ID uint64

func ParseLocalID(k []byte) (ID, error) {
	if len(k) != 8 {
		return 0, fmt.Errorf("local ID key too short: %d", len(k))
	}
	return ID(binary.BigEndian.Uint64(k[:8])), nil
}

func (lvid ID) Marshal(out []byte) []byte {
	return binary.BigEndian.AppendUint64(out, uint64(lvid))
}

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

// OIDFromLocalID maps a local volume ID to a Blobcache OID
func OIDFromLocalID(x ID) blobcache.OID {
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

// LocalIDFromOID maps an OID to a LocalID
// It reverses the mapping used by OIDFromLocalID
func LocalIDFromOID(oid blobcache.OID) (ID, error) {
	if oid == (blobcache.OID{}) {
		return 0, nil
	}
	var buf [16]byte
	var key [16]byte
	aesDecrypt(&key, oid[:], buf[:])
	if !bytes.Equal(buf[:8], buf[8:16]) {
		return 0, fmt.Errorf("OID is not for a local volume")
	}
	return ID(binary.BigEndian.Uint64(buf[0:8])), nil
}

// putLocalVolumeTxn adds an entry to the LOCAL_VOLUME_TXNS table.
func putLocalVolumeTxn(ba pdb.WO, volID ID, txid pdb.MVTag) error {
	return pdb.TablePut(ba, dbtab.TID_LOCAL_VOLUME_TXNS, volID.Marshal(nil), binary.BigEndian.AppendUint64(nil, uint64(txid)))
}

// deleteLocalVolumeTxn deletes from the LOCAL_VOLUME_TXNS table.
func deleteLocalVolumeTxn(ba pdb.WO, volID ID, txid pdb.MVTag) error {
	return pdb.TableDelete(ba, dbtab.TID_LOCAL_VOLUME_TXNS, volID.Marshal(nil))
}
