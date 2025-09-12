package bclocal

import (
	"bytes"
	"crypto/aes"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"

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

	// tid_BLOB_DATA holds blob contents.
	tid_BLOB_DATA
	// tid_BLOB_META holds blob metadata.
	tid_BLOB_META

	// tid_VOLUMES holds volume information.
	// This includes all volume information, not just local volumes.
	tid_VOLUMES
	// tid_VOLUME_DEPS holds the dependencies for a volume.
	// These are not user-defined relationships
	// Volumes that wrap other volumes will reference those volumes as dependencies.
	tid_VOLUME_DEPS
	// tid_VOLUME_DEPS_INV holds the inverse of the VOLUME_DEPS table.
	tid_VOLUME_DEPS_INV
	// tid_VOLUME_LINKS holds links from one volume to another.
	tid_VOLUME_LINKS
	// tid_VOLUME_LINKS_INV holds the inverse of the VOLUME_LINKS table.
	tid_VOLUME_LINKS_INV

	// tid_LOCAL_VOLUME_TXNS holds active transactions on local volumes.
	tid_LOCAL_VOLUME_TXNS
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
	return ba.Commit(pebble.Sync)
}

// seqLastTx is the key for the last transaction committed.
var seqLastTx = pdb.TKey{
	TableID: tid_SYS_TXNS,
	Key:     binary.BigEndian.AppendUint64(nil, 0),
}.Marshal(nil)

// txSystem manages the transaction sequence number, and the set of active transactions.
type txSystem struct {
	db *pebble.DB
	mu sync.Mutex
}

func newTxSystem(db *pebble.DB) txSystem {
	return txSystem{
		db: db,
	}
}

// allocateTxID allocates a new transaction ID.
// It is guaranteed to be unique.
// If the ID is lost and never used, that's fine.
func (txs *txSystem) allocateTxID() (pdb.MVID, error) {
	txs.mu.Lock()
	defer txs.mu.Unlock()
	ba := txs.db.NewIndexedBatch()
	defer ba.Close()
	txid, err := pdb.IncrUint64(ba, seqLastTx, 1)
	if err != nil {
		return 0, err
	}
	if err := ba.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	return pdb.MVID(txid), nil
}

// markActive marks a transaction as active.
func (txs *txSystem) markActive(ba pdb.WO, txid pdb.MVID) error {
	k := pdb.TKey{
		TableID: tid_SYS_TXNS,
		Key:     binary.BigEndian.AppendUint64(nil, uint64(txid)),
	}
	return ba.Set(k.Marshal(nil), nil, pebble.Sync)
}

// removeActive removes a transaction from the active set.
// For unsuccessful transactions (rollback): any rows written, must be removed from the database before calling this.
func (txs *txSystem) removeActive(ba pdb.WO, txid pdb.MVID) error {
	return removeActive(ba, txid)
}

func removeActive(ba pdb.WO, txid pdb.MVID) error {
	k := pdb.TKey{
		TableID: tid_SYS_TXNS,
		Key:     binary.BigEndian.AppendUint64(nil, uint64(txid)),
	}
	return ba.Delete(k.Marshal(nil), nil)
}

func (txs *txSystem) readActive(sp pdb.RO, dst map[pdb.MVID]struct{}) error {
	return readActive(sp, dst)
}

// readActiveSysTxns reads the active transactions
// The caller should clear(dst) before calling.
func readActive(sp pdb.RO, dst map[pdb.MVID]struct{}) error {
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: pdb.TKey{TableID: tid_SYS_TXNS, Key: binary.BigEndian.AppendUint64(nil, 1)}.Marshal(nil),
		UpperBound: pdb.TableUpperBound(tid_SYS_TXNS),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := pdb.ParseTKey(iter.Key())
		if err != nil {
			return err
		}
		if len(k.Key) != 8 {
			return fmt.Errorf("active transaction key is not a valid MVID: %d", len(k.Key))
		}
		mvid := pdb.MVID(binary.BigEndian.Uint64(k.Key))
		dst[mvid] = struct{}{}
	}
	return nil
}

var allOnesOID = blobcache.OID(bytes.Repeat([]byte{0xff}, blobcache.OIDSize))

func volumeBlobKey(volID blobcache.OID, cid blobcache.CID, mvc pdb.MVID) pdb.MVKey {
	return pdb.MVKey{
		TableID: tid_LOCAL_VOLUME_BLOBS,
		Key:     slices.Concat(volID[:], cid[:]),
		Version: mvc,
	}
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

func oidFromUint64(x uint64) blobcache.OID {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], x)
	binary.BigEndian.PutUint64(buf[8:16], x)

	var key [16]byte
	aesEncrypt(&key, buf[:], buf[:])
	return blobcache.OID(buf)
}

func uint64FromOID(oid blobcache.OID) (uint64, error) {
	var buf [16]byte
	var key [16]byte
	aesDecrypt(&key, oid[:], buf[:])
	if !bytes.Equal(buf[:8], buf[8:16]) {
		return 0, fmt.Errorf("OID is not for a local volume")
	}
	return binary.BigEndian.Uint64(buf[0:8]), nil
}
