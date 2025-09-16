package bclocal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"github.com/cockroachdb/pebble"
)

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
func (txs *txSystem) allocateTxID() (pdb.MVTag, error) {
	txs.mu.Lock()
	defer txs.mu.Unlock()
	ba := txs.db.NewIndexedBatch()
	defer ba.Close()
	txid, err := pdb.IncrUint64(ba, seqLastTx, 1)
	if err != nil {
		return 0, err
	}
	if err := ba.Commit(nil); err != nil {
		return 0, err
	}
	return pdb.MVTag(txid), nil
}

// start marks a transaction as active.
func (txs *txSystem) start(ba pdb.WO, txid pdb.MVTag) error {
	k := pdb.TKey{
		TableID: tid_SYS_TXNS,
		Key:     binary.BigEndian.AppendUint64(nil, uint64(txid)),
	}
	return ba.Set(k.Marshal(nil), nil, nil)
}

// success marks a transaction as finished and removes the row from the active set.
func (txs *txSystem) success(ba pdb.WO, txid pdb.MVTag) error {
	return ba.Delete(sysTxnKey(nil, txid), nil)
}

// failure marks a transaction as failed.
// The transaction stays in the active set.
func (txs *txSystem) failure(ba pdb.WO, txid pdb.MVTag) error {
	return ba.Set(sysTxnKey(nil, txid), []byte{0x01}, nil)
}

// removeFailed removes a failed transaction from the active set.
// This should only be called after all the rows written by the transaction have been cleaned up.
func (txs *txSystem) removeFailed(ba pdb.WO, txid pdb.MVTag) error {
	return ba.Delete(sysTxnKey(nil, txid), nil)
}

func (txs *txSystem) readActive(sp pdb.RO, dst map[pdb.MVTag]struct{}) error {
	return readActive(sp, dst)
}

func (txs *txSystem) isActive(sn pdb.RO, txid pdb.MVTag) (bool, error) {
	_, closer, err := sn.Get(sysTxnKey(nil, txid))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	return closer != nil, nil
}

// readActiveSysTxns reads the active transactions
// The caller should clear(dst) before calling.
func readActive(sp pdb.RO, dst map[pdb.MVTag]struct{}) error {
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
		mvid := pdb.MVTag(binary.BigEndian.Uint64(k.Key))
		dst[mvid] = struct{}{}
	}
	return nil
}

// seqLastTx is the key for the last transaction ID given out.
var seqLastTx = sysTxnKey(nil, 0)

func sysTxnKey(out []byte, txid pdb.MVTag) []byte {
	k := pdb.TKey{
		TableID: tid_SYS_TXNS,
		Key:     binary.BigEndian.AppendUint64(nil, uint64(txid)),
	}
	return k.Marshal(out)
}
