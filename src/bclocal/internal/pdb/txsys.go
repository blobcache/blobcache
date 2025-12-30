package pdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

// TxSys manages the transaction sequence number, and the set of active transactions.
type TxSys struct {
	db    *pebble.DB
	table TableID
	mu    sync.Mutex
}

func NewTxSys(db *pebble.DB, tabid TableID) TxSys {
	return TxSys{
		db:    db,
		table: tabid,
	}
}

// allocateTxID allocates a new transaction ID.
// It is guaranteed to be unique.
// If the ID is lost and never used, that's fine.
func (txs *TxSys) AllocateTxID() (MVTag, error) {
	txs.mu.Lock()
	defer txs.mu.Unlock()
	ba := txs.db.NewIndexedBatch()
	defer ba.Close()
	txid, err := IncrUint64(ba, txs.seqLastTx(), 1)
	if err != nil {
		return 0, err
	}
	if err := ba.Commit(nil); err != nil {
		return 0, err
	}
	return MVTag(txid), nil
}

// start marks a transaction as active.
func (txs *TxSys) start(ba WO, txid MVTag) error {
	k := TKey{
		TableID: txs.table,
		Key:     binary.BigEndian.AppendUint64(nil, uint64(txid)),
	}
	return ba.Set(k.Marshal(nil), nil, nil)
}

// success marks a transaction as finished and removes the row from the active set.
func (txs *TxSys) Success(ba WO, txid MVTag) error {
	return ba.Delete(txs.sysTxnKey(nil, txid), nil)
}

// failure marks a transaction as failed.
// The transaction stays in the active set.
func (txs *TxSys) Failure(ba WO, txid MVTag) error {
	return ba.Set(txs.sysTxnKey(nil, txid), []byte{0x01}, nil)
}

// removeFailed removes a failed transaction from the active set.
// This should only be called after all the rows written by the transaction have been cleaned up.
func (txs *TxSys) RemoveFailed(ba WO, txid MVTag) error {
	return ba.Delete(txs.sysTxnKey(nil, txid), nil)
}

func (txs *TxSys) ReadActive(sp RO, dst map[MVTag]struct{}) error {
	return txs.readActive(sp, dst)
}

func (txs *TxSys) IsActive(sn RO, txid MVTag) (bool, error) {
	_, closer, err := sn.Get(txs.sysTxnKey(nil, txid))
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
func (txs *TxSys) readActive(sp RO, dst map[MVTag]struct{}) error {
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: TKey{TableID: txs.table, Key: binary.BigEndian.AppendUint64(nil, 1)}.Marshal(nil),
		UpperBound: TableUpperBound(txs.table),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := ParseTKey(iter.Key())
		if err != nil {
			return err
		}
		if len(k.Key) != 8 {
			return fmt.Errorf("active transaction key is not a valid MVID: %d", len(k.Key))
		}
		mvid := MVTag(binary.BigEndian.Uint64(k.Key))
		dst[mvid] = struct{}{}
	}
	return nil
}

func (txs *TxSys) sysTxnKey(out []byte, txid MVTag) []byte {
	k := TKey{
		TableID: txs.table,
		Key:     binary.BigEndian.AppendUint64(nil, uint64(txid)),
	}
	return k.Marshal(out)
}

func (txs *TxSys) seqLastTx() []byte {
	return txs.sysTxnKey(nil, 0)
}
