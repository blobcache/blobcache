package blobcache

import (
	"context"
	"fmt"
)

// BeginTx begins a new transaction and returns the Tx type.
func BeginTx(ctx context.Context, s Service, volH Handle, txp TxParams) (*Tx, error) {
	txh, err := s.BeginTx(ctx, volH, txp)
	if err != nil {
		return nil, err
	}
	info, err := s.InspectVolume(ctx, volH)
	if err != nil {
		return nil, err
	}
	if err := info.HashAlgo.Validate(); err != nil {
		return nil, err
	}
	if info.MaxSize <= 0 {
		return nil, fmt.Errorf("max size must be positive")
	}
	return NewTx(s, *txh, info.HashAlgo.HashFunc(), int(info.MaxSize)), nil
}

// Tx is a convenience type for managing a transaction within a Service.
type Tx struct {
	s       Service
	h       Handle
	hash    HashFunc
	maxSize int

	done bool
}

func NewTx(s Service, h Handle, hash HashFunc, maxSize int) *Tx {
	return &Tx{
		s:       s,
		h:       h,
		hash:    hash,
		maxSize: maxSize,
	}
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	return tx.s.Load(ctx, tx.h, dst)
}

func (tx *Tx) Commit(ctx context.Context, root []byte) error {
	if tx.done {
		return ErrTxDone{ID: tx.h.OID}
	}
	err := tx.s.Commit(ctx, tx.h, root)
	tx.done = true
	return err
}

func (tx *Tx) Abort(ctx context.Context) error {
	if tx.done {
		return ErrTxDone{ID: tx.h.OID}
	}
	err := tx.s.Abort(ctx, tx.h)
	tx.done = true
	return err
}

func (tx *Tx) KeepAlive(ctx context.Context) error {
	return tx.s.KeepAlive(ctx, []Handle{tx.h})
}

func (tx *Tx) Post(ctx context.Context, data []byte) (CID, error) {
	return tx.s.Post(ctx, tx.h, nil, data)
}

func (tx *Tx) Exists(ctx context.Context, cid CID) (bool, error) {
	return tx.s.Exists(ctx, tx.h, cid)
}

func (tx *Tx) Delete(ctx context.Context, cid CID) error {
	return tx.s.Delete(ctx, tx.h, cid)
}

func (tx *Tx) Get(ctx context.Context, cid CID, buf []byte) (int, error) {
	return tx.s.Get(ctx, tx.h, cid, nil, buf)
}

func (tx *Tx) Hash(data []byte) CID {
	return tx.hash(nil, data)
}

func (tx *Tx) MaxSize() int {
	return tx.maxSize
}

func (tx *Tx) AllowLink(ctx context.Context, target Handle) error {
	return tx.s.AllowLink(ctx, tx.h, target)
}

// BeginTxSalt is the salted variant of BeginTx.
func BeginTxSalt(ctx context.Context, s Service, volH Handle, txp TxParams) (*TxSalt, error) {
	txh, err := s.BeginTx(ctx, volH, txp)
	if err != nil {
		return nil, err
	}
	info, err := s.InspectVolume(ctx, volH)
	if err != nil {
		return nil, err
	}
	return NewTxSalt(s, *txh, info.HashAlgo.HashFunc(), int(info.MaxSize)), nil
}

// TxSalt is a convenience type for managing a salted transaction within a Service.
type TxSalt struct {
	s       Service
	h       Handle
	hash    HashFunc
	maxSize int

	done bool
}

func NewTxSalt(s Service, h Handle, hash HashFunc, maxSize int) *TxSalt {
	return &TxSalt{
		s:       s,
		h:       h,
		hash:    hash,
		maxSize: maxSize,
	}
}

func (tx *TxSalt) Load(ctx context.Context, dst *[]byte) error {
	return tx.s.Load(ctx, tx.h, dst)
}

func (tx *TxSalt) Commit(ctx context.Context, root []byte) error {
	if tx.done {
		return ErrTxDone{ID: tx.h.OID}
	}
	err := tx.s.Commit(ctx, tx.h, root)
	tx.done = true
	return err
}

func (tx *TxSalt) Abort(ctx context.Context) error {
	if tx.done {
		return ErrTxDone{ID: tx.h.OID}
	}
	err := tx.s.Abort(ctx, tx.h)
	tx.done = true
	return err
}

func (tx *TxSalt) KeepAlive(ctx context.Context) error {
	return tx.s.KeepAlive(ctx, []Handle{tx.h})
}

func (tx *TxSalt) Post(ctx context.Context, salt *CID, data []byte) (CID, error) {
	return tx.s.Post(ctx, tx.h, salt, data)
}

func (tx *TxSalt) Exists(ctx context.Context, cid CID) (bool, error) {
	return tx.s.Exists(ctx, tx.h, cid)
}

func (tx *TxSalt) Delete(ctx context.Context, cid CID) error {
	return tx.s.Delete(ctx, tx.h, cid)
}

func (tx *TxSalt) Get(ctx context.Context, cid CID, buf []byte) (int, error) {
	return tx.s.Get(ctx, tx.h, cid, nil, buf)
}

func (tx *TxSalt) Hash(salt *CID, data []byte) CID {
	return tx.hash(salt, data)
}

func (tx *TxSalt) MaxSize() int {
	return tx.maxSize
}
