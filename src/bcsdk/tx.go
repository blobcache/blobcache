package bcsdk

import (
	"context"
	"fmt"
	"sync/atomic"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/exp/slices2"
)

type (
	CID       = blobcache.CID
	OID       = blobcache.OID
	Handle    = blobcache.Handle
	ActionSet = blobcache.ActionSet
)

// BeginTx begins a new transaction and returns the Tx type.
func BeginTx(ctx context.Context, s blobcache.Service, volH blobcache.Handle, txp blobcache.TxParams) (*Tx, error) {
	txh, err := s.BeginTx(ctx, volH, txp)
	if err != nil {
		return nil, err
	}
	info, err := s.InspectVolume(ctx, volH)
	if err != nil {
		return nil, err
	}
	params := info.VolumeConfig
	if err := params.Validate(); err != nil {
		return nil, err
	}
	if params.MaxSize <= 0 {
		return nil, fmt.Errorf("max size must be positive")
	}
	return NewTx(s, *txh, params.HashAlgo, int(params.MaxSize)), nil
}

// Tx is a convenience type for managing a transaction within a Service.
type Tx struct {
	s       blobcache.Service
	h       blobcache.Handle
	hash    blobcache.HashAlgo
	hf      blobcache.HashFunc
	maxSize int

	done atomic.Bool
}

func NewTx(s blobcache.Service, h blobcache.Handle, hash blobcache.HashAlgo, maxSize int) *Tx {
	return &Tx{
		s:       s,
		h:       h,
		hash:    hash,
		hf:      hash.HashFunc(),
		maxSize: maxSize,
	}
}

func (tx *Tx) Inspect(ctx context.Context) (*blobcache.TxInfo, error) {
	return tx.s.InspectTx(ctx, tx.h)
}

func (tx *Tx) HashAlgo() blobcache.HashAlgo {
	return tx.hash
}

func (tx *Tx) Save(ctx context.Context, src []byte) error {
	return tx.s.Save(ctx, tx.h, src)
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	return tx.s.Load(ctx, tx.h, dst)
}

func (tx *Tx) Commit(ctx context.Context) error {
	if err := tx.checkDone(); err != nil {
		return err
	}
	err := tx.s.Commit(ctx, tx.h)
	tx.done.Store(true)
	return err
}

func (tx *Tx) Abort(ctx context.Context) error {
	if err := tx.checkDone(); err != nil {
		return err
	}
	err := tx.s.Abort(ctx, tx.h)
	tx.done.Store(true)
	return err
}

func (tx *Tx) KeepAlive(ctx context.Context) error {
	return tx.s.KeepAlive(ctx, []blobcache.Handle{tx.h})
}

func (tx *Tx) Post(ctx context.Context, data []byte) (CID, error) {
	return tx.s.Post(ctx, tx.h, data, blobcache.PostOpts{})
}

func (tx *Tx) Exists(ctx context.Context, cids []CID, exists []bool) error {
	return tx.s.Exists(ctx, tx.h, cids, exists)
}

func (tx *Tx) Delete(ctx context.Context, cids []CID) error {
	return tx.s.Delete(ctx, tx.h, cids)
}

func (tx *Tx) Get(ctx context.Context, cid CID, buf []byte) (int, error) {
	return tx.s.Get(ctx, tx.h, cid, buf, blobcache.GetOpts{})
}

func (tx *Tx) GetBytes(ctx context.Context, cid CID, hardMax int) ([]byte, error) {
	return GetBytes(ctx, tx.s, tx.h, cid, hardMax)
}

func (tx *Tx) Hash(data []byte) CID {
	return tx.hf(nil, data)
}

// MaxSize is the largest Blob that could be accepted or returned.
func (tx *Tx) MaxSize() int {
	return tx.maxSize
}

func (tx *Tx) Link(ctx context.Context, target blobcache.Handle, mask ActionSet) (*blobcache.LinkToken, error) {
	return tx.s.Link(ctx, tx.h, target, mask)
}

func (tx *Tx) Unlink(ctx context.Context, targets []blobcache.LinkToken) error {
	return tx.s.Unlink(ctx, tx.h, targets)
}

func (tx *Tx) VisitLinks(ctx context.Context, targets []blobcache.LinkToken) error {
	return tx.s.VisitLinks(ctx, tx.h, targets)
}

func (tx *Tx) Visit(ctx context.Context, cids []CID) error {
	return tx.s.Visit(ctx, tx.h, cids)
}

func (tx *Tx) IsVisited(ctx context.Context, cids []CID, yesVisited []bool) error {
	return tx.s.IsVisited(ctx, tx.h, cids, yesVisited)
}

func (tx *Tx) Copy(ctx context.Context, srcs []*Tx, cids []CID, success []bool) error {
	hs := slices2.Map(srcs, func(src *Tx) Handle { return src.h })
	return tx.s.Copy(ctx, tx.h, hs, cids, success)
}

func (tx *Tx) checkDone() error {
	if tx.done.Load() {
		return blobcache.ErrTxDone{ID: tx.h.OID}
	}
	return nil
}

// BeginTxSalt is the salted variant of BeginTx.
func BeginTxSalt(ctx context.Context, s blobcache.Service, volH Handle, txp blobcache.TxParams) (*TxSalt, error) {
	txh, err := s.BeginTx(ctx, volH, txp)
	if err != nil {
		return nil, err
	}
	info, err := s.InspectVolume(ctx, volH)
	if err != nil {
		return nil, err
	}
	params := info.VolumeConfig
	if err := params.Validate(); err != nil {
		return nil, err
	}
	return NewTxSalt(s, *txh, params.HashAlgo, int(params.MaxSize)), nil
}

// TxSalt is a convenience type for managing a salted transaction within a Service.
type TxSalt struct {
	s       blobcache.Service
	h       Handle
	hash    blobcache.HashAlgo
	hf      blobcache.HashFunc
	maxSize int

	done bool
}

func NewTxSalt(s blobcache.Service, h Handle, hash blobcache.HashAlgo, maxSize int) *TxSalt {
	return &TxSalt{
		s:       s,
		h:       h,
		hash:    hash,
		hf:      hash.HashFunc(),
		maxSize: maxSize,
	}
}

func (tx *TxSalt) Inspect(ctx context.Context) (*blobcache.TxInfo, error) {
	return tx.s.InspectTx(ctx, tx.h)
}

func (tx *TxSalt) HashAlgo() blobcache.HashAlgo {
	return tx.hash
}

func (tx *TxSalt) Load(ctx context.Context, dst *[]byte) error {
	return tx.s.Load(ctx, tx.h, dst)
}

func (tx *TxSalt) Save(ctx context.Context, src []byte) error {
	return tx.s.Save(ctx, tx.h, src)
}

func (tx *TxSalt) Commit(ctx context.Context) error {
	if tx.done {
		return blobcache.ErrTxDone{ID: tx.h.OID}
	}
	err := tx.s.Commit(ctx, tx.h)
	tx.done = true
	return err
}

func (tx *TxSalt) Abort(ctx context.Context) error {
	if tx.done {
		return blobcache.ErrTxDone{ID: tx.h.OID}
	}
	err := tx.s.Abort(ctx, tx.h)
	tx.done = true
	return err
}

func (tx *TxSalt) KeepAlive(ctx context.Context) error {
	return tx.s.KeepAlive(ctx, []Handle{tx.h})
}

func (tx *TxSalt) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (CID, error) {
	return tx.s.Post(ctx, tx.h, data, opts)
}

func (tx *TxSalt) Exists(ctx context.Context, cid CID) (bool, error) {
	return ExistsSingle(ctx, tx.s, tx.h, cid)
}

func (tx *TxSalt) Delete(ctx context.Context, cid CID) error {
	return tx.s.Delete(ctx, tx.h, []CID{cid})
}

func (tx *TxSalt) Get(ctx context.Context, cid CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return tx.s.Get(ctx, tx.h, cid, buf, opts)
}

func (tx *TxSalt) Hash(salt *CID, data []byte) CID {
	return tx.hf(salt, data)
}

func (tx *TxSalt) MaxSize() int {
	return tx.maxSize
}

func View(ctx context.Context, svc blobcache.Service, volh blobcache.Handle, fn func(s RO, root []byte) error) error {
	tx, err := BeginTx(ctx, svc, volh, blobcache.TxParams{
		Modify: false,
	})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	return fn(tx, root)
}

func View1[T any](ctx context.Context, svc blobcache.Service, volh blobcache.Handle, fn func(s RO, root []byte) (T, error)) (T, error) {
	var zero T
	tx, err := BeginTx(ctx, svc, volh, blobcache.TxParams{
		Modify: false,
	})
	if err != nil {
		return zero, err
	}
	defer tx.Abort(ctx)
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return zero, err
	}
	return fn(tx, root)
}

// Modify performs a modifying transaction on the volume.
func Modify(ctx context.Context, svc blobcache.Service, volh blobcache.Handle, fn func(s RW, root []byte) ([]byte, error)) error {
	return ModifyTx(ctx, svc, volh, func(tx *Tx, root []byte) ([]byte, error) {
		return fn(tx, root)
	})
}

func ModifyTx(ctx context.Context, svc blobcache.Service, volh blobcache.Handle, fn func(tx *Tx, root []byte) ([]byte, error)) error {
	tx, err := BeginTx(ctx, svc, volh, blobcache.TxParams{
		Modify: true,
	})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var prev []byte
	if err := tx.Load(ctx, &prev); err != nil {
		return err
	}
	next, err := fn(tx, prev)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, next); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ExistsSingle is a convenience function for checking if a single CID exists using the slice based API.
func ExistsSingle(ctx context.Context, s interface {
	Exists(ctx context.Context, txh Handle, cids []CID, dst []bool) error
}, txh Handle, cid CID) (bool, error) {
	var dst [1]bool
	if err := s.Exists(ctx, txh, []CID{cid}, dst[:]); err != nil {
		return false, err
	}
	return dst[0], nil
}
