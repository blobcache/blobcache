// Package radixkv implements a radix tree key-value store in
// a Blobcache volume.
package radixkv

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries"
)

type (
	// Machine makes Schema-consistent changes to a Volume.
	// It holds caches and configuration
	Machine = tries.Machine
	// Tx is used to make a set of changes to a radixkv
	// within a single transaction.
	Tx = tries.Tx

	// Root transitively contains all information representing a key-value store.
	Root     = tries.Root
	Iterator = tries.Iterator
	Entry    = tries.Entry
	Span     = tries.Span
	Op       = tries.Op
)

// New constructs a new Machine
func New(salt *blobcache.CID, hf blobcache.HashFunc) *Machine {
	if salt == nil {
		panic("salt cannot be nil, use new(blobcache.CID) for an all zero salt")
	}
	return tries.NewMachine(salt, hf)
}

func Modify(ctx context.Context, svc blobcache.Service, volh blobcache.Handle, mach *Machine, fn func(*Tx) error) error {
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{Modify: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	kvtx, err := wrapTx(ctx, mach, tx)
	if err != nil {
		return err
	}
	if err := fn(kvtx); err != nil {
		return err
	}
	nextRoot, err := kvtx.Flush(ctx)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, nextRoot.Marshal(nil)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// View opens a read-only transaction on the volume, wraps the transaction
// With a radixkv transaction, then calls fn with it.
// The transaction is cleanup when the function returns.
func View(ctx context.Context, svc blobcache.Service, volh blobcache.Handle, mach *Machine, fn func(*Tx) error) error {
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	kvtx, err := wrapTx(ctx, mach, tx)
	if err != nil {
		return err
	}
	if err := fn(kvtx); err != nil {
		return err
	}
	return nil
}

func wrapTx(ctx context.Context, mach *Machine, tx *bcsdk.Tx) (*Tx, error) {
	var rootData []byte
	if err := tx.Load(ctx, &rootData); err != nil {
		return nil, err
	}
	if len(rootData) == 0 {
		return mach.NewTxOnEmpty(tx), nil
	}
	var root Root
	if err := root.Unmarshal(rootData); err != nil {
		return nil, err
	}
	return mach.NewTx(tx, root), nil
}
