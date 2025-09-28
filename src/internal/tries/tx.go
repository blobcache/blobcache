package tries

import (
	"context"
	"slices"

	"blobcache.io/blobcache/src/schema"
)

type Tx struct {
	mach     *Machine
	prevRoot Root

	// changes to be applied to the prevRoot
	// if edits[k] == nil, then the key is being deleted.
	edits map[string][]byte
}

func (m *Machine) NewTx(prevRoot Root) *Tx {
	return &Tx{
		prevRoot: prevRoot,
		mach:     m,
	}
}

// Put creates or replace the entry for key, such that it points to value.
func (tx *Tx) Put(ctx context.Context, key []byte, value []byte) error {
	if len(value) == 0 {
		value = []byte{}
	}
	if tx.edits == nil {
		tx.edits = make(map[string][]byte)
	}
	tx.edits[string(key)] = slices.Clone(value)
	return nil
}

// Get retrieves a key from the store, and writes the value to dst.
func (tx *Tx) Get(ctx context.Context, s schema.RO, key []byte, dst *[]byte) error {
	if val, ok := tx.edits[string(key)]; ok {
		if val == nil {
			return ErrNotFound{Key: key}
		}
		*dst = append((*dst)[:0], val...)
		return nil
	}
	return tx.mach.Get(ctx, s, tx.prevRoot, key, dst)
}

func (tx *Tx) Delete(ctx context.Context, key []byte) error {
	if tx.edits == nil {
		tx.edits = make(map[string][]byte)
	}
	tx.edits[string(key)] = nil
	return nil
}

func (tx *Tx) Finish(ctx context.Context, s schema.RW) (*Root, error) {
	return &tx.prevRoot, nil
}
