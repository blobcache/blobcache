package tries

import (
	"context"
	"maps"

	"blobcache.io/blobcache/src/schema"
)

// Tx is a transaction on a trie data structure.
// Tx buffers operations, and presents a logical view of the trie.
// Tx is not thread-safe.
type Tx struct {
	mach     *Machine
	prevRoot Root

	// changes to be applied to the prevRoot
	// if edits[k] == nil, then the key is being deleted.
	edits map[string]Op
}

func (m *Machine) NewTx(prevRoot Root) *Tx {
	return &Tx{
		prevRoot: prevRoot,
		mach:     m,
	}
}

func (m *Machine) NewTxOnEmpty(ctx context.Context, s schema.WO) (*Tx, error) {
	root, err := m.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	return m.NewTx(*root), nil
}

// Put creates or replace the entry for key, such that it points to value.
func (tx *Tx) Put(ctx context.Context, s schema.RW, key []byte, value []byte) error {
	if len(value) == 0 {
		value = []byte{}
	}
	if tx.edits == nil {
		tx.edits = make(map[string]Op)
	}
	tx.edits[string(key)] = OpPut(key, value)
	return nil
}

// Get retrieves a key from the store, and writes the value to dst.
func (tx *Tx) Get(ctx context.Context, s schema.RO, key []byte, dst *[]byte) error {
	if val, ok := tx.edits[string(key)]; ok {
		if val.IsDelete() {
			return ErrNotFound{Key: key}
		}
		*dst = append((*dst)[:0], val.Value...)
		return nil
	}
	return tx.mach.Get(ctx, s, tx.prevRoot, key, dst)
}

func (tx *Tx) Delete(ctx context.Context, key []byte) error {
	if tx.edits == nil {
		tx.edits = make(map[string]Op)
	}
	tx.edits[string(key)] = OpDelete(key)
	return nil
}

func (tx *Tx) Queued() int {
	return len(tx.edits)
}

// Flush applies the edits to the previous root, and returns the new root.
// The transaction can continue, after this.
func (tx *Tx) Flush(ctx context.Context, s schema.RW) (*Root, error) {
	root, err := tx.mach.BatchEdit(ctx, s, tx.prevRoot, maps.Values(tx.edits))
	if err != nil {
		return nil, err
	}
	clear(tx.edits)
	tx.prevRoot = *root
	return &tx.prevRoot, nil
}
