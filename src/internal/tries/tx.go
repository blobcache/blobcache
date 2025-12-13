package tries

import (
	"context"
	"maps"

	"blobcache.io/blobcache/src/bcsdk"
)

// Tx is a transaction on a trie data structure.
// Tx buffers operations, and presents a logical view of the trie.
// Tx is not thread-safe.
type Tx struct {
	mach     *Machine
	s        bcsdk.RWD
	prevRoot Root

	// changes to be applied to the prevRoot
	// if edits[k].Value == nil, then the key is being deleted.
	edits map[string]Op
}

// NewTx creates a new transaction based on an existing Trie
func (mach *Machine) NewTx(s bcsdk.RWD, prevRoot Root) *Tx {
	return &Tx{
		mach:     mach,
		s:        s,
		prevRoot: prevRoot,
	}
}

// NewTxOnEmpty creates a new Tx based on an empty Trie.
func (mach *Machine) NewTxOnEmpty(s bcsdk.RWD) *Tx {
	return mach.NewTx(s, Root{})
}

// Put creates or replace the entry for key, such that it points to value.
func (tx *Tx) Put(ctx context.Context, key []byte, value []byte) error {
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
func (tx *Tx) Get(ctx context.Context, key []byte, dst *[]byte) (bool, error) {
	if op, ok := tx.edits[string(key)]; ok {
		if op.IsDelete() {
			return false, nil
		}
		*dst = append((*dst)[:0], op.Value...)
		return true, nil
	}
	if !tx.prevRootIsValid() {
		return false, nil
	}
	return tx.mach.Get(ctx, tx.s, tx.prevRoot, key, dst)
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
func (tx *Tx) Flush(ctx context.Context) (*Root, error) {
	if !tx.prevRootIsValid() {
		root, err := tx.mach.NewEmpty(ctx, tx.s)
		if err != nil {
			return nil, err
		}
		tx.prevRoot = *root
	}
	root, err := tx.mach.BatchEdit(ctx, tx.s, tx.prevRoot, maps.Values(tx.edits))
	if err != nil {
		return nil, err
	}
	clear(tx.edits)
	tx.prevRoot = *root
	return &tx.prevRoot, nil
}

func (tx *Tx) prevRootIsValid() bool {
	return !tx.prevRoot.Ref.CID.IsZero()
}
