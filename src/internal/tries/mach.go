package tries

import (
	"bytes"
	"context"
	"iter"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
)

// Machine holds caches and configuration for operating on tries.
type Machine struct {
	cache  *lru.Cache
	crypto *bccrypto.Machine
}

func NewMachine(salt *blobcache.CID, hf blobcache.HashFunc) *Machine {
	cache, _ := lru.New(16)
	return &Machine{
		cache:  cache,
		crypto: bccrypto.NewMachine(salt, hf),
	}
}

func (o *Machine) NewEmpty(ctx context.Context, s schema.RW) (*Root, error) {
	idx, err := o.PostSlice(ctx, s, nil)
	if err != nil {
		return nil, err
	}
	return (*Root)(idx), nil
}

// PostSlice returns a new instance containing ents
func (o *Machine) PostSlice(ctx context.Context, s schema.RW, ents []Entry) (*Root, error) {
	node, err := mkNode(ents, nil)
	if err != nil {
		return nil, err
	}
	idx, err := o.postNode(ctx, s, node, true)
	if err != nil {
		return nil, err
	}
	return (*Root)(idx), nil
}

// Get retrieves a value at key if it exists, otherwise ErrNotFound is returned
func (o *Machine) Get(ctx context.Context, s schema.RO, root Root, key []byte, dst *[]byte) (bool, error) {
	if !bytes.HasPrefix(key, root.Prefix) {
		return false, nil
	}
	key = compressKey(root.Prefix, key)
	node, err := o.getNode(ctx, s, Index(root))
	if err != nil {
		return false, err
	}
	el, err := node.Entries()
	if err != nil {
		return false, err
	}
	for i := 0; i < el.Len(); i++ {
		ent := el.At(i)
		entKey, err := ent.Key()
		if err != nil {
			return false, err
		}
		if !bytes.HasPrefix(entKey, key) {
			continue
		}
		switch ent.Which() {
		case triescnp.Entry_Which_value:
			if bytes.Equal(key, entKey) {
				val, err := ent.Value()
				if err != nil {
					return false, err
				}
				*dst = append((*dst)[:0], val...)
				return true, nil
			}
		case triescnp.Entry_Which_index:
			var ient Index
			if err := ient.fromCNP(ent); err != nil {
				return false, err
			}
			return o.Get(ctx, s, Root(ient), key, dst)
		default:
			return false, errors.Errorf("unsupported entry type: %s", ent.Which())
		}
	}
	return false, nil
}

// Put returns a copy of root where key maps to value, and all other mappings are unchanged.
func (mach *Machine) Put(ctx context.Context, s schema.RW, root Root, key, value []byte) (*Root, error) {
	return mach.BatchEdit(ctx, s, root, func(yield func(Op) bool) {
		yield(OpPut(key, value))
	})
}

// Delete removes
func (mach *Machine) Delete(ctx context.Context, s schema.RWD, root Root, key []byte) (*Root, error) {
	if !bytes.HasPrefix(key, root.Prefix) {
		return &root, nil
	}
	return mach.BatchEdit(ctx, s, root, func(yield func(Op) bool) {
		yield(OpDelete(key))
	})
}

// BatchEdit applies a batch of operations to a root, returning a new root.
// All ops produced by opsSeq will be collected and sorted.
func (mach *Machine) BatchEdit(ctx context.Context, s schema.RW, root Root, opsSeq iter.Seq[Op]) (*Root, error) {
	ops := slices.Collect(opsSeq)
	slices.SortFunc(ops, func(a, b Op) int {
		return bytes.Compare(a.Key, b.Key)
	})

	node, err := mach.getNode(ctx, s, Index(root))
	if err != nil {
		return nil, err
	}
	ents, ients, err := unmkNode(*node)
	if err != nil {
		return nil, err
	}

	var localEdits []Op
	childEdits := make([][]Op, len(ients))
	for _, op := range ops {
		var foundIndex bool
		for j, ient := range ients {
			if bytes.HasPrefix(op.Key, ient.Prefix) {
				childEdits[j] = append(childEdits[j], op)
				foundIndex = true
				break
			}
		}
		if !foundIndex {
			localEdits = append(localEdits, op)
		}
	}

	// apply all the local edits
	ents = applyOps(nil, ents, localEdits)
	// apply all the edits to children and replace the IndexEntries
	for i, ops := range childEdits {
		ient := ients[i]
		root2, err := mach.BatchEdit(ctx, s, Root(ient), slices.Values(ops))
		if err != nil {
			return nil, err
		}
		ients[i] = Index(*root2)
	}
	node2, err := mkNode(ents, ients)
	if err != nil {
		return nil, err
	}
	ret, err := mach.postNode(ctx, s, node2, true)
	if err != nil {
		return nil, err
	}
	return (*Root)(ret), nil
}

// Op is a single operation on the trie.
type Op struct {
	Key   []byte
	Value []byte
}

func OpDelete(key []byte) Op {
	return Op{Key: key, Value: nil}
}

func OpPut(key []byte, value []byte) Op {
	if len(value) == 0 {
		value = []byte{}
	}
	return Op{Key: key, Value: value}
}

func (op Op) IsDelete() bool {
	return op.Value == nil
}

// Entry returns the entry that the op would create.
// Nil is returned if the op is a delete.
func (op Op) Entry() *Entry {
	if op.IsDelete() {
		return nil
	}
	return &Entry{Key: op.Key, Value: op.Value}
}

// applyOps applies ops to ents, and appends the result to out.
// Both ents, and ops must be sorted.
func applyOps(out []Entry, ents []Entry, ops []Op) []Entry {
	var i, j int
	for i < len(ents) && j < len(ops) {
		cmp := bytes.Compare(ents[i].Key, ops[j].Key)
		switch {
		case cmp < 0:
			out = append(out, ents[i])
			i++
		case cmp > 0:
			out = append(out, Entry{Key: ops[j].Key, Value: ops[j].Value})
			j++
		default:
			if ops[j].IsDelete() {
				i++
			} else {
				out = append(out, Entry{Key: ops[j].Key, Value: ops[j].Value})
			}
			i++
			j++
		}
	}
	for ; i < len(ents); i++ {
		// no more ops, just copy over the remaining ents
		out = append(out, ents[i])
	}
	for ; j < len(ops); j++ {
		// no more ents, just create entries for any puts
		op := ops[j]
		if !op.IsDelete() {
			out = append(out, Entry{Key: op.Key, Value: op.Value})
		}
	}
	return out
}
