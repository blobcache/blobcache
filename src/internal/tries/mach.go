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

func (mach *Machine) NewEmpty(ctx context.Context, s schema.WO) (*Root, error) {
	idx, err := mach.PostSlice(ctx, s, nil)
	if err != nil {
		return nil, err
	}
	return (*Root)(idx), nil
}

// PostSlice returns a new instance containing ents
func (o *Machine) PostSlice(ctx context.Context, s schema.WO, ents []*Entry) (*Root, error) {
	node, err := triescnp.NewRootNode(nil)
	if err != nil {
		return nil, err
	}
	ents2, err := node.NewEntries(int32(len(ents)))
	if err != nil {
		return nil, err
	}
	for i := range ents {
		if err := ents2.At(i).SetKey(ents[i].Key); err != nil {
			return nil, err
		}
		if err := ents2.At(i).SetValue(ents[i].Value); err != nil {
			return nil, err
		}
	}
	idx, err := o.postNode(ctx, s, node)
	if err != nil {
		return nil, err
	}
	return (*Root)(idx), nil
}

// Get retrieves a value at key if it exists, otherwise ErrNotFound is returned
func (mach *Machine) Get(ctx context.Context, s schema.RO, root Root, key []byte, dst *[]byte) (bool, error) {
	if !bytes.HasPrefix(key, root.Prefix) {
		return false, nil
	}
	key = compressKey(root.Prefix, key)
	node, err := o.getNode(ctx, s, IndexEntry(root))
	if err != nil {
		return false, err
	}
	ents, err := node.Entries()
	if err != nil {
		return err
	}
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		entKey, err := ent.Key()
		if err != nil {
			return err
		}
		switch ent.Which() {
		case triescnp.Entry_Which_value:
			if bytes.Equal(key, entKey) {
				v, err := ent.Value()
				if err != nil {
					return err
				}
				*dst = append((*dst)[:0], v...)
				return nil
			}
		case triescnp.Entry_Which_index:
			idx, err := ent.Index()
			if err != nil {
				return err
			}
			idxData, err := idx.Ref()
			if err != nil {
				return err
			}
			idx2, err := parseRef(idxData)
			if err != nil {
				return err
			}
			return o.Get(ctx, s, Root(*idx2), key, dst)
		default:
			return errors.Errorf("unsupported entry type: %s", ent.Which())
		}
	}
	if root.IsParent {
		for _, ent := range ents {
			if len(ent.Key) == 0 && len(key) == 0 {
				*dst = append((*dst)[:0], ent.Value...)
				return true, nil
			}
			if bytes.HasPrefix(key, ent.Key) {
				var idx IndexEntry
				if err := idx.FromEntry(*ent); err != nil {
					return false, err
				}
				return mach.Get(ctx, s, Root(idx), key, dst)
			}
		}
	} else {
		for _, ent := range ents {
			if bytes.Equal(key, ent.Key) {
				*dst = append((*dst)[:0], ent.Value...)
				return true, nil
			}
		}
	}
	return false, nil
}

// Put returns a copy of root where key maps to value, and all other mappings are unchanged.
func (mach *Machine) Put(ctx context.Context, s schema.RW, root Root, key, value []byte) (*Root, error) {
	tx := mach.NewTx(root)
	if err := tx.Put(ctx, s, key, value); err != nil {
		return nil, err
	}
	return tx.Flush(ctx, s)
}

// Delete removes
func (mach *Machine) Delete(ctx context.Context, s schema.RWD, root Root, key []byte) (*Root, error) {
	if !bytes.HasPrefix(key, root.Prefix) {
		return &root, nil
	}
	key = compressKey(root.Prefix, key)
	if root.IsParent {
		panic("deleting from parent not implemented")
	} else {
		xs, err := o.getNode(ctx, s, IndexEntry(root), false)
		if err != nil {
			return nil, err
		}
		var ys []*Entry
		for _, ent := range xs {
			if !bytes.Equal(key, ent.Key) {
				ys = append(ys, ent)
			}
		}
		idx, err := mach.postLeaf(ctx, s, ys)
		if err != nil {
			return nil, err
		}
		return (*Root)(idx), nil
	}
}

// BatchEdit applies a batch of operations to a root, returning a new root.
// All ops produced by opsSeq will be collected and sorted.
func (mach *Machine) BatchEdit(ctx context.Context, s schema.RW, root Root, opsSeq iter.Seq[Op]) (*Root, error) {
	ops := slices.Collect(opsSeq)
	slices.SortFunc(ops, func(a, b Op) int {
		return bytes.Compare(a.Key, b.Key)
	})
	if root.IsParent {
		e, children, err := o.getParent(ctx, s, IndexEntry(root), true)
		if err != nil {
			return nil, err
		}
		localOp, groups := groupOps(slices.Values(ops))
		if localOp != nil {
			e = localOp.Entry()
		}
		for i := range groups {
			group := groups[i]
			child := children[i]
			child2, err := mach.BatchEdit(ctx, s, Root(child), slices.Values(group))
			if err != nil {
				return nil, err
			}
			children[i] = IndexEntry(*child2)
		}
		idx, err := mach.postParent(ctx, s, children[:], e)
		if err != nil {
			return nil, err
		}
		return (*Root)(idx), nil
	} else {
		xs, err := o.getNode(ctx, s, IndexEntry(root), true)
		if err != nil {
			return nil, err
		}
		ys := applyOps(nil, xs, ops)
		idx, err := mach.postNode(ctx, s, ys)
		if err != nil {
			return nil, err
		}
		return (*Root)(idx), nil
	}
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

// applyOps returns a sequence that can be used to read the new entries after applying the ops.
// Both ents, and ops must be sorted.
func applyOps(out []*Entry, ents []*Entry, ops []Op) []*Entry {
	var i, j int
	for i < len(ents) && j < len(ops) {
		cmp := bytes.Compare(ents[i].Key, ops[j].Key)
		switch {
		case cmp < 0:
			out = append(out, ents[i])
			i++
		case cmp > 0:
			out = append(out, &Entry{Key: ops[j].Key, Value: ops[j].Value})
			j++
		default:
			if ops[j].IsDelete() {
				i++
			} else {
				out = append(out, &Entry{Key: ops[j].Key, Value: ops[j].Value})
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
			out = append(out, &Entry{Key: op.Key, Value: op.Value})
		}
	}
	return out
}
