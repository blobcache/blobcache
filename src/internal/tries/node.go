package tries

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"slices"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
	"capnproto.org/go/capnp/v3"
	"github.com/pkg/errors"
	"go.brendoncarroll.net/state/cadata"
)

// getNode returns node at x.
// all the entries will be in compressed form.
func (o *Machine) getNode(ctx context.Context, s schema.RO, x IndexEntry) (*triescnp.Node, error) {
	var n triescnp.Node
	if err := o.getF(ctx, s, x.Ref, func(data []byte) error {
		data = slices.Clone(data)
		msg, err := capnp.Unmarshal(data)
		if err != nil {
			return err
		}
		n, err = triescnp.ReadRootNode(msg)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	ents, err := n.Entries()
	if err != nil {
		return nil, err
	}
	if err := validateEntries(ents); err != nil {
		return nil, err
	}
	if err := checkIndexEntries(ctx, s, x, ents); err != nil {
		return nil, err
	}
	return &n, nil
}

// postNode creates a new node with ents, ents will be split if necessary
func (mach *Machine) postNode(ctx context.Context, s schema.WO, node triescnp.Node) (*IndexEntry, error) {
	data, err := capnp.Canonicalize(capnp.Struct(node))
	if err != nil {
		return nil, err
	}
	if len(data) > s.MaxSize() {
		valEnt, idxEnts, err := mach.split(ctx, s, node)
		if err != nil {
			return nil, err
		}
		entsLen := len(idxEnts)
		if valEnt != nil {
			entsLen++
		}
		node2, err := triescnp.NewNode(nil)
		if err != nil {
			return nil, err
		}
		ents, err := node2.NewEntries(int32(entsLen))
		if err != nil {
			return nil, err
		}
		if valEnt != nil {
			ent := ents.At(0)
			if err := ent.SetKey(valEnt.Key); err != nil {
				return nil, err
			}
			if err := ent.SetValue(valEnt.Value); err != nil {
				return nil, err
			}
		}
		for i := 0; i < len(idxEnts); i++ {
			ent := ents.At(i + (entsLen - len(idxEnts)))
			if err := idxEnts[i].toCNP(&ent); err != nil {
				return nil, err
			}
		}

		node = node2
		data, err = capnp.Canonicalize(capnp.Struct(node2))
		if err != nil {
			return nil, err
		}
	}

	// base case: encrypt and post to store, create IndexEntry
	var prefix []byte
	var count uint64
	ents, err := node.Entries()
	if err != nil {
		return nil, err
	}
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		key, err := ent.Key()
		if err != nil {
			return nil, err
		}
		prefix = longestCommonPrefix(prefix, key)
	}
	ref, err := mach.crypto.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &IndexEntry{
		Prefix: prefix,
		Ref: Ref{
			CID:    ref.CID,
			DEK:    ref.DEK,
			Length: uint32(len(data)),
		},
		Count: count,
	}, nil
}

func (o *Machine) split(ctx context.Context, s schema.WO, node triescnp.Node) (*Entry, []IndexEntry, error) {
	ents, err := node.Entries()
	if err != nil {
		return nil, nil, err
	}
	if ents.Len() < 2 {
		return nil, nil, ErrCannotSplit
	}
	e, groups := groupEntries(ents)
	var children []IndexEntry
	for _, childEnts := range groups {
		childRoot, err := mach.postNode(ctx, s, childEnts)
		if err != nil {
			return nil, nil, err
		}
		children = append(children, *childRoot)
	}
	return e, children, nil
}

func (mach *Machine) collapse(ctx context.Context, s cadata.Store, children []Root) ([]*Entry, error) {
	panic("collapse not implemented")
}

func compressKey(prefix, x []byte) []byte {
	if !bytes.HasPrefix(x, prefix) {
		panic(fmt.Sprintf("cannot compress key %q does not have prefix %q", x, prefix))
	}
	return bytes.TrimPrefix(x, prefix)
}

func expandKey(prefix, x []byte) []byte {
	var y []byte
	y = append(y, prefix...)
	y = append(y, x...)
	return y
}

func compressEntry(prefix []byte, ent *Entry) *Entry {
	return &Entry{
		Key:   compressKey(prefix, ent.Key),
		Value: ent.Value,
	}
}

func expandEntry(prefix []byte, ent *Entry) *Entry {
	return &Entry{
		Key:   expandKey(prefix, ent.Key),
		Value: ent.Value,
	}
}

func compressEntries(xs []*Entry) ([]byte, []*Entry) {
	var lcp []byte
	for i, x := range xs {
		key := x.Key
		if i == 0 {
			lcp = key
			continue
		}
		if len(key) < len(lcp) {
			lcp = lcp[:len(key)]
		}
		for i := 0; i < len(lcp) && i < len(key); i++ {
			if key[i] != lcp[i] {
				lcp = lcp[:i]
				break
			}
		}
	}
	ys := make([]*Entry, len(xs))
	for i := range xs {
		ys[i] = compressEntry(lcp, xs[i])
	}
	return lcp, ys
}

func validateEntries(ents triescnp.Entry_List) error {
	var lastKey []byte
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		k, err := ent.Key()
		if err != nil {
			return err
		}
		if i > 0 {
			if bytes.HasPrefix(k, lastKey) || bytes.HasPrefix(lastKey, k) {
				return errors.Errorf("entries must not be prefixes of one another")
			}
			if bytes.Compare(k, lastKey) <= 0 {
				return errors.Errorf("entries must be sorted")
			}
		}
		lastKey = k
	}
	return nil
}

func checkIndexEntries(ctx context.Context, s schema.RO, x IndexEntry, ents triescnp.Entry_List) error {
	var actualSum uint64
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		switch ent.Which() {
		case triescnp.Entry_Which_index:
			idx, err := ent.Index()
			if err != nil {
				return err
			}
			refData, err := idx.Ref()
			if err != nil {
				return err
			}
			ref, err := parseRef(refData)
			if err != nil {
				return err
			}
			exists, err := schema.ExistsUnit(ctx, s, ref.CID)
			if err != nil {
				return err
			}
			if !exists {
				return errors.Errorf("index reference %s does not exist", ref.CID)
			}
			actualSum += idx.Count()
		}
	}
	if actualSum != x.Count {
		return errors.Errorf("count mismatch: actual %d, expected %d", actualSum, x.Count)
	}
	return nil
}

func groupEntries(ents iter.Seq[*Entry]) (local *Entry, groups [256][]*Entry) {
	for ent := range ents {
		if len(ent.Key) == 0 {
			local = ent
			continue
		}
		b := ent.Key[0]
		groups[b] = append(groups[b], ent)
	}
	return local, groups
}

func groupOps(ops iter.Seq[Op]) (local *Op, groups [256][]Op) {
	for op := range ops {
		if len(op.Key) == 0 {
			local = &op
			continue
		}
		b := op.Key[0]
		groups[b] = append(groups[b], op)
	}
	return local, groups
}

func longestCommonPrefix(a, b []byte) []byte {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a
}
