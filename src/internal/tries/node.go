package tries

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"slices"

	"capnproto.org/go/capnp/v3"
	"github.com/pkg/errors"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
)

// getNode returns node at x.
// all the entries will be in compressed form.
func (o *Machine) getNode(ctx context.Context, s schema.RO, x Index) (*triescnp.Node, error) {
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
	el, err := n.Entries()
	if err != nil {
		return nil, err
	}
	if err := validateEntries(el); err != nil {
		return nil, err
	}
	if err := checkEntries(ctx, s, x, el); err != nil {
		return nil, err
	}
	return &n, nil
}

// postNode creates a new node with ents, ents will be split if necessary
func (mach *Machine) postNode(ctx context.Context, s schema.RW, node triescnp.Node, allowSplit bool) (*Index, error) {
	if !allowSplit {
		return nil, fmt.Errorf("node cannot be split further")
	}
	// TODO: use the canonical serialization here.
	msg := node.Message()
	if msg == nil {
		return nil, fmt.Errorf("node has no message")
	}
	data, err := msg.Marshal()
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
		var ents []Entry
		if valEnt != nil {
			ents = append(ents, *valEnt)
		}
		node2, err := mkNode(ents, idxEnts)
		if err != nil {
			return nil, err
		}
		return mach.postNode(ctx, s, node2, false)
	}

	// base case: encrypt and post to store, create IndexEntry
	var count uint64
	ents, err := node.Entries()
	if err != nil {
		return nil, err
	}
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		switch ent.Which() {
		case triescnp.Entry_Which_value:
			count++
		case triescnp.Entry_Which_index:
			idx, err := ent.Index()
			if err != nil {
				return nil, err
			}
			count += idx.Count()
		default:
			return nil, fmt.Errorf("unsupported entry type %v", ent.Which())
		}
	}
	ref, err := mach.crypto.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &Index{
		Prefix: nil,
		Ref: Ref{
			CID:    ref.CID,
			DEK:    ref.DEK,
			Length: uint32(len(data)),
		},
		Count: count,
	}, nil
}

func (mach *Machine) split(ctx context.Context, s schema.RW, node triescnp.Node) (*Entry, []Index, error) {
	ents, ients, err := unmkNode(node)
	if err != nil {
		return nil, nil, err
	}
	e, groups := groupEntries(slices.Values(ents))

	for _, childEnts := range groups {
		node2, err := mkNode(childEnts, nil)
		if err != nil {
			return nil, nil, err
		}
		childRoot, err := mach.postNode(ctx, s, node2, true)
		if err != nil {
			return nil, nil, err
		}
		ients = append(ients, *childRoot)
	}
	ients, err = mach.compactIndexes(ctx, s, ients)
	if err != nil {
		return nil, nil, err
	}
	return e, ients, nil
}

// compactIndexes looks for indexes with overlapping prefixes, and merges them.
func (mach *Machine) compactIndexes(ctx context.Context, s schema.RW, ients []Index) ([]Index, error) {
	slices.SortStableFunc(ients, indexComp)
	var merged []Index
	for i := 0; i < len(ients); i++ {
		toMerge := []Index{ients[i]}
		for j := i + 1; j < len(ients); j++ {
			if !prefixesOverlap(ients[i].Prefix, ients[j].Prefix) {
				break
			}
			toMerge = append(toMerge, ients[j])
		}
		m, err := mach.mergeIndexes(ctx, s, toMerge)
		if err != nil {
			return nil, err
		}
		merged = append(merged, *m)
	}
	return merged, nil
}

func (mach *Machine) mergeIndexes(ctx context.Context, s schema.RW, ients []Index) (*Index, error) {
	if len(ients) == 0 {
		return nil, fmt.Errorf("cannot merge 0 indexes")
	}
	for i := 0; i < len(ients); i++ {
		if !bytes.Equal(ients[i].Prefix, ients[i+1].Prefix) {
			return nil, fmt.Errorf("cannot merge non-equal indexes")
		}
	}
	var ents []Entry
	var idxs []Index
	for _, ient := range ients {
		node, err := mach.getNode(ctx, s, ient)
		if err != nil {
			return nil, err
		}
		ents, idxs, err := unmkNode(*node)
		if err != nil {
			return nil, err
		}
		ents = append(ents, ents...)
		idxs = append(idxs, idxs...)
	}
	node, err := mkNode(ents, idxs)
	if err != nil {
		return nil, err
	}
	return mach.postNode(ctx, s, node, true)
}

// collapse takes multiple nodes by reference, and attemps to consolidate all
// their data into a single node.
// Expect collapse to return ErrCannotCollapse, if the data cannot fit into a single node.
func (o *Machine) collapse(ctx context.Context, s schema.RW, children []Root) ([]*Entry, error) {
	panic("collapse not implemented")
}

// prefixesOverlaps checks if either a is a prefix of b or b is a prefix of a
func prefixesOverlap(a, b []byte) bool {
	return bytes.HasPrefix(a, b) || bytes.HasPrefix(b, a)
}

func compressKey(prefix, x []byte) []byte {
	if !bytes.HasPrefix(x, prefix) {
		panic(fmt.Sprintf("cannot compress key %q does not have prefix %q", x, prefix))
	}
	return bytes.TrimPrefix(x, prefix)
}

func expandKey(prefix, x []byte) []byte {
	return slices.Concat(prefix, x)
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

// checkEntries checks that the entries are in sorted order.
func checkEntries(ctx context.Context, s schema.RO, x Index, ents triescnp.Entry_List) error {
	var actualSum uint64
	var lastKey []byte
	for i := 0; i < ents.Len(); i++ {
		x := ents.At(i)
		k, err := x.Key()
		if err != nil {
			return err
		}
		// check that the keys do not have overlapping prefixes
		if i > 0 {
			if prefixesOverlap(k, lastKey) {
				return errors.Errorf("entries must not be prefixes of one another")
			}
			if bytes.Compare(k, lastKey) <= 0 {
				return errors.Errorf("entries must be sorted")
			}
		}
		lastKey = k

		switch x.Which() {
		case triescnp.Entry_Which_value:
			actualSum += 1
		case triescnp.Entry_Which_index:
			idx, err := x.Index()
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
		default:
			return fmt.Errorf("unsupported entry type %v", x.Which())
		}
	}
	if actualSum != x.Count {
		return errors.Errorf("count mismatch: actual %d, expected %d", actualSum, x.Count)
	}
	return nil
}

func groupEntries(ents iter.Seq[Entry]) (local *Entry, groups [256][]Entry) {
	for ent := range ents {
		if len(ent.Key) == 0 {
			local = &ent
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
