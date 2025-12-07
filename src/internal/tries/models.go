package tries

import (
	"bytes"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	capnp "capnproto.org/go/capnp/v3"
)

// Entry represents a key-value pair in the trie.
// See-also: IndexEntry for entries that refer to other nodes.
// and VNodeEntry for entries that contain other nodes inline.
type Entry struct {
	Key   []byte
	Value []byte
}

func (ent *Entry) fromCNP(x triescnp.Entry) error {
	if x.Which() != triescnp.Entry_Which_value {
		return fmt.Errorf("wrong entry type %v", x.Which())
	}
	val, err := x.Value()
	if err != nil {
		return err
	}
	key, err := x.Key()
	if err != nil {
		return err
	}
	ent.Value = val
	ent.Key = key
	return nil
}

func (ent *Entry) toCNP(x triescnp.Entry) error {
	if err := x.SetValue(ent.Value); err != nil {
		return err
	}
	if err := x.SetKey(ent.Key); err != nil {
		return err
	}
	return nil
}

func entryComp(a, b Entry) int {
	return bytes.Compare(a.Key, b.Key)
}

// Index represents metadata about a trie node reference
type Index struct {
	// Prefix is the common prefix of all the entries in the referenced node.
	Prefix []byte
	// Ref is the reference to the node.
	Ref Ref
	// Count is the cumulative number of entries transitively reachable from this node.
	Count uint64
}

func (idx *Index) fromCNP(x triescnp.Entry) error {
	if x.Which() != triescnp.Entry_Which_index {
		return fmt.Errorf("cannot convert entry (%s) to index", x.Which())
	}
	key, err := x.Key()
	if err != nil {
		return err
	}
	idx.Prefix = key
	idx2, err := x.Index()
	if err != nil {
		return err
	}
	refData, err := idx2.Ref()
	if err != nil {
		return err
	}
	ref, err := parseRef(refData)
	if err != nil {
		return err
	}
	idx.Ref = *ref
	idx.Count = idx2.Count()
	return nil
}

func (idx *Index) toCNP(ent *triescnp.Entry) error {
	if err := ent.SetKey(idx.Prefix); err != nil {
		return err
	}
	idx2, err := ent.NewIndex()
	if err != nil {
		return err
	}
	if err := idx2.SetRef(marshalRef(idx.Ref)); err != nil {
		return err
	}
	idx2.SetCount(idx.Count)
	return nil
}

// Marshal serializes an Index using Cap'n Proto
func (idx *Index) Marshal(out []byte) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}
	capnpIdx, err := triescnp.NewRootIndex(seg)
	if err != nil {
		panic(err)
	}
	if err := capnpIdx.SetRef(marshalRef(idx.Ref)); err != nil {
		panic(err)
	}
	capnpIdx.SetCount(idx.Count)
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

// UnmarshalIndex deserializes an Index using Cap'n Proto
func (idx *Index) Unmarshal(data []byte) error {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return err
	}
	capnpIdx, err := triescnp.ReadRootIndex(msg)
	if err != nil {
		return err
	}
	ref, err := capnpIdx.Ref()
	if err != nil {
		return err
	}
	ref2, err := parseRef(ref)
	if err != nil {
		return err
	}
	idx.Ref = *ref2
	idx.Count = capnpIdx.Count()
	return nil
}

func (idx *Index) ToEntry() *Entry {
	idx2 := *idx
	idx2.Prefix = nil
	return &Entry{
		Key:   idx2.Prefix,
		Value: idx2.Marshal(nil),
	}
}

func (idx *Index) FromEntry(ent Entry) error {
	if err := idx.Unmarshal(ent.Value); err != nil {
		return err
	}
	idx.Prefix = ent.Key
	return nil
}

func indexComp(a, b Index) int {
	return bytes.Compare(a.Prefix, b.Prefix)
}

type VNodeEntry struct {
	Prefix []byte
	Node   triescnp.Node
}

// mkNode returns a new node from a list of entries.
func mkNode(ents []Entry, ients []Index) (triescnp.Node, error) {
	slices.SortStableFunc(ents, entryComp)
	slices.SortStableFunc(ients, indexComp)
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}
	node, err := triescnp.NewRootNode(seg)
	if err != nil {
		panic(err)
	}
	el, err := node.NewEntries(int32(len(ents) + len(ients)))
	if err != nil {
		return triescnp.Node{}, err
	}

	i, j := 0, 0
	for k := 0; k < el.Len(); k++ {
		if i < len(ents) && (j >= len(ients) || bytes.Compare(ents[i].Key, ients[j].Prefix) < 0) {
			ent := ents[i]
			slot := el.At(k)
			if err := slot.SetKey(ent.Key); err != nil {
				return triescnp.Node{}, err
			}
			if err := slot.SetValue(ent.Value); err != nil {
				return triescnp.Node{}, err
			}
			i++
			continue
		}

		idx := ients[j]
		slot := el.At(k)
		if err := slot.SetKey(idx.Prefix); err != nil {
			return triescnp.Node{}, err
		}
		idxCNP, err := slot.NewIndex()
		if err != nil {
			return triescnp.Node{}, err
		}
		if err := idxCNP.SetRef(marshalRef(idx.Ref)); err != nil {
			return triescnp.Node{}, err
		}
		idxCNP.SetCount(idx.Count)
		j++
	}

	return node, nil
}

// nodeAll returns an iterator over all the entries in the node.
func unmkNode(node triescnp.Node) ([]Entry, []Index, error) {
	var ret []Entry
	var ret2 []Index
	children, err := node.Entries()
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < children.Len(); i++ {
		x := children.At(i)
		switch x.Which() {
		case triescnp.Entry_Which_index:
			var ient Index
			if err := ient.fromCNP(x); err != nil {
				return nil, nil, err
			}
			ret2 = append(ret2, ient)
		case triescnp.Entry_Which_value:
			key, err := x.Key()
			if err != nil {
				return nil, nil, err
			}
			value, err := x.Value()
			if err != nil {
				return nil, nil, err
			}
			ret = append(ret, Entry{Key: key, Value: value})
		}
	}
	return ret, ret2, nil
}
