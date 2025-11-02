package tries

import (
	"fmt"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	capnp "capnproto.org/go/capnp/v3"
)

// Entry represents a key-value pair in the trie.
// See-also: IndexEntry for entries that refer to other nodes.
type Entry struct {
	Key   []byte
	Value []byte
}

func (e *Entry) fromCNP(capnpEnt triescnp.Entry) error {
	key, err := capnpEnt.Key()
	if err != nil {
		return err
	}
	e.Key = key
	value, err := capnpEnt.Value()
	if err != nil {
		return err
	}
	e.Value = value
	return nil
}

func (e *Entry) toCNP(ent triescnp.Entry) error {
	if err := ent.SetKey(e.Key); err != nil {
		return err
	}
	if err := ent.SetValue(e.Value); err != nil {
		return err
	}
	return nil
}

// IndexEntry represents metadata about a trie node reference
type IndexEntry struct {
	// Prefix is the common prefix of all the entries in the referenced node.
	Prefix []byte
	// Ref is the reference to the node.
	Ref Ref
	// Count is the cumulative number of entries transitively reachable from this node.
	Count uint64
}

func (idx *IndexEntry) fromCNP(x triescnp.Entry) error {
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

func (idx *IndexEntry) toCNP(ent *triescnp.Entry) error {
	if err := ent.SetKey(idx.Prefix); err != nil {
		return err
	}
	idx2, err := triescnp.NewIndex(ent.Segment())
	if err != nil {
		return err
	}
	idx2.SetRef(marshalRef(idx.Ref))
	idx2.SetCount(idx.Count)
	return ent.SetIndex(idx2)
}

// Marshal serializes an Index using Cap'n Proto
func (idx *IndexEntry) Marshal(out []byte) []byte {
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
	capnpIdx.SetPrefix(idx.Prefix)
	capnpIdx.SetCount(idx.Count)
	capnpIdx.SetIsParent(idx.IsParent)
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

// UnmarshalIndex deserializes an Index using Cap'n Proto
func (idx *IndexEntry) Unmarshal(data []byte) error {
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
	idx.IsParent = capnpIdx.IsParent()
	idx.Count = capnpIdx.Count()
	return nil
}

func (idx *IndexEntry) ToEntry() *Entry {
	idx2 := *idx
	idx2.Prefix = nil
	return &Entry{
		Key:   idx2.Prefix,
		Value: idx2.Marshal(nil),
	}
}

func (idx *IndexEntry) FromEntry(ent Entry) error {
	if err := idx.Unmarshal(ent.Value); err != nil {
		return err
	}
	idx.Prefix = ent.Key
	return nil
}

type VNodeEntry struct {
	Prefix []byte
	Node   triescnp.Node
}
