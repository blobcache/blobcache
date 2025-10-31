package tries

import (
	"blobcache.io/blobcache/src/internal/tries/triescnp"
	capnp "capnproto.org/go/capnp/v3"
)

// Entry represents a key-value pair in the trie
type Entry struct {
	Key   []byte
	Value []byte
}

func (e *Entry) Marshal() ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	capnpEnt, err := triescnp.NewRootEntry(seg)
	if err != nil {
		return nil, err
	}
	if err := capnpEnt.SetKey(e.Key); err != nil {
		return nil, err
	}
	if err := capnpEnt.SetValue(e.Value); err != nil {
		return nil, err
	}
	return msg.Marshal()
}

func (e *Entry) Unmarshal(data []byte) error {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return err
	}
	capnpEnt, err := triescnp.ReadRootEntry(msg)
	if err != nil {
		return err
	}
	e.Key, err = capnpEnt.Key()
	if err != nil {
		return err
	}
	e.Value, err = capnpEnt.Value()
	if err != nil {
		return err
	}
	return nil
}

// Node represents a trie node containing multiple entries
type Node struct {
	Entries []*Entry
}

// Marshal serializes a Node using Cap'n Proto
func (n *Node) Marshal() ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	capnpNode, err := triescnp.NewRootNode(seg)
	if err != nil {
		return nil, err
	}

	entList, err := capnpNode.NewEntries(int32(len(n.Entries)))
	if err != nil {
		return nil, err
	}

	for i, ent := range n.Entries {
		capnpEnt := entList.At(i)
		if err := capnpEnt.SetKey(ent.Key); err != nil {
			return nil, err
		}
		if err := capnpEnt.SetValue(ent.Value); err != nil {
			return nil, err
		}
	}

	return msg.Marshal()
}

// UnmarshalNode deserializes a Node using Cap'n Proto
func (n *Node) Unmarshal(data []byte) error {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return err
	}
	capnpNode, err := triescnp.ReadRootNode(msg)
	if err != nil {
		return err
	}
	entList, err := capnpNode.Entries()
	if err != nil {
		return err
	}
	n.Entries = make([]*Entry, 0, entList.Len())
	for i := 0; i < entList.Len(); i++ {
		capnpEnt := entList.At(i)
		key, err := capnpEnt.Key()
		if err != nil {
			return err
		}
		value, err := capnpEnt.Value()
		if err != nil {
			return err
		}
		n.Entries = append(n.Entries, &Entry{
			Key:   key,
			Value: value,
		})
	}
	return nil
}

// Index represents metadata about a trie node reference
type Index struct {
	// Ref is the reference to the node.
	Ref Ref
	// Count is the cumulative number of entries transitively reachable from this node.
	Count uint64
	// IsParent is true if the node at Ref is a parent node.
	IsParent bool
}

// Marshal serializes an Index using Cap'n Proto
func (idx *Index) Marshal() ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	capnpIdx, err := triescnp.NewRootIndex(seg)
	if err != nil {
		return nil, err
	}

	if err := capnpIdx.SetRef(marshalRef(idx.Ref)); err != nil {
		return nil, err
	}
	capnpIdx.SetIsParent(idx.IsParent)
	capnpIdx.SetCount(idx.Count)

	return msg.Marshal()
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
	idx.IsParent = capnpIdx.IsParent()
	idx.Count = capnpIdx.Count()
	return nil
}
