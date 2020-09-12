package tries

import (
	"bytes"
	"context"
	"sort"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	ErrNotExist = errors.Errorf("no entry for key")

	ErrCannotCollapse = errors.Errorf("cannot collapse parent into child")
	ErrCannotSplit    = errors.Errorf("cannot split, < 2 entries")
)

func New() *Node {
	return &Node{}
}

func PostNode(ctx context.Context, s blobs.Store, n *Node) (*blobs.ID, error) {
	for {
		data, err := proto.Marshal(n)
		if err != nil {
			return nil, err
		}
		if len(data) <= blobs.MaxSize {
			id, err := s.Post(ctx, data)
			if err != nil {
				return nil, err
			}
			return &id, nil
		}
		n, err = Split(ctx, s, n)
		if err != nil {
			return nil, err
		}
	}
}

func GetNode(ctx context.Context, s blobs.Store, id blobs.ID) (*Node, error) {
	n := &Node{}
	if err := s.GetF(ctx, id, func(data []byte) error {
		return proto.Unmarshal(data, n)
	}); err != nil {
		return nil, err
	}
	if err := ValidateNode(n); err != nil {
		return nil, err
	}
	return n, nil
}

func Put(ctx context.Context, s blobs.Store, id blobs.ID, key, value []byte) (*blobs.ID, error) {
	n, err := GetNode(ctx, s, id)
	if err != nil {
		return nil, err
	}

	if !bytes.HasPrefix(key, n.Prefix) {
		return nil, errors.Errorf("key does not have node prefix")
	}
	if IsParent(n) {
		if len(n.Prefix) == len(key) {
			n.Entries = []*Entry{makeEntry(n.Prefix, key, value)}
			return PostNode(ctx, s, n)
		}
		c := key[len(n.Prefix)]
		childID := blobs.IDFromBytes(n.Children[c])
		childID2, err := Put(ctx, s, childID, key, value)
		if err != nil {
			return nil, err
		}
		n.Children[c] = childID2[:]
		return PostNode(ctx, s, n)
	}
	n.Entries = append(n.Entries, makeEntry(n.Prefix, key, value))
	sort.Slice(n.Entries, func(i, j int) bool {
		return bytes.Compare(n.Entries[i].Key, n.Entries[j].Key) < 0
	})
	return PostNode(ctx, s, n)
}

func Get(ctx context.Context, s blobs.Store, id blobs.ID, key []byte) ([]byte, error) {
	n, err := GetNode(ctx, s, id)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(key, n.Prefix) {
		return nil, errors.Errorf("key %x does not have node prefix %x", key, n.Prefix)
	}
	if IsParent(n) {
		if len(n.Prefix) == len(key) {
			if len(n.Entries) < 1 {
				return nil, ErrNotExist
			}
			return n.Entries[0].Value, nil
		}
		c := key[len(n.Prefix)]
		if len(n.Children[c]) == 0 {
			return nil, ErrNotExist
		}
		childID := blobs.IDFromBytes(n.Children[c])
		return Get(ctx, s, childID, key)
	}
	for _, ent := range n.Entries {
		entKey := append(n.Prefix, ent.Key...)
		if bytes.Equal(entKey, key) {
			return ent.Value, nil
		}
	}
	return nil, ErrNotExist
}

func Delete(ctx context.Context, s blobs.Store, id blobs.ID, key []byte) (*blobs.ID, error) {
	n, err := GetNode(ctx, s, id)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(key, n.Prefix) {
		return nil, errors.Errorf("key %x does not have node prefix %x", key, n.Prefix)
	}
	if IsParent(n) {
		if len(n.Prefix) == len(key) {
			if len(n.Entries) > 0 {
				n.Entries = nil
				return PostNode(ctx, s, n)
			}
			return &id, nil
		}
		c := key[len(n.Prefix)]
		if len(n.Children[c]) == 0 {
			return &id, nil
		}
		childID := blobs.IDFromBytes(n.Children[c])
		childID2, err := Delete(ctx, s, childID, key)
		if err != nil {
			return nil, err
		}
		n.Children[c] = childID2[:]
		n2, err := Collapse(ctx, s, n)
		if err != nil && err != ErrCannotCollapse {
			return nil, err
		} else if err != nil {
			n = n2
		}
		return PostNode(ctx, s, n)
	}
	for i, ent := range n.Entries {
		entKey := append(n.Prefix, ent.Key...)
		if bytes.Equal(entKey, key) {
			n.Entries = deleteEntry(n.Entries, i)
			return PostNode(ctx, s, n)
		}
	}
	return &id, nil
}

func Split(ctx context.Context, s blobs.Store, x *Node) (*Node, error) {
	if len(x.Entries) < 2 {
		return nil, ErrCannotSplit
	}
	y := &Node{}
	childEntries := [256][]*Entry{}
	for _, ent := range x.Entries {
		if len(ent.Key) == 0 {
			y.Entries = []*Entry{ent}
			continue
		}
		c := ent.Key[0]
		childEntries[c] = append(childEntries[c], ent)
	}

	y.Children = make([][]byte, 256)
	for i := range childEntries {
		if len(childEntries[i]) < 1 {
			y.Children[i] = nil
			continue
		}
		child := &Node{
			Prefix:  append(x.Prefix, uint8(i)),
			Entries: childEntries[i],
		}
		childID, err := PostNode(ctx, s, child)
		if err != nil {
			return nil, err
		}
		y.Children[i] = childID[:]
	}
	return y, nil
}

func Collapse(ctx context.Context, s blobs.Store, x *Node) (*Node, error) {
	if !IsParent(x) {
		return x, nil
	}
	y := &Node{}
	for i := range x.Children {
		if len(x.Children[i]) == 0 {
			continue
		}
		childID := blobs.IDFromBytes(x.Children[i])
		child, err := GetNode(ctx, s, childID)
		if err != nil {
			return nil, err
		}
		if IsParent(child) {
			return nil, ErrCannotCollapse
		}
		y.Entries = append(y.Entries, child.Entries...)
	}
	if proto.Size(y) > blobs.MaxSize {
		return nil, ErrCannotCollapse
	}
	return y, nil
}

func Validate(ctx context.Context, s blobs.Store, id blobs.ID) error {
	n, err := GetNode(ctx, s, id)
	if err != nil {
		return err
	}
	if err := ValidateNode(n); err != nil {
		return err
	}
	if IsParent(n) {
		for i := range n.Children {
			childID := blobs.IDFromBytes(n.Children[i])
			if err := Validate(ctx, s, childID); err != nil {
				return err
			}
		}
	}
	return nil
}

func ValidateNode(x *Node) error {
	switch {
	case len(x.Children) == 0:
		for i := range x.Entries {
			if i > 0 {
				if bytes.Compare(x.Entries[i].Key, x.Entries[i-1].Key) <= 0 {
					return errors.Errorf("keys must be sorted")
				}
			}
		}
		return nil
	case len(x.Children) == 256 && len(x.Entries) < 2:
		for i := range x.Children {
			l := len(x.Children[i])
			if l != 0 && l != blobs.IDSize {
				return errors.Errorf("child ref is wrong size")
			}

		}
		return nil
	default:
		return errors.Errorf("children is incorrect length %d", len(x.Children))
	}
}

func IsParent(x *Node) bool {
	return len(x.Children) == 256
}

func makeEntry(prefix, key, value []byte) *Entry {
	if len(prefix) == len(key) {
		return &Entry{Value: value}
	}
	return &Entry{
		Key:   key[len(prefix):],
		Value: value,
	}
}

func deleteEntry(ents []*Entry, i int) []*Entry {
	if i == len(ents)-1 {
		return ents[:len(ents)-1]
	}
	return append(ents[:i], ents[i+1:]...)
}
