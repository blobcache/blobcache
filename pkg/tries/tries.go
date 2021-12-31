package tries

import (
	"bytes"
	"context"
	"sort"

	"github.com/brendoncarroll/go-state/cadata"
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

func PostNode(ctx context.Context, s cadata.Store, n *Node) (*Ref, error) {
	for {
		data, err := proto.Marshal(n)
		if err != nil {
			return nil, err
		}
		if len(data) < s.MaxSize() {
			return post(ctx, s, data)
		}
		n, err = Split(ctx, s, n)
		if err != nil {
			return nil, err
		}
	}
}

func GetNode(ctx context.Context, s cadata.Store, ref Ref) (*Node, error) {
	n := &Node{}
	if err := getF(ctx, s, ref, func(data []byte) error {
		return proto.Unmarshal(data, n)
	}); err != nil {
		return nil, err
	}
	if err := ValidateNode(n); err != nil {
		return nil, err
	}
	return n, nil
}

func Put(ctx context.Context, s cadata.Store, ref Ref, key, value []byte) (*Ref, error) {
	n, err := GetNode(ctx, s, ref)
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
		childRef := fromChildProto(n.Children[c])
		childRef2, err := Put(ctx, s, childRef, key, value)
		if err != nil {
			return nil, err
		}
		n.Children[c] = toChildProto(*childRef2)
		return PostNode(ctx, s, n)
	}
	n.Entries = append(n.Entries, makeEntry(n.Prefix, key, value))
	sort.Slice(n.Entries, func(i, j int) bool {
		return bytes.Compare(n.Entries[i].Key, n.Entries[j].Key) < 0
	})
	return PostNode(ctx, s, n)
}

func Get(ctx context.Context, s cadata.Store, ref Ref, key []byte) ([]byte, error) {
	n, err := GetNode(ctx, s, ref)
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
		if n.Children[c] == nil {
			return nil, ErrNotExist
		}
		childRef := fromChildProto(n.Children[c])
		return Get(ctx, s, childRef, key)
	}
	for _, ent := range n.Entries {
		entKey := append(n.Prefix, ent.Key...)
		if bytes.Equal(entKey, key) {
			return ent.Value, nil
		}
	}
	return nil, ErrNotExist
}

func Delete(ctx context.Context, s cadata.Store, ref Ref, key []byte) (*Ref, error) {
	n, err := GetNode(ctx, s, ref)
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
			return &ref, nil
		}
		c := key[len(n.Prefix)]
		if n.Children[c] == nil {
			return &ref, nil
		}
		childRef := fromChildProto(n.Children[c])
		childRef2, err := Delete(ctx, s, childRef, key)
		if err != nil {
			return nil, err
		}
		n.Children[c] = toChildProto(*childRef2)
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
	return &ref, nil
}

func Split(ctx context.Context, s cadata.Store, x *Node) (*Node, error) {
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

	y.Children = make([]*ChildRef, 256)
	for i := range childEntries {
		if len(childEntries[i]) < 1 {
			y.Children[i] = nil
			continue
		}
		child := &Node{
			Prefix:  append(x.Prefix, uint8(i)),
			Entries: childEntries[i],
		}
		childRef, err := PostNode(ctx, s, child)
		if err != nil {
			return nil, err
		}
		y.Children[i] = toChildProto(*childRef)
	}
	return y, nil
}

func Collapse(ctx context.Context, s cadata.Store, x *Node) (*Node, error) {
	if !IsParent(x) {
		return x, nil
	}
	y := &Node{}
	for i := range x.Children {
		if x.Children[i] == nil {
			continue
		}
		childRef := fromChildProto(x.Children[i])
		child, err := GetNode(ctx, s, childRef)
		if err != nil {
			return nil, err
		}
		if IsParent(child) {
			return nil, ErrCannotCollapse
		}
		y.Entries = append(y.Entries, child.Entries...)
	}
	if proto.Size(y) > s.MaxSize() {
		return nil, ErrCannotCollapse
	}
	return y, nil
}

func Validate(ctx context.Context, s cadata.Store, ref Ref) error {
	n, err := GetNode(ctx, s, ref)
	if err != nil {
		return err
	}
	if err := ValidateNode(n); err != nil {
		return err
	}
	if IsParent(n) {
		for i := range n.Children {
			childID := fromChildProto(n.Children[i])
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
