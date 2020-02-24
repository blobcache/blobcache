package tries

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"sort"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

var enc = base64.URLEncoding

const maxEntries = 512

type Pair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type Trie struct {
	store blobs.GetPostDelete

	Prefix   []byte         `json:"prefix"`
	Children *[256]blobs.ID `json:"children,omitempty"`
	Entries  []Pair         `json:"entries,omitempty"`
}

func New(store blobs.GetPostDelete) *Trie {
	return &Trie{
		store: store,
	}
}

func NewWithPrefix(store blobs.GetPostDelete, prefix []byte) *Trie {
	return &Trie{
		store:  store,
		Prefix: prefix,
	}
}

func (t *Trie) Clone() *Trie {
	var children *[256]blobs.ID
	if t.Children != nil {
		c := *t.Children
		children = &c
	}

	return &Trie{
		store:    t.store,
		Prefix:   t.Prefix,
		Children: children,
		Entries:  append([]Pair{}, t.Entries...),
	}
}

func (t *Trie) Put(ctx context.Context, key, value []byte) error {
	if err := t.Validate(); err != nil {
		return err
	}
	return t.put(ctx, Pair{Key: key, Value: value})
}

func (t *Trie) put(ctx context.Context, pair Pair) error {
	if !bytes.HasPrefix(pair.Key, t.Prefix) {
		return errors.New("wrong prefix for this trie")
	}
	if t.Children == nil {
		return t.putInThis(ctx, pair)
	}

	err := t.replaceChild(ctx, pair.Key, func(x Trie) (*Trie, error) {
		err := x.put(ctx, pair)
		return &x, err
	})
	return err
}

func (t *Trie) putInThis(ctx context.Context, pair Pair) error {
	if len(t.Entries) < maxEntries {
		t.Entries = append(t.Entries, pair)
		sort.Slice(t.Entries, func(i, j int) bool {
			return bytes.Compare(t.Entries[i].Key, t.Entries[j].Key) < 0
		})
		return nil
	}

	// convert to parent
	t.Entries = nil
	children := t.split()
	t.Children = new([256]blobs.ID)
	for i, child := range children {
		data := child.Marshal()
		ref, err := t.store.Post(ctx, data)
		if err != nil {
			return err
		}
		t.Children[i] = ref
	}

	// add to child
	return t.replaceChild(ctx, pair.Key, func(x Trie) (*Trie, error) {
		err := x.put(ctx, pair)
		return &x, err
	})
}

func (t *Trie) Get(ctx context.Context, key []byte) (*Pair, error) {
	if !bytes.HasPrefix(key, t.Prefix) {
		return nil, errors.New("wrong prefix for this trie")
	}

	if t.Children == nil {
		key := key[len(t.Prefix):]
		for _, pair := range t.Entries {
			if bytes.Compare(pair.Key, key) == 0 {
				return &pair, nil
			}
		}
		return nil, nil
	}

	_, subT, err := t.childFor(ctx, key)
	if err != nil {
		return nil, err
	}
	return subT.Get(ctx, key)
}

func (t *Trie) GetChild(ctx context.Context, c byte) (*Trie, error) {
	if t.Children == nil {
		panic("GetChild called on child trie")
	}
	return t.getChild(ctx, int(c))
}

func (t *Trie) GetPrefix() []byte {
	return t.Prefix
}

func (t *Trie) IsParent() bool {
	return t.Children != nil
}

func (t *Trie) split() []Trie {
	subTs := make([]Trie, 256)
	for i := range subTs {
		subTs[i].store = t.store
		subTs[i].Prefix = append(t.Prefix, byte(i))
	}
	for _, p := range t.Entries {
		c := p.Key[len(t.Prefix)+1]

		ents := subTs[c].Entries
		subTs[c].Entries = append(ents, p)
	}
	return subTs
}

func (t *Trie) Validate() error {
	if len(t.Entries) > 0 && t.Children != nil {
		return errors.New("cannot be parent and leaf")
	}

	return nil
}

func (t *Trie) Delete(ctx context.Context, key []byte) error {
	return t.delete(ctx, key)
}

func (t *Trie) delete(ctx context.Context, key []byte) error {
	if t.Children == nil {
		ents := []Pair{}
		for _, p := range t.Entries {
			if bytes.Compare(p.Key, key) != 0 {
				ents = append(ents, p)
			}
		}
		t.Entries = ents
		return nil
	}
	err := t.replaceChild(ctx, key, func(x Trie) (*Trie, error) {
		err := x.delete(ctx, key)
		return &x, err
	})
	return err
}

func (t *Trie) replaceChild(ctx context.Context, key []byte, fn func(x Trie) (*Trie, error)) error {
	i, child, err := t.childFor(ctx, key)
	if err != nil {
		return err
	}
	child1, err := fn(*child)
	if err != nil {
		return err
	}
	data := child1.Marshal()
	ref, err := t.store.Post(ctx, data)
	if err != nil {
		return err
	}
	if err := t.store.Delete(ctx, t.Children[i]); err != nil {
		return err
	}
	t.Children[i] = ref
	return nil
}

func (t *Trie) childFor(ctx context.Context, key []byte) (int, *Trie, error) {
	i := int(key[len(t.Prefix)])
	child, err := t.getChild(ctx, i)
	if err != nil {
		return -1, nil, err
	}
	return i, child, nil
}

func (t *Trie) getChild(ctx context.Context, i int) (*Trie, error) {
	ref := t.Children[i]
	data, err := t.store.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	child := &Trie{}
	if err = t.UnmarshalText(data); err != nil {
		return nil, err
	}
	return child, nil
}

const (
	TypeLeaf = "leaf"
	TypeTree = "tree"
)

func (t *Trie) MarshalText() ([]byte, error) {
	buf := bytes.Buffer{}

	var ty string
	if t.Children == nil {
		ty = TypeLeaf
	} else {
		ty = TypeTree
	}

	if _, err := buf.WriteString(ty); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString("\n"); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(enc.EncodeToString(t.Prefix)); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString("\n"); err != nil {
		return nil, err
	}
	switch ty {
	case TypeTree:
		for _, ref := range t.Children {
			b64Str := enc.EncodeToString(ref[:])
			if _, err := buf.WriteString(b64Str); err != nil {
				return nil, err
			}
			if _, err := buf.WriteString("\n"); err != nil {
				return nil, err
			}
		}
	case TypeLeaf:
		for _, pair := range t.Entries {
			keyb64 := enc.EncodeToString(pair.Key)
			if _, err := buf.WriteString(keyb64); err != nil {
				return nil, err
			}
			if _, err := buf.WriteString("\t"); err != nil {
				return nil, err
			}
			valueb64 := enc.EncodeToString(pair.Value)
			if _, err := buf.WriteString(valueb64); err != nil {
				return nil, err
			}
			if _, err := buf.WriteString("\n"); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func (t *Trie) Marshal() []byte {
	data, err := t.MarshalText()
	if err != nil {
		panic(err)
	}
	return data
}

func (t *Trie) UnmarshalText(data []byte) error {
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) < 2 {
		return errors.New("Trie.Unmarshal: too few lines")
	}

	// this is to prevent keeping a reference to the whole buffer.
	prefix := make([]byte, len(lines[1]))
	copy(prefix, lines[1])
	t.Prefix = prefix

	things := lines[2:]
	switch string(lines[0]) {
	case TypeLeaf:
		t.Entries = make([]Pair, len(things))
		for i := range things {
			if len(things[i]) < 1 {
				continue
			}
			parts := bytes.SplitN(things[i], []byte("\t"), 2)
			if len(parts) < 2 {
				return errors.New("Trie.Unmarshal: invalid pair")
			}
			keyb64, valueb64 := string(parts[0]), string(parts[1])
			key, err := enc.DecodeString(keyb64)
			if err != nil {
				return err
			}
			value, err := enc.DecodeString(valueb64)
			if err != nil {
				return err
			}
			t.Entries[i] = Pair{Key: key, Value: value}
		}
	case TypeTree:
		t.Children = new([256]blobs.ID)
		for i := range things {
			copy(t.Children[i][:], things[i])
		}
	default:
		return errors.New("invalid trie type")
	}

	return nil
}

func (t *Trie) Unmarshal(data []byte) error {
	return t.UnmarshalText(data)
}

func FromBytes(store blobs.GetPostDelete, data []byte) (*Trie, error) {
	t := &Trie{store: store}
	return t, t.Unmarshal(data)
}

func (t *Trie) ID() blobs.ID {
	return blobs.Hash(t.Marshal())
}
