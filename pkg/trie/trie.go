package trie

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

const maxEntries = 512

type GetPostDelete interface {
	Post(ctx context.Context, key []byte) (blobs.ID, error)
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
}

type Pair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type Trie struct {
	store blobs.Store

	Prefix   []byte         `json:"prefix"`
	Children *[256]blobs.ID `json:"children"`
	Entries  []Pair         `json:"entries"`
}

func New(store blobs.Store) *Trie {
	return &Trie{
		store: store,
	}
}

func (t *Trie) Put(ctx context.Context, pair Pair) error {
	if err := t.Validate(); err != nil {
		return err
	}
	return t.put(ctx, pair)
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
		err := x.Put(ctx, pair)
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
	t.Children[i] = ref
	return nil
}

func (t *Trie) childFor(ctx context.Context, key []byte) (int, *Trie, error) {
	i := int(key[len(t.Prefix)])
	ref := t.Children[i]
	data, err := t.store.Get(ctx, ref)
	if err != nil {
		return -1, nil, err
	}
	child := &Trie{}
	if err = json.Unmarshal(data, child); err != nil {
		return -1, nil, err
	}
	return i, child, nil
}

const (
	typeLeaf = "leaf"
	typeTree = "tree"
)

func (t *Trie) Marshal() []byte {
	buf := bytes.Buffer{}

	var ty string
	if t.Children == nil {
		ty = typeLeaf
	} else {
		ty = typeTree
	}

	if _, err := buf.WriteString(ty); err != nil {
		panic(err)
	}
	if _, err := buf.WriteString("\n"); err != nil {
		panic(err)
	}
	if _, err := buf.WriteString(hex.EncodeToString(t.Prefix)); err != nil {
		panic(err)
	}
	if _, err := buf.WriteString("\n"); err != nil {
		panic(err)
	}
	for _, ref := range t.Children {
		if _, err := buf.WriteString(hex.EncodeToString(ref[:])); err != nil {
			panic(err)
		}
		if _, err := buf.WriteString("\n"); err != nil {
			panic(err)
		}
	}
	return buf.Bytes()
}

func (t *Trie) Unmarshal(data []byte) error {
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
	case typeLeaf:
		for i := range things {
			parts := bytes.SplitN(things[i], []byte("\t"), 2)
			if len(parts) < 2 {
				return errors.New("Trie.Unmarshal: invalid pair")
			}
			key, value := parts[0], parts[1]
			t.Entries[i] = Pair{Key: key, Value: value}
		}
	case typeTree:
		t.Children = new([256]blobs.ID)
		for i := range things {
			copy(t.Children[i][:], things[i])
		}
	default:
		return errors.New("invalid trie type")
	}

	return nil
}

func FromBytes(store blobs.Store, data []byte) (*Trie, error) {
	t := &Trie{store: store}
	return t, t.Unmarshal(data)
}
