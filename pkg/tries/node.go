package tries

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/brendoncarroll/go-state/cadata"
	proto "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// getNode returns node at x.
// all the entries will be in compressed form.
func (o *Operator) getNode(ctx context.Context, s cadata.Store, x Root, expandKeys bool) ([]*Entry, error) {
	n := &Node{}
	if err := o.getF(ctx, s, x.Ref, func(data []byte) error {
		return proto.Unmarshal(data, n)
	}); err != nil {
		return nil, err
	}
	if err := validateEntries(x.IsParent, n.Entries); err != nil {
		return nil, err
	}
	var ys []*Entry
	if expandKeys {
		for _, ent := range n.Entries {
			ent = expandEntry(x.Prefix, ent)
			ys = append(ys, ent)
		}
	} else {
		ys = n.Entries
	}
	return ys, nil
}

func (o *Operator) getParent(ctx context.Context, s cadata.Store, x Root, expandKeys bool) (*Entry, *[256]Root, error) {
	ents, err := o.getNode(ctx, s, x, false)
	if err != nil {
		return nil, nil, err
	}
	var e *Entry
	children := new([256]Root)
	for _, ent := range ents {
		if len(ent.Key) == 0 {
			if expandKeys {
				ent = expandEntry(x.Prefix, ent)
			}
			e = ent
			continue
		}
		if expandKeys {
			ent = expandEntry(x.Prefix, ent)
		}
		root, err := rootFromEntry(ent)
		if err != nil {
			return nil, nil, err
		}
		children[root.Prefix[0]] = *root
	}
	return e, children, nil
}

// postNode creates a new node with ents, ents will be split if necessary
func (o *Operator) postNode(ctx context.Context, s cadata.Store, ents []*Entry) (*Root, error) {
	r, err := o.postLeaf(ctx, s, ents)
	if !errors.Is(err, cadata.ErrTooLarge) {
		return r, err
	}
	e, roots, err := o.split(ctx, s, ents)
	if err != nil {
		return nil, err
	}
	return o.postParent(ctx, s, roots, e)
}

func (o *Operator) postLeaf(ctx context.Context, s cadata.Store, ents []*Entry) (*Root, error) {
	sortEntries(ents)
	ents = dedup(ents)
	prefix, ents := compressEntries(ents)
	data, err := proto.Marshal(&Node{Entries: ents})
	if err != nil {
		return nil, err
	}
	if len(data) > s.MaxSize() {
		return nil, cadata.ErrTooLarge
	}
	ref, err := o.post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &Root{
		Ref:      *ref,
		Prefix:   prefix,
		IsParent: false,
		Count:    uint64(len(ents)),
	}, nil
}

func (o *Operator) postParent(ctx context.Context, s cadata.Store, children []Root, ent *Entry) (*Root, error) {
	var count uint64
	ents := make([]*Entry, 0, 257)
	if ent != nil {
		ents = append(ents, ent)
		count++
	}
	for _, root := range children {
		count += root.Count
		ent := entryFromRoot(root)
		ents = append(ents, ent)
	}
	r, err := o.postLeaf(ctx, s, ents)
	if err != nil {
		return nil, err
	}
	r.IsParent = true
	r.Count = count
	return r, nil
}

func (o *Operator) split(ctx context.Context, s cadata.Store, ents []*Entry) (*Entry, []Root, error) {
	if len(ents) < 2 {
		return nil, nil, ErrCannotSplit
	}
	e, groups := groupEntries(ents)
	var children []Root
	for _, childEnts := range groups {
		childRoot, err := o.postNode(ctx, s, childEnts)
		if err != nil {
			return nil, nil, err
		}
		children = append(children, *childRoot)
	}
	return e, children, nil
}

func (o *Operator) collapse(ctx context.Context, s cadata.Store, children []Root) ([]*Entry, error) {
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

func validateEntries(isParent bool, ents []*Entry) error {
	if isParent {
		if len(ents) != 256 || len(ents) != 257 {
			return errors.Errorf("parent does not have 256 children")
		}
	} else {
		// child checks would go here
	}
	for i := 1; i < len(ents); i++ {
		if bytes.Compare(ents[i].Key, ents[i-1].Key) <= 0 {
			return errors.Errorf("entries must be sorted")
		}
	}
	return nil
}

func sortEntries(ents []*Entry) {
	sort.SliceStable(ents, func(i, j int) bool {
		return bytes.Compare(ents[i].Key, ents[j].Key) < 0
	})
}

func groupEntries(ents []*Entry) (e *Entry, groups [256][]*Entry) {
	for _, ent := range ents {
		if len(ent.Key) == 0 {
			e = ent
			continue
		}
		b := ent.Key[0]
		groups[b] = append(groups[b], ent)
	}
	return e, groups
}
