package tries

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/schema"
	"github.com/pkg/errors"
	"go.brendoncarroll.net/state"
)

var (
	ErrCannotCollapse = errors.Errorf("cannot collapse parent into child")
	ErrCannotSplit    = errors.Errorf("cannot split, < 2 entries")
)

// ErrNotFound is returned when a key cannot be found.
type ErrNotFound = state.ErrNotFound[[]byte]

func IsErrNotFound(err error) bool {
	return state.IsErrNotFound[[]byte](err)
}

type Span = state.ByteSpan

type Root struct {
	Ref      Ref
	IsParent bool   `json:"is_parent"`
	Count    uint64 `json:"count"`
	Prefix   []byte `json:"prefix"`
}

func ParseRoot(x []byte) (*Root, error) {
	var root Root
	if err := json.Unmarshal(x, &root); err != nil {
		return nil, err
	}
	return &root, nil
}

func (r *Root) Marshal() []byte {
	data, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return data
}

func rootFromEntry(ent *Entry) (*Root, error) {
	var idx Index
	if err := idx.Unmarshal(ent.Value); err != nil {
		return nil, err
	}
	return &Root{
		Ref:      idx.Ref,
		IsParent: idx.IsParent,
		Count:    idx.Count,

		Prefix: ent.Key,
	}, nil
}

func entryFromRoot(x Root) *Entry {
	idx := &Index{
		Ref:      x.Ref,
		IsParent: x.IsParent,
		Count:    x.Count,
	}
	data, err := idx.Marshal()
	if err != nil {
		panic(err)
	}
	return &Entry{
		Key:   x.Prefix,
		Value: data,
	}
}

func (o *Machine) Validate(ctx context.Context, s schema.RO, x Root) error {
	// getEntries includes validation
	ents, err := o.getNode(ctx, s, x, false)
	if err != nil {
		return err
	}
	if x.IsParent {
		for _, ent := range ents {
			root, err := rootFromEntry(ent)
			if err != nil {
				return err
			}
			if err := o.Validate(ctx, s, *root); err != nil {
				return err
			}
		}
	}
	return nil
}
