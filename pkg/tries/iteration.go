package tries

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
)

type Iterator struct {
	op      *Operator
	s       cadata.Store
	root    Root
	span    Span
	lastKey []byte
}

func (o *Operator) NewIterator(s cadata.Store, root Root, span Span) *Iterator {
	return &Iterator{op: o, s: s, root: root, span: span}
}

func (it *Iterator) Next(ctx context.Context) (*Entry, error) {
	var gteq []byte
	if it.lastKey != nil {
		gteq = append(it.lastKey, 0x00)
	} else {
		gteq = append([]byte{}, it.span.Begin...)
	}
	ent, err := it.op.MinEntry(ctx, it.s, it.root, gteq)
	if err != nil {
		if errors.Is(err, ErrNotExist) {
			err = io.EOF
		}
		return nil, err
	}
	it.lastKey = append(it.lastKey[:0], ent.Key...)
	return ent, nil
}

// MinEntry returns the first entry >= gteq
func (o *Operator) MinEntry(ctx context.Context, s cadata.Store, root Root, gteq []byte) (*Entry, error) {
	ents, err := o.getNode(ctx, s, root, false)
	if err != nil {
		return nil, err
	}
	gteq = compressKey(root.Prefix, gteq)
	if root.IsParent {
		for _, ent := range ents {
			if len(ent.Key) == 0 {
				if bytes.Equal(gteq, ent.Key) {
					return expandEntry(root.Prefix, ent), nil
				}
				continue
			}
			if bytes.Compare(ent.Key, gteq) >= 0 {
				root2, err := rootFromEntry(ent)
				if err != nil {
					return nil, err
				}
				minEnt, err := o.MinEntry(ctx, s, *root2, gteq)
				if err != nil {
					return nil, err
				}
				return expandEntry(root.Prefix, minEnt), nil
			}
		}
	} else {
		for _, ent := range ents {
			if bytes.Compare(ent.Key, gteq) >= 0 {
				return expandEntry(root.Prefix, ent), nil
			}
		}
	}
	return nil, ErrNotExist
}

func prefixOverlaps(prefix, first, last []byte) bool {
	return bytes.Compare(first, prefix) <= 0 &&
		(last == nil || bytes.Compare(last, prefix) > 0)
}
