package tries

import (
	"bytes"
	"context"
	"fmt"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/streams"
)

type Iterator struct {
	m    *Machine
	s    schema.RO
	root Root

	span    Span
	lastKey []byte
}

func (mach *Machine) NewIterator(s schema.RO, root Root, span Span) *Iterator {
	return &Iterator{m: mach, s: s, root: root, span: span}
}

func (it *Iterator) Next(ctx context.Context, dst []Entry) (int, error) {
	var gteq []byte
	if it.lastKey != nil {
		gteq = append(it.lastKey, 0x00)
	} else {
		gteq = append([]byte{}, it.span.Begin...)
	}
	if ok, err := it.m.MinEntry(ctx, it.s, it.root, gteq, &dst[0]); err != nil {
		return 0, err
	} else if !ok {
		return 0, streams.EOS()
	}
	it.lastKey = append(it.lastKey[:0], dst[0].Key...)
	return 1, nil
}

// MinEntry returns the first entry >= gteq
func (mach *Machine) MinEntry(ctx context.Context, s schema.RO, root Root, gteq []byte, dst *Entry) (bool, error) {
	node, err := mach.getNode(ctx, s, Index(root))
	if err != nil {
		return false, err
	}
	gteq = compressKey(root.Prefix, gteq)
	if ok, err := mach.minEntry(ctx, s, *node, gteq, dst); err != nil {
		return false, err
	} else if ok {
		dst.Key = expandKey(root.Prefix, dst.Key)
		return true, nil
	} else {
		return false, nil
	}
}

func (mach *Machine) minEntry(ctx context.Context, s schema.RO, node triescnp.Node, gteq []byte, dst *Entry) (bool, error) {
	el, err := node.Entries()
	if err != nil {
		return false, err
	}
	for i := 0; i < el.Len(); i++ {
		xent := el.At(i)
		k, err := xent.Key()
		if err != nil {
			return false, err
		}

		switch xent.Which() {
		case triescnp.Entry_Which_value:
			if bytes.Compare(k, gteq) >= 0 {
				if err := dst.fromCNP(xent); err != nil {
					return false, err
				}
				return true, nil
			}

		case triescnp.Entry_Which_index:
			var ient Index
			if err := ient.fromCNP(xent); err != nil {
				return false, err
			}
			var gteq2 []byte
			if bytes.Compare(gteq, k) <= 0 {
				gteq2 = compressKey(gteq, k)
			}
			ok, err := mach.MinEntry(ctx, s, Root(ient), gteq2, dst)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			dst.Key = expandKey(dst.Key, k)
			return true, nil
		default:
			return false, fmt.Errorf("unknown entry type: %v", xent.Which())
		}
	}
	// no entry >= gteq
	return false, nil
}
