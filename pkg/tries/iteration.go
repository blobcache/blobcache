package tries

import (
	"bytes"
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
)

func ForEach(ctx context.Context, s blobs.Store, x Ref, first, last []byte, fn func(key, value []byte) error) error {
	n, err := GetNode(ctx, s, x)
	if err != nil {
		return err
	}
	for _, ent := range n.Entries {
		var key []byte
		key = append(key, n.Prefix...)
		key = append(key, ent.Key...)
		if bytes.Compare(first, key) > 0 {
			continue
		} else if last != nil && bytes.Compare(last, key) <= 0 {
			break
		}
		if err := fn(ent.Key, ent.Value); err != nil {
			return err
		}
	}
	for i, child := range n.Children {
		if child == nil {
			continue
		}
		var prefix []byte
		prefix = append(prefix, n.Prefix...)
		prefix = append(prefix, uint8(i))
		if !prefixOverlaps(prefix, first, last) {
			continue
		}
		childRef := fromChildProto(child)
		if err := ForEach(ctx, s, childRef, first, last, fn); err != nil {
			return err
		}
	}
	return nil
}

func prefixOverlaps(prefix, first, last []byte) bool {
	return bytes.Compare(first, prefix) <= 0 &&
		(last == nil || bytes.Compare(last, prefix) > 0)
}
