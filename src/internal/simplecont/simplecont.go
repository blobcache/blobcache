// Package simplecont implements a simple container for volumes.
package simplecont

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
	"go.brendoncarroll.net/state/cadata"
)

var (
	_ volumes.Schema    = &Schema{}
	_ volumes.Container = &Schema{}
)

// Schema
type Schema struct{}

func (sch Schema) Validate(ctx context.Context, s cadata.Getter, root []byte) error {
	return sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		return nil
	})
}

func (sch Schema) WalkOIDs(ctx context.Context, s cadata.Getter, cid blobcache.CID, fn func(blobcache.OID) error) error {
	buf := make([]byte, blobcache.CIDBytes*2)
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return err
	}
	switch n {
	case 16:
		return nil // base case, this is a Volume's OID
	case 2 * blobcache.CIDBytes:
		// this is a Merkle tree node, so we need to validate the children
		left := cadata.IDFromBytes(buf[:blobcache.CIDBytes])
		right := cadata.IDFromBytes(buf[blobcache.CIDBytes:])
		for _, cid2 := range []cadata.ID{left, right} {
			if err := sch.Validate(ctx, s, cid2[:]); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid root: %d bytes", n)
	}
}

func (sch Schema) ListLinks(ctx context.Context, s cadata.Getter, root []byte) ([]volumes.Link, error) {
	var ret []volumes.Link
	err := sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		ret = append(ret, volumes.Link{
			Target: oid,
			Rights: blobcache.Action_ALL,
		})
		return nil
	})
	return ret, err
}
