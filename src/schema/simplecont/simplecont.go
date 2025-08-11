// Package simplecont implements a simple container for volumes.
package simplecont

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/state/cadata"
)

var (
	_ schema.Schema    = &Schema{}
	_ schema.Container = &Schema{}
)

// Schema
type Schema struct{}

func (sch Schema) Validate(ctx context.Context, s cadata.Getter, root []byte) error {
	var prev blobcache.OID
	return sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		if oid.Compare(prev) < 0 {
			return fmt.Errorf("OID %s is less than previous OID %s", oid, prev)
		}
		prev = oid
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

func (sch Schema) ListOIDs(ctx context.Context, s cadata.Getter, root []byte) ([]blobcache.OID, error) {
	var ret []blobcache.OID
	err := sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		ret = append(ret, oid)
		return nil
	})
	return ret, err
}

func (sch Schema) ListLinks(ctx context.Context, s cadata.Getter, root []byte) ([]schema.Link, error) {
	var ret []schema.Link
	err := sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		ret = append(ret, schema.Link{
			Target: oid,
			Rights: blobcache.Action_ALL,
		})
		return nil
	})
	return ret, err
}
