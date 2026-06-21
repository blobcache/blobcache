// Package bcnstests implements a test suite for bcns
package bcnstests

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/schema/bcns"
)

type (
	Scene       = blobcachetests.Scene
	Volume      = blobcachetests.Volume
	LocalVolume = blobcachetests.LocalVolume
)

// Entry is an namespace entry
type Entry struct {
	Name   string
	Rights blobcache.ActionSet
	// Target refers to a Volume on the same node by index
	Target int
}

var _ blobcachetests.Contents = &NS{}

// NS is namespace contents for a volume
type NS struct {
	Schema  bcns.Namespace
	Entries []Entry
}

func (n *NS) Fill(ctx context.Context, tx *bcsdk.Tx, nodes []blobcache.NodeID, vols [][]blobcache.OID) error {
	_ = nodes
	if n == nil {
		return nil
	}
	if n.Schema == nil {
		return fmt.Errorf("namespace schema is nil")
	}
	if vols == nil || len(vols) < 1 {
		return fmt.Errorf("no volumes provided")
	}
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	for _, ent := range n.Entries {
		if ent.Target < 0 || ent.Target >= len(vols[0]) {
			return fmt.Errorf("entry target out of range: %d", ent.Target)
		}
		lt, err := tx.Link(ctx, blobcache.Handle{OID: vols[0][ent.Target]}, ent.Rights)
		if err != nil {
			return err
		}
		root, err = n.Schema.NSPut(ctx, tx, root, bcns.Entry{
			Name:   ent.Name,
			Target: lt.Target,
			Rights: lt.Rights,
			Secret: lt.Secret,
		})
		if err != nil {
			return err
		}
	}
	return tx.Save(ctx, root)
}
