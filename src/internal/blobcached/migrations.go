package blobcached

import (
	"context"
	"errors"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/jsonns"
)

// MigrateLinkTokenHashes rewrites legacy sha3 link-token hashes to per-volume hashes.
func (d *Daemon) MigrateLinkTokenHashes(ctx context.Context) error {
	svc, err := d.newService(ctx)
	if err != nil {
		return err
	}
	if err := bclocal.MigrateLinkTokenHashes(ctx, svc, map[string]bcns.Namespace{
		string(jsonns.SchemaName): jsonns.Schema{},
	}); err != nil {
		return errors.Join(err, svc.Close())
	}
	return svc.Close()
}
