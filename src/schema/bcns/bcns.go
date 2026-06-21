package bcns

import (
	"context"
	"fmt"
	"regexp"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

// Entry represents an entry in a namespace.
type Entry struct {
	// Name is the key for this entry within the namespace.
	Name string `json:"name"`
	// Target is the OID of the entry.
	Target blobcache.OID `json:"target"`
	// Rights is the set of rights for the entry.
	Rights blobcache.ActionSet `json:"rights"`
	Secret blobcache.LTSecret  `json:"secret"`
}

func (ent *Entry) LinkToken() blobcache.LinkToken {
	return blobcache.LinkToken{
		Target: ent.Target,
		Rights: ent.Rights,
		Secret: ent.Secret,
	}
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9/_.-]*[A-Za-z0-9])?$`)

func IsValidName(name string) bool {
	return nameRe.MatchString(name)
}

func CheckName(name string) error {
	if !IsValidName(name) {
		return fmt.Errorf("invalid name: %q. names must match %s", name, nameRe.String())
	}
	return nil
}

// Namespace is an interface for Schemas which support common Namespace operations.
type Namespace interface {
	schema.Schema

	NSList(c schema.ROCtx) ([]Entry, error)
	// NSGet retrieves the entry at the given name.
	// If the entry exists, it is returned in dst and true is returned.
	// If the entry does not exist, dst is not modified and false is returned.
	NSGet(c schema.ROCtx, name string, dst *Entry) (bool, error)
	// Delete deletes the entry at the given name.
	// Delete is idempotent, and does not fail if the entry does not exist.
	NSDelete(c schema.RWCtx, name string) ([]byte, error)
	// Put performs an idempotent create or replace operation.
	NSPut(c schema.RWCtx, ent Entry) ([]byte, error)
}

// Init initializes a namespace in a Volume.
func Init(ctx context.Context, svc blobcache.Service, sch Namespace, volh blobcache.Handle) error {
	return Modify(ctx, svc, sch, volh, func(tx *Tx) error {
		return tx.Init(ctx)
	})
}
