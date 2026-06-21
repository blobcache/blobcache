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

// Open performs the multi-volume lookup, creating clients as required.
// It returns a Handle to the volume that the final entry points to.
func Open(ctx context.Context, bc blobcache.Service, nsRoot blobcache.Handle, p string) (blobcache.Handle, error) {
	p := fqp.Path
	for p != "" {
		nsc, err := SchemaForVolume(ctx, bc, *h)
		if err != nil {
			return blobcache.Handle{}, err
		}
		ent, rem, err := nsc.Lookup(ctx, *h, p)
		if err != nil {
			return blobcache.Handle{}, err
		}
		h2, err := nsc.Service.OpenFrom(ctx, *h, ent.LinkToken(), blobcache.Action_ALL)
		if err != nil {
			return blobcache.Handle{}, err
		}
		h = h2
		p = rem
	}
	return *h, nil
}
