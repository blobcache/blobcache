package schema

import (
	"context"
	"fmt"
	"regexp"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

// Initializer is an interface for Schemas which support initialization.
type Initializer interface {
	Init(ctx context.Context, s bcsdk.WO) ([]byte, error)
}

// KV is an interface for Schemas which support common Key-Value operations.
type KV[K, V any] interface {
	Put(ctx context.Context, s bcsdk.RW, root []byte, key K, value V) ([]byte, error)
	Get(ctx context.Context, s bcsdk.RO, root []byte, key K, dst *V) (bool, error)
	Delete(ctx context.Context, s bcsdk.RW, root []byte, key K) ([]byte, error)
}

// NSEntry represents an entry in a namespace.
type NSEntry struct {
	// Name is the key for this entry within the namespace.
	Name string
	// Target is the OID of the entry.
	Target blobcache.OID
	// Rights is the set of rights for the entry.
	Rights blobcache.ActionSet
}

func (ent *NSEntry) Link() Link {
	return Link{
		Target: ent.Target,
		Rights: ent.Rights,
	}
}

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

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
	NSList(ctx context.Context, s bcsdk.RO, root []byte) ([]NSEntry, error)
	// NSGet retrieves the entry at the given name.
	// If the entry exists, it is returned in dst and true is returned.
	// If the entry does not exist, dst is not modified and false is returned.
	NSGet(ctx context.Context, s bcsdk.RO, root []byte, name string, dst *NSEntry) (bool, error)
	// Delete deletes the entry at the given name.
	// Delete is idempotent, and does not fail if the entry does not exist.
	NSDelete(ctx context.Context, s bcsdk.RW, root []byte, name string) ([]byte, error)
	// Put performs an idempotent create or replace operation.
	NSPut(ctx context.Context, s bcsdk.RW, root []byte, ent NSEntry) ([]byte, error)
}

// NSClient allows manipulation of namespace volumes.
type NSClient struct {
	Service  blobcache.Service
	Protocol Namespace
}

func (nsc NSClient) Init(ctx context.Context, volh blobcache.Handle) error {
	sch, ok := nsc.Protocol.(Initializer)
	if !ok {
		return fmt.Errorf("protocol does not support initialization")
	}
	volh, err := nsc.resolve(ctx, volh)
	if err != nil {
		return err
	}
	return bcsdk.Modify(ctx, nsc.Service, volh, func(s bcsdk.RW, root []byte) ([]byte, error) {
		if len(root) != 0 {
			return nil, fmt.Errorf("cannot initialize namespace, there is already something in the volume")
		}
		return sch.Init(ctx, s)
	})
}

func (nsc NSClient) Put(ctx context.Context, volh blobcache.Handle, ent NSEntry) error {
	volh, err := nsc.resolve(ctx, volh)
	if err != nil {
		return err
	}
	if err := CheckName(ent.Name); err != nil {
		return err
	}
	return bcsdk.Modify(ctx, nsc.Service, volh, func(s bcsdk.RW, root []byte) ([]byte, error) {
		return nsc.Protocol.NSPut(ctx, s, root, ent)
	})
}

func (nsc NSClient) Get(ctx context.Context, volh blobcache.Handle, name string, dst *NSEntry) (bool, error) {
	volh, err := nsc.resolve(ctx, volh)
	if err != nil {
		return false, err
	}
	return bcsdk.View1(ctx, nsc.Service, volh, func(s bcsdk.RO, root []byte) (bool, error) {
		return nsc.Protocol.NSGet(ctx, s, root, name, dst)
	})
}

func (nsc NSClient) List(ctx context.Context, volh blobcache.Handle) ([]NSEntry, error) {
	volh, err := nsc.resolve(ctx, volh)
	if err != nil {
		return nil, err
	}
	return bcsdk.View1(ctx, nsc.Service, volh, func(s bcsdk.RO, root []byte) ([]NSEntry, error) {
		return nsc.Protocol.NSList(ctx, s, root)
	})
}

func (nsc NSClient) resolve(ctx context.Context, volh blobcache.Handle) (blobcache.Handle, error) {
	if volh.Secret == ([16]byte{}) {
		volh2, err := nsc.Service.OpenFiat(ctx, volh.OID, blobcache.Action_ALL)
		if err != nil {
			return blobcache.Handle{}, err
		}
		volh = *volh2
	}
	return volh, nil
}

type NSTx struct {
	tx  *bcsdk.Tx
	sch Namespace
}

func (ntx NSTx) Put(ctx context.Context, ent NSEntry) error {
	if err := CheckName(ent.Name); err != nil {
		return err
	}
	panic("not implemented")
}
