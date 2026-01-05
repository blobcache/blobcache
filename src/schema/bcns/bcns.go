package bcns

import (
	"context"
	"fmt"
	"regexp"
	"slices"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/slices2"
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
	NSList(ctx context.Context, s bcsdk.RO, root []byte) ([]Entry, error)
	// NSGet retrieves the entry at the given name.
	// If the entry exists, it is returned in dst and true is returned.
	// If the entry does not exist, dst is not modified and false is returned.
	NSGet(ctx context.Context, s bcsdk.RO, root []byte, name string, dst *Entry) (bool, error)
	// Delete deletes the entry at the given name.
	// Delete is idempotent, and does not fail if the entry does not exist.
	NSDelete(ctx context.Context, s bcsdk.RW, root []byte, name string) ([]byte, error)
	// Put performs an idempotent create or replace operation.
	NSPut(ctx context.Context, s bcsdk.RW, root []byte, ent Entry) ([]byte, error)
}

// Client allows manipulation of namespace volumes.
type Client struct {
	Service blobcache.Service
	Schema  Namespace
}

func (nsc *Client) Init(ctx context.Context, volh blobcache.Handle) error {
	sch, ok := nsc.Schema.(schema.Initializer)
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

func (nsc *Client) Put(ctx context.Context, nsh blobcache.Handle, name string, volh blobcache.Handle, mask blobcache.ActionSet) error {
	nsh, err := nsc.resolve(ctx, nsh)
	if err != nil {
		return err
	}
	if err := CheckName(name); err != nil {
		return err
	}
	return bcsdk.ModifyTx(ctx, nsc.Service, nsh, func(tx *bcsdk.Tx, root []byte) ([]byte, error) {
		lt, err := tx.Link(ctx, volh, mask)
		if err != nil {
			return nil, err
		}
		ent := Entry{
			Name:   name,
			Target: lt.Target,
			Rights: lt.Rights,
			Secret: lt.Secret,
		}
		return nsc.Schema.NSPut(ctx, tx, root, ent)
	})
}

func (nsc *Client) Delete(ctx context.Context, nsh blobcache.Handle, name string) error {
	nsh, err := nsc.resolve(ctx, nsh)
	if err != nil {
		return err
	}
	return bcsdk.ModifyTx(ctx, nsc.Service, nsh, func(tx *bcsdk.Tx, root []byte) ([]byte, error) {
		var ent Entry
		found, err := nsc.Schema.NSGet(ctx, tx, root, name, &ent)
		if err != nil {
			return nil, err
		}
		if !found {
			// no change needed
			return root, nil
		}
		root, err = nsc.Schema.NSDelete(ctx, tx, root, name)
		if err != nil {
			return nil, err
		}
		ents, err := nsc.Schema.NSList(ctx, tx, root)
		if err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(ents, func(x Entry) bool {
			return x.Target == ent.Target
		}) {
			// if the target is not referenced by any other entry, unlink it
			if err := tx.Unlink(ctx, []blobcache.LinkToken{ent.LinkToken()}); err != nil {
				return nil, err
			}
		}
		return root, nil
	})
}

func (nsc *Client) Get(ctx context.Context, volh blobcache.Handle, name string, dst *Entry) (bool, error) {
	volh, err := nsc.resolve(ctx, volh)
	if err != nil {
		return false, err
	}
	return bcsdk.View1(ctx, nsc.Service, volh, func(s bcsdk.RO, root []byte) (bool, error) {
		return nsc.Schema.NSGet(ctx, s, root, name, dst)
	})
}

func (nsc *Client) List(ctx context.Context, volh blobcache.Handle) ([]Entry, error) {
	volh, err := nsc.resolve(ctx, volh)
	if err != nil {
		return nil, err
	}
	return bcsdk.View1(ctx, nsc.Service, volh, func(s bcsdk.RO, root []byte) ([]Entry, error) {
		return nsc.Schema.NSList(ctx, s, root)
	})
}

func (nsc *Client) ListNames(ctx context.Context, volh blobcache.Handle) ([]string, error) {
	ents, err := nsc.List(ctx, volh)
	if err != nil {
		return nil, err
	}
	return slices2.Map(ents, func(x Entry) string { return x.Name }), nil
}

func (nsc *Client) OpenAt(ctx context.Context, nsh blobcache.Handle, name string, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	nsh, err := nsc.resolve(ctx, nsh)
	if err != nil {
		return nil, err
	}
	var ent Entry
	found, err := nsc.Get(ctx, nsh, name, &ent)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("ns: no entry found at %s", name)
	}
	subvolh, err := nsc.Service.OpenFrom(ctx, nsh, ent.LinkToken(), blobcache.Action_ALL)
	if err != nil {
		return nil, err
	}
	return subvolh, nil
}

func (nsc *Client) CreateAt(ctx context.Context, nsh blobcache.Handle, name string, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	nsh, err := nsc.resolve(ctx, nsh)
	if err != nil {
		return nil, err
	}
	volh, _, err := bcsdk.CreateOnSameHost(ctx, nsc.Service, nsh, spec)
	if err != nil {
		return nil, err
	}
	if err := bcsdk.ModifyTx(ctx, nsc.Service, nsh, func(tx *bcsdk.Tx, root []byte) ([]byte, error) {
		found, err := nsc.Schema.NSGet(ctx, tx, root, name, new(Entry))
		if err != nil {
			return nil, err
		}
		if found {
			return nil, fmt.Errorf("ns: entry already exists at %s", name)
		}
		lt, err := tx.Link(ctx, *volh, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		return nsc.Schema.NSPut(ctx, tx, root, Entry{
			Name:   name,
			Target: lt.Target,
			Rights: lt.Rights,
			Secret: lt.Secret,
		})
	}); err != nil {
		return nil, err
	}
	return volh, nil
}

// GC garbage collects the volume
func (nsc *Client) GC(ctx context.Context, volh blobcache.Handle) error {
	gcsch, ok := nsc.Schema.(schema.VisitAll)
	if !ok {
		return fmt.Errorf("cannot GC, schema does not support visit all")
	}
	tx, err := bcsdk.BeginTx(ctx, nsc.Service, volh, blobcache.TxParams{Modify: true, GCBlobs: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	visit := func(cids []blobcache.CID, ltoks []blobcache.LinkToken) error {
		if len(cids) > 0 {
			if err := tx.Visit(ctx, cids); err != nil {
				return err
			}
		}
		if len(ltoks) > 0 {
			if err := tx.VisitLinks(ctx, ltoks); err != nil {
				return err
			}
		}
		return nil
	}
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	if err := gcsch.VisitAll(ctx, tx, root, visit); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (nsc Client) resolve(ctx context.Context, volh blobcache.Handle) (blobcache.Handle, error) {
	if volh.Secret == ([16]byte{}) {
		volh2, err := nsc.Service.OpenFiat(ctx, volh.OID, blobcache.Action_ALL)
		if err != nil {
			return blobcache.Handle{}, err
		}
		volh = *volh2
	}
	return volh, nil
}
