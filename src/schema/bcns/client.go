package bcns

import (
	"context"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/slices2"
)

// Client gives access to the namespace
// Client manages multi-volume interactions, see Tx for manipulating
// a single namespace volume.
type Client struct {
	svc           blobcache.Service
	root          blobcache.OID
	defaultSchema Namespace
	factory       schema.Factory
}

func NewClient(svc blobcache.Service, root blobcache.OID) Client {
	return Client{svc: svc, root: root, factory: schemareg.Factory}
}

func (nsc *Client) SetDefaultSchema(defaultSchema Namespace) Client {
	nsc.defaultSchema = defaultSchema
	return *nsc
}

func (nsc *Client) Root() blobcache.OID {
	return nsc.root
}

// FQP produces an FQP from p.
func (nsc *Client) FQP(ctx context.Context, p string) (FQP, error) {
	ep, err := nsc.svc.Endpoint(ctx)
	if err != nil {
		return FQP{}, err
	}
	return FQP{Node: ep.Node, Root: nsc.root, Path: p}, nil
}

func (nsc *Client) newFQP(p string) FQP {
	return FQP{Root: nsc.root, Path: p}
}

// Open returns a handle to the object at p.
func (nsc *Client) Open(ctx context.Context, p string) (blobcache.Handle, error) {
	h, sch, err := nsc.openRoot(ctx)
	if err != nil {
		return blobcache.Handle{}, err
	}
	for p != "" {
		if err := View(ctx, nsc.svc, sch, h, func(tx *Tx) error {
			ent, p, err := tx.Match(ctx, p)
			if err != nil {
				return err
			}
			nsc.defaultSchema
			ent.LinkToken()
			return nil
		}); err != nil {
			return blobcache.Handle{}, err
		}
	}
	return h, nil

}

// DoAtCtx is the context provided to the DoAt callback
type DoAtCtx struct {
	// Node is the node that owns the namespace Volume
	Node blobcache.NodeID
	// NS is the handle to the namespace Volume, that the transaction is for
	NS blobcache.Handle
	// Tx is a transaction for maniuplating the namespace
	Tx *Tx
	// Prefix is the part of the path that was matched.
	Prefix string
	// Name is the remaining name within the namespace
	Name string
}

// DoAt resolves as much of p as possible and then calls fn with the
func (nsc *Client) DoAt(ctx context.Context, p string, modify bool, fn func(DoAtCtx) error) error {
	nsh, sch, err := nsc.openRoot(ctx)
	if err != nil {
		return err
	}
}

func (nsc *Client) Put(ctx context.Context, p string, target blobcache.Handle, mask blobcache.ActionSet) error {
	return nsc.DoAt(ctx, p, true, func(dac DoAtCtx) error {
		return dac.Tx.Put(ctx, dac.Name, target, mask)
	})
}

func (nsc *Client) Get(ctx context.Context, p string, dst *Entry) (bool, error) {
	var found bool
	err := nsc.DoAt(ctx, p, false, func(dac DoAtCtx) error {
		var err error
		found, err = dac.Tx.Get(ctx, dac.Name, dst)
		return err
	})
	return found, err
}

func (nsc *Client) List(ctx context.Context, p string) ([]Entry, error) {
	nsh, err := nsc.Open(ctx, p)
	if err != nil {
		return nil, err
	}
	sch := SchemaForVolume(ctx, nsc.svc, nsh)
}

func (nsc *Client) ListNames(ctx context.Context, volh blobcache.Handle) ([]string, error) {
	ents, err := nsc.List(ctx, volh)
	if err != nil {
		return nil, err
	}
	return slices2.Map(ents, func(x Entry) string { return x.Name }), nil
}

func (nsc *Client) CreateVolumeAt(ctx context.Context, nsh blobcache.Handle, name string, spec blobcache.VolumeSpec) (blobcache.Handle, error) {
	var ret blobcache.Handle
	err := nsc.DoAt(ctx, name, func(c DoAtCtx) error {
		var ent Entry
		exists, err := c.Tx.Get(ctx, c.Name, &ent)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("cannot create, there is already an object there %s => %v", ent.Name, ent.Target)
		}
		subvolh, _, err := bcsdk.CreateOnSameHost(ctx, nsc.svc, c.NS, spec)
		if err != nil {
			return err
		}
		if _, err := c.Tx.CreateAt(ctx, c.Name, *subvolh, blobcache.Action_ALL); err != nil {
			return err
		}
		ret = *subvolh
		return err
	})
	return ret, err
}

func (nsc *Client) CreateAt(ctx context.Context, nsh blobcache.Handle, name string, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if err := CheckName(name); err != nil {
		return nil, err
	}
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

// Move atomically renames an entry from oldName to newName within a namespace volume.
// The link token is preserved as-is.
// oldName is resolved first, and newName must share the resolved prefix, or an error is returned.
func (nsc *Client) Move(ctx context.Context, nsh blobcache.Handle, oldName, newName string) error {
	return nsc.DoAt(ctx, oldName, true, func(dac DoAtCtx) error {
		nn := strings.TrimPrefix(oldName, dac.Prefix)
		nn = strings.Trim(on, string(Sep))
		return dac.Tx.Move(ctx, dac.Name, nn)
	})
	nsh, err := nsc.resolve(ctx, nsh)
	if err != nil {
		return err
	}
	if err := CheckName(newName); err != nil {
		return err
	}
	return bcsdk.ModifyTx(ctx, nsc.Service, nsh, func(tx *bcsdk.Tx, root []byte) ([]byte, error) {
	})
}

func (nsc *Client) openRoot(ctx context.Context) (blobcache.Handle, Namespace, error) {
	h, err := nsc.svc.OpenFiat(ctx, nsc.root, blobcache.Action_ALL)
	if err != nil {
		return blobcache.Handle{}, nil, err
	}
	sch, err := SchemaForVolume(ctx, nsc.svc, *h)
	if err != nil {
		return blobcache.Handle{}, nil, err
	}
	return *h, sch, nil
}

// SchemaForVolume returns a Client configured to use the Namespace schema for the Volume
// If the Volume does not have a known Schema or the Schema is not Namespace then an error is returned.
func SchemaForVolume(ctx context.Context, svc blobcache.Service, nsvolh blobcache.Handle) (Namespace, error) {
	vinfo, err := svc.InspectVolume(ctx, nsvolh)
	if err != nil {
		return nil, err
	}
	sch, err := schemareg.Factory(vinfo.Schema)
	if err != nil {
		return nil, err
	}
	nssch, ok := sch.(Namespace)
	if !ok {
		return nil, fmt.Errorf("volume has a non-namespace Schema %v", vinfo.Schema.Name)
	}
	return nssch, nil
}
