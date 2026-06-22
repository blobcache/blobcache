package bcns

import (
	"context"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"go.brendoncarroll.net/exp/slices2"
)

// Client gives access to the Namespace
// Client manages multi-volume interactions, see Tx for manipulating a single Namespace volume.
// The Client deals with paths, and the Tx deals with names.
type Client struct {
	svc           blobcache.Service
	root          blobcache.OID
	defaultSchema Namespace
}

func NewClient(svc blobcache.Service, root blobcache.OID) Client {
	return Client{svc: svc, root: root}
}

func (nsc *Client) SetDefaultSchema(defaultSchema Namespace) {
	nsc.defaultSchema = defaultSchema
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
	return FQP{Node: ep.Node, NS: nsc.root, Path: p}, nil
}

func (nsc *Client) newFQP(p string) FQP {
	return FQP{NS: nsc.root, Path: p}
}

// Init initializes the root
func (nsc *Client) Init(ctx context.Context) error {
	h, sch, err := nsc.openRoot(ctx)
	if err != nil {
		return err
	}
	return Init(ctx, nsc.svc, sch, h)
}

// OpenFrom opens the path p from the namespace in nsh.
func (nsc *Client) OpenFrom(ctx context.Context, nsh blobcache.Handle, p string, mask blobcache.ActionSet) (blobcache.Handle, error) {
	sch, err := nsc.schemaForVolume(ctx, nsh)
	if err != nil {
		return blobcache.Handle{}, err
	}
	return nsc.openFrom(ctx, nsh, sch, p, mask)
}

func (nsc *Client) openFrom(ctx context.Context, h blobcache.Handle, sch Namespace, p string, mask blobcache.ActionSet) (blobcache.Handle, error) {
	p = strings.Trim(p, string(Sep))
	for p != "" {
		sch, err := nsc.schemaForVolume(ctx, h)
		if err != nil {
			return blobcache.Handle{}, err
		}
		var ent Entry
		var rem string
		if err := View(ctx, nsc.svc, sch, h, func(tx *Tx) error {
			var err error
			ent, rem, err = tx.Match(ctx, p)
			return err
		}); err != nil {
			return blobcache.Handle{}, err
		}
		h2, err := nsc.svc.OpenFrom(ctx, h, ent.LinkToken(), ent.Rights)
		if err != nil {
			return blobcache.Handle{}, err
		}
		h = *h2
		p = rem
		if p == "" {
			return h, nil
		}
	}
	return h, nil
}

// Open returns a handle to the object at p.
func (nsc *Client) Open(ctx context.Context, p string, mask blobcache.ActionSet) (blobcache.Handle, error) {
	h, sch, err := nsc.openRoot(ctx)
	if err != nil {
		return blobcache.Handle{}, err
	}
	return nsc.openFrom(ctx, h, sch, p, mask)
}

// Resolve returns a resolved path.
// A resolved path can be looked up in a single namespace volume, and does not require
// traversal.
// It may not be directly accessible with OpenFiat.
func (nsc *Client) Resolve(ctx context.Context, p string) (FQP, error) {
	var ret FQP
	err := nsc.Do(ctx, p, false, func(dc DoCtx) error {
		ret.Node = dc.Node
		ret.Path = dc.Name
		ret.NS = dc.NS.OID
		return nil
	})
	return ret, err
}

// DoCtx is the context provided to the Do callback
type DoCtx struct {
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

// Do resolves as much of p as possible and then calls fn with the
func (nsc *Client) Do(ctx context.Context, p string, modify bool, fn func(DoCtx) error) error {
	p = strings.Trim(p, string(Sep))
	nsh, sch, err := nsc.openRoot(ctx)
	if err != nil {
		return err
	}
	prefix := ""
	for {
		node, err := nsc.volumeNode(ctx, nsh)
		if err != nil {
			return err
		}
		var nextNS *blobcache.Handle
		var nextPath string
		invoke := func(tx *Tx) error {
			ent, rem, err := tx.Match(ctx, p)
			if err != nil {
				if _, ok := err.(*ErrNoMatch); ok {
					return fn(DoCtx{
						Node:   node,
						NS:     nsh,
						Tx:     tx,
						Prefix: prefix,
						Name:   p,
					})
				}
				return err
			}
			if rem == "" {
				return fn(DoCtx{
					Node:   node,
					NS:     nsh,
					Tx:     tx,
					Prefix: prefix,
					Name:   ent.Name,
				})
			}
			h, err := nsc.svc.OpenFrom(ctx, nsh, ent.LinkToken(), ent.Rights)
			if err != nil {
				return err
			}
			nextNS = h
			nextPath = rem
			if prefix == "" {
				prefix = ent.Name
			} else {
				prefix = prefix + string(Sep) + ent.Name
			}
			return nil
		}
		if modify {
			if err := Modify(ctx, nsc.svc, sch, nsh, invoke); err != nil {
				return err
			}
		} else {
			if err := View(ctx, nsc.svc, sch, nsh, invoke); err != nil {
				return err
			}
		}
		if nextNS == nil {
			return nil
		}
		nsh = *nextNS
		p = nextPath
		sch, err = nsc.schemaForVolume(ctx, nsh)
		if err != nil {
			return err
		}
	}
}

func (nsc *Client) Put(ctx context.Context, p string, target blobcache.Handle, mask blobcache.ActionSet) error {
	return nsc.Do(ctx, p, true, func(dac DoCtx) error {
		return dac.Tx.Put(ctx, dac.Name, target, mask)
	})
}

func (nsc *Client) Get(ctx context.Context, p string, dst *Entry) (bool, error) {
	var found bool
	err := nsc.Do(ctx, p, false, func(dac DoCtx) error {
		var err error
		found, err = dac.Tx.Get(ctx, dac.Name, dst)
		return err
	})
	return found, err
}

func (nsc *Client) List(ctx context.Context, p string) ([]Entry, error) {
	nsh, err := nsc.Open(ctx, p, blobcache.Action_ALL)
	if err != nil {
		return nil, err
	}
	sch, err := nsc.schemaForVolume(ctx, nsh)
	if err != nil {
		return nil, err
	}
	var ents []Entry
	err = View(ctx, nsc.svc, sch, nsh, func(tx *Tx) error {
		var err error
		ents, err = tx.List(ctx)
		return err
	})
	return ents, err
}

func (nsc *Client) ListNames(ctx context.Context, p string) ([]string, error) {
	ents, err := nsc.List(ctx, p)
	if err != nil {
		return nil, err
	}
	return slices2.Map(ents, func(x Entry) string { return x.Name }), nil
}

func (nsc *Client) CreateVolume(ctx context.Context, p string, spec blobcache.VolumeSpec) (blobcache.Handle, error) {
	var ret blobcache.Handle
	err := nsc.Do(ctx, p, true, func(c DoCtx) error {
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
		if err := c.Tx.Create(ctx, c.Name, *subvolh, blobcache.Action_ALL); err != nil {
			return err
		}
		ret = *subvolh
		return nil
	})
	return ret, err
}

func (nsc *Client) Delete(ctx context.Context, p string) error {
	return nsc.Do(ctx, p, true, func(dac DoCtx) error {
		return dac.Tx.Delete(ctx, dac.Name)
	})
}

// Move atomically renames an entry from oldPath to newPath within a namespace volume.
// The link token is preserved as-is.
// oldPath is resolved first, and newPath must share the resolved prefix, or an error is returned.
func (nsc *Client) Move(ctx context.Context, oldPath, newPath string) error {
	oldPath = strings.Trim(oldPath, string(Sep))
	newPath = strings.Trim(newPath, string(Sep))

	return nsc.Do(ctx, oldPath, true, func(dac DoCtx) error {
		pfx := strings.Trim(dac.Prefix+string(Sep), string(Sep))
		if !strings.HasPrefix(newPath, pfx) {
			return fmt.Errorf("new name %q does not share resolved prefix %q", newPath, dac.Prefix)
		}
		newName := strings.TrimPrefix(newPath, pfx)
		if newName == "" {
			return fmt.Errorf("new name resolves to empty name")
		}
		if err := CheckName(newName); err != nil {
			return err
		}
		return dac.Tx.Move(ctx, dac.Name, newName)
	})
}

func (nsc *Client) openRoot(ctx context.Context) (blobcache.Handle, Namespace, error) {
	h, err := nsc.svc.OpenFiat(ctx, nsc.root, blobcache.Action_ALL)
	if err != nil {
		return blobcache.Handle{}, nil, err
	}
	sch, err := nsc.schemaForVolume(ctx, *h)
	if err != nil {
		return blobcache.Handle{}, nil, err
	}
	return *h, sch, nil
}

func (nsc *Client) schemaForVolume(ctx context.Context, nsvolh blobcache.Handle) (Namespace, error) {
	sch, err := SchemaForVolume(ctx, nsc.svc, nsvolh)
	if err == nil {
		return sch, nil
	}
	if nsc.defaultSchema != nil {
		return nsc.defaultSchema, nil
	}
	return nil, err
}

func (nsc *Client) volumeNode(ctx context.Context, volh blobcache.Handle) (blobcache.NodeID, error) {
	vi, err := nsc.svc.InspectVolume(ctx, volh)
	if err != nil {
		return blobcache.NodeID{}, err
	}
	switch {
	case vi.Backend.Remote != nil:
		return vi.Backend.Remote.Endpoint.Node, nil
	case vi.Backend.Peer != nil:
		return vi.Backend.Peer.Peer, nil
	default:
		ep, err := nsc.svc.Endpoint(ctx)
		if err != nil {
			return blobcache.NodeID{}, err
		}
		return ep.Node, nil
	}
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
