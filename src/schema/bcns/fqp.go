package bcns

import (
	"context"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"go.inet256.org/inet256/src/inet256"
)

const Sep = '/'

// FQP is a fully qualified path
// FQPs can be written as `{Node};{NSRoot}/{Path}`
// The zero NodeID is used to refer to the local node, and it can be ommitted.
type FQP struct {
	// Node is the node that the root namespace Volume is on.
	// If this is zero, then assume the local node.
	Node blobcache.NodeID
	// Root is the root namespace, it must be accessible by OpenFiat on Node.
	Root blobcache.OID
	// Path is a slash separated path.
	Path string
}

func (fqp FQP) String() string {
	sb := strings.Builder{}
	if !fqp.Node.IsZero() {
		sb.WriteString(fqp.Node.String())
		sb.WriteByte(';')
	}
	if fqp.Root != (blobcache.OID{}) {
		sb.WriteString(fqp.Root.String())
	}
	if fqp.Path != "" {
		sb.WriteByte(Sep)
		sb.WriteString(strings.Trim(fqp.Path, "/"))
	}
	return sb.String()
}

func ParseFQP(x string) (FQP, error) {
	if x == "" {
		// This is the root on the local node
		return FQP{}, nil
	}
	parts := strings.SplitN(x, ";", 2)

	var p string
	oidAndPath := parts[len(parts)-1]
	firstSep := strings.Index(oidAndPath, string(Sep))
	var oid blobcache.OID
	var err error
	if firstSep < 0 {
		oid, err = blobcache.ParseOID(oidAndPath)
		if err != nil {
			return FQP{}, err
		}
	} else {
		oid, err = blobcache.ParseOID(oidAndPath[:firstSep])
		if err != nil {
			return FQP{}, err
		}
		p = strings.Trim(oidAndPath[firstSep:], string(Sep))
	}

	var node blobcache.NodeID
	if len(parts) == 2 {
		node, err = inet256.ParseAddrBase64([]byte(parts[0]))
		if err != nil {
			return FQP{}, err
		}
	}
	return FQP{Node: node, Root: oid, Path: p}, nil
}

func (fqp FQP) Open(ctx context.Context, bc blobcache.Service) (blobcache.Handle, error) {
	return Resolve(ctx, bc, fqp)
}

// Resolve performs the multi-volume lookup, creating clients as required.
// It returns a Handle to the volume that the final entry points to.
func Resolve(ctx context.Context, bc blobcache.Service, fqp FQP) (blobcache.Handle, error) {
	h, err := bc.OpenFiat(ctx, fqp.Root, blobcache.Action_ALL)
	if err != nil {
		return blobcache.Handle{}, err
	}
	p := fqp.Path
	for p != "" {
		nsc, err := ClientForVolume(ctx, bc, *h)
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

// DoAtCtx is the context provided to the DoAt callback
type DoAtCtx struct {
	// Node is the node that owns the namespace Volume
	Node blobcache.NodeID
	// NS is the handle to the namespace Volume, that the transaction is for
	NS blobcache.Handle
	// Tx is a transaction for maniuplating the namespace
	Tx *Tx
	// Name is the remaining name within the namespace
	Name string
}

// DoAt resolves fqp opens a transaction and
func DoAt(ctx context.Context, bc blobcache.Service, fqp FQP, fn func(c DoAtCtx) error) error {

	return nil
}

// CreateVolumeAt creates a volume according to spec after looking up p.
func CreateVolumeAt(ctx context.Context, bc blobcache.Service, fqp FQP, spec blobcache.VolumeSpec) (blobcache.Handle, error) {
	var ret blobcache.Handle
	err := DoAt(ctx, bc, fqp, func(c DoAtCtx) error {
		var ent Entry
		exists, err := c.Tx.Get(ctx, c.Name, &ent)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("cannot create, there is already an object there %s => %v", ent.Name, ent.Target)
		}
		subvolh, _, err := bcsdk.CreateOnSameHost(ctx, bc, c.NS, spec)
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

// ClientForVolume returns a Client configured to use the Namespace schema for the Volume
// If the Volume does not have a known Schema or the Schema is not Namespace then an error is returned.
func ClientForVolume(ctx context.Context, svc blobcache.Service, nsvolh blobcache.Handle) (*Client, error) {
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
	return &Client{
		Service: svc,
		Schema:  nssch,
	}, nil
}
