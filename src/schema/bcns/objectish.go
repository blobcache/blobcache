package bcns

import (
	"context"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
)

// Objectish is information capable of producing a Handle to an Object.
// Base is used to establish a connection to the local node.
// If the Secret is empty, then OpenFiat will be used to establish a connection
// The zero value for Objectish refers to the root Volume.
type Objectish struct {
	Base blobcache.Handle
	Path []string
}

func ParseObjectish(x string) (Objectish, error) {
	parts := strings.Split(x, ";")
	var baseH blobcache.Handle
	if parts[0] != "" {
		h, err := blobcache.ParseHandle(parts[0])
		if err != nil {
			oid, err := blobcache.ParseOID(parts[0])
			if err != nil {
				return Objectish{}, err
			}
			baseH = blobcache.Handle{OID: oid}
		} else {
			baseH = h
		}
	}
	return Objectish{
		Base: baseH,
		Path: parts[1:],
	}, nil
}

func (oish *Objectish) String() string {
	sb := strings.Builder{}
	if oish.BaseIsHandle() {
		fmt.Fprint(&sb, oish.Base)
	} else {
		fmt.Fprint(&sb, oish.Base.OID)
	}
	if len(oish.Path) > 0 {
		sb.WriteByte(';')
		sb.WriteString(strings.Join(oish.Path, ";"))
	}
	return sb.String()
}

// BaseIsHandle returns true if Base is a handle instead of an OID
func (oish *Objectish) BaseIsHandle() bool {
	return oish.Base.Secret != ([16]byte{})
}

func (oish *Objectish) BaseHandle() *blobcache.Handle {
	if oish.BaseIsHandle() {
		return &oish.Base
	}
	return nil
}

func (oish *Objectish) BaseOID() blobcache.OID {
	return oish.Base.OID
}

func (oish *Objectish) Open(ctx context.Context, bc blobcache.Service) (*blobcache.Handle, error) {
	var nsh blobcache.Handle
	if !oish.BaseIsHandle() {
		h, err := bc.OpenFiat(ctx, oish.Base.OID, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		nsh = *h
	} else {
		nsh = oish.Base
	}

	for _, name := range oish.Path {
		nsc, err := ClientForVolume(ctx, bc, nsh)
		if err != nil {
			return nil, fmt.Errorf("getting NSClient for %v: %w", nsh.OID, err)
		}
		subvolh, err := nsc.OpenAt(ctx, nsh, name, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		nsh = *subvolh
	}
	return &nsh, nil
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
