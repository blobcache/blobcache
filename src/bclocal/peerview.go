package bclocal

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

var _ blobcache.Service = &peerView{}

type peerView struct {
	*Service
	Caller blobcache.PeerID
}

func (pv *peerView) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	pol := pv.Service.env.Policy
	if !pol.CanCreate(pv.Caller) {
		return nil, ErrNotAllowed{
			Peer:   pv.Caller,
			Action: "CreateVolume",
		}
	}
	if host != nil {
		return nil, fmt.Errorf("peers cannot ask us to create volumes on remote nodes")
	}
	return pv.Service.CreateVolume(ctx, nil, vspec)
}

func (pv *peerView) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	pol := pv.Service.env.Policy
	if rights := pol.OpenFiat(pv.Caller, x); rights == 0 {
		return nil, ErrNotAllowed{
			Peer:   pv.Caller,
			Action: "OpenFiat",
			Target: x,
		}
	} else {
		return pv.Service.OpenFiat(ctx, x, rights&mask)
	}
}
