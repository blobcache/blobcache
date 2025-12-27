package bcsys

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type peerView[LK any, LV LocalVolume[LK]] struct {
	*Service[LK, LV]
	Caller blobcache.PeerID
}

func (pv *peerView[LK, LV]) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
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

func (pv *peerView[LK, LV]) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
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
