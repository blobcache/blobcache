package remotebe

import (
	"context"
	"fmt"
	"iter"
	"net/netip"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
)

var _ backend.VolumeSystem[PeerParams, *Volume] = &PeerSystem{}

type PeerParams = blobcache.VolumeBackend_Peer

// PeerLocator finds the address of peers.
type PeerLocator interface {
	WhereIs(ctx context.Context, peer blobcache.PeerID) iter.Seq[netip.AddrPort]
}

type PeerSystem struct {
	inner   *System
	locator PeerLocator
}

func NewPeerSystem(inner *System, locator PeerLocator) PeerSystem {
	return PeerSystem{
		inner:   inner,
		locator: locator,
	}
}

func (ps *PeerSystem) VolumeUp(ctx context.Context, p PeerParams) (*Volume, error) {
	if ps.locator == nil {
		return nil, fmt.Errorf("bcpeer: no PeerLocator configured")
	}
	for addr := range ps.locator.WhereIs(ctx, p.Peer) {
		ep := blobcache.Endpoint{
			Peer:   p.Peer,
			IPPort: addr,
		}
		vol, err := ps.inner.VolumeUp(ctx, Params{
			Endpoint: ep,
			Volume:   p.Volume,
			HashAlgo: p.HashAlgo,
		})
		if err == nil {
			return vol, nil
		}
	}
	return nil, fmt.Errorf("bcpeer: could not reach peer %s", p.Peer)
}

func (ps *PeerSystem) VolumeDestroy(ctx context.Context, vol *Volume) error {
	return ps.inner.VolumeDestroy(ctx, vol)
}
