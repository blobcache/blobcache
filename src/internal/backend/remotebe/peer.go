package remotebe

import (
	"context"
	"fmt"
	"iter"
	"net/netip"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var _ backend.VolumeSystem[PeerParams, *Volume] = &PeerSystem{}

type PeerParams = blobcache.VolumeBackend_Peer

// PeerLocator finds the address of peers.
type PeerLocator interface {
	WhereIs(ctx context.Context, peer blobcache.NodeID) iter.Seq[netip.AddrPort]
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
	const dialTimeout = 3 * time.Second
	var lastErr error
	for addr := range ps.locator.WhereIs(ctx, p.Peer) {
		ep := blobcache.Endpoint{
			Node:   p.Peer,
			IPPort: addr,
		}
		dialCtx, cf := context.WithTimeout(ctx, dialTimeout)
		defer cf()
		vol, err := ps.inner.VolumeUp(dialCtx, Params{
			Endpoint: ep,
			Volume:   p.Volume,
			HashAlgo: p.HashAlgo,
		})
		cf()
		if err == nil {
			logctx.Info(ctx, "found peer", zap.Stringer("peer", p.Peer))
			return vol, nil
		} else {
			logctx.Warn(ctx, "error looking for peer", zap.Stringer("peer", p.Peer), zap.Error(err))
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("bcpeer: could not reach peer %s: %w", p.Peer, lastErr)
	}
	return nil, fmt.Errorf("bcpeer: could not reach peer %s", p.Peer)
}

func (ps *PeerSystem) VolumeDestroy(ctx context.Context, vol *Volume) error {
	return ps.inner.VolumeDestroy(ctx, vol)
}
