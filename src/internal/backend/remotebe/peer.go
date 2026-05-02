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
	const dialTimeout = 3 * time.Second
	return withEndpoint(ctx, ps.locator, p.Peer, func(ep blobcache.Endpoint) (*Volume, error) {
		dialCtx, cf := context.WithTimeout(ctx, dialTimeout)
		defer cf()
		vol, err := ps.inner.VolumeUp(dialCtx, Params{
			Endpoint: ep,
			Volume:   p.Volume,
			HashAlgo: p.HashAlgo,
		})
		return vol, err
	})
}

func (ps *PeerSystem) VolumeDestroy(ctx context.Context, vol *Volume) error {
	return ps.inner.VolumeDestroy(ctx, vol)
}

// Create creates a new volume on the remote
func (ps *PeerSystem) CreateVolume(ctx context.Context, peer blobcache.NodeID, vspec blobcache.VolumeSpec) (*Volume, error) {
	return withEndpoint(ctx, ps.locator, peer, func(ep blobcache.Endpoint) (*Volume, error) {
		return ps.inner.CreateVolume(ctx, ep, vspec)
	})
}

func withEndpoint[T any](ctx context.Context, loc PeerLocator, peer blobcache.NodeID, fn func(blobcache.Endpoint) (T, error)) (T, error) {
	var ret T
	if loc == nil {
		return ret, fmt.Errorf("bcpeer: no PeerLocator configured")
	}
	const dialTimeout = 3 * time.Second
	var lastErr error
	for addr := range loc.WhereIs(ctx, peer) {
		ep := blobcache.Endpoint{
			Node:   peer,
			IPPort: addr,
		}
		val, err := fn(ep)
		if err == nil {
			logctx.Info(ctx, "found peer", zap.Stringer("peer", peer), zap.Stringer("ip_port", addr))
			return val, nil
		}
		logctx.Warn(ctx, "error looking for peer", zap.Stringer("peer", peer), zap.Error(err))
		lastErr = err
	}
	if lastErr != nil {
		return ret, fmt.Errorf("bcpeer: could not reach peer %s: %w", peer, lastErr)
	}
	return ret, fmt.Errorf("bcpeer: could not reach peer %s", peer)
}
