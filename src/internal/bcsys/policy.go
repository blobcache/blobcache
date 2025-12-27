package bcsys

import (
	"slices"

	"blobcache.io/blobcache/src/blobcache"
)

type Policy interface {
	OpenFiat(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet
	// CanCreate returns true if the peer can create a new volume.
	CanCreate(peer blobcache.PeerID) bool
	CanConnect(peer blobcache.PeerID) bool
}

// AllOrNothingPolicy is a policy that allows or disallows all actions for all peers.
type AllOrNothingPolicy struct {
	Allow []blobcache.PeerID
}

func (p *AllOrNothingPolicy) OpenFiat(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	if !slices.Contains(p.Allow, peer) {
		return 0
	}
	return blobcache.Action_ALL
}

func (p *AllOrNothingPolicy) CanConnect(peer blobcache.PeerID) bool {
	return slices.Contains(p.Allow, peer)
}

func (p *AllOrNothingPolicy) CanCreate(peer blobcache.PeerID) bool {
	return slices.Contains(p.Allow, peer)
}
