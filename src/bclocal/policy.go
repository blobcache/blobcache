package bclocal

import (
	"fmt"
	"iter"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

type Policy interface {
	// GrantsFor lookups up all the grants that have been made to a given peer.
	GrantsFor(x blobcache.PeerID) iter.Seq[schema.Link]
	Open(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet
	// CanCreate returns true if the peer can create a new volume.
	CanCreate(peer blobcache.PeerID) bool
}

func policyMentions(policy Policy, peer blobcache.PeerID) bool {
	for range policy.GrantsFor(peer) {
		return true
	}
	return false
}

type ErrNotAllowed struct {
	Peer   blobcache.PeerID
	Action string
	Target blobcache.OID
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("access denied: %s %s %s", e.Peer, e.Action, e.Target)
}

// AllOrNothingPolicy is a policy that allows or disallows all actions for all peers.
type AllOrNothingPolicy struct {
	Allow []blobcache.PeerID
}

func (p *AllOrNothingPolicy) GrantsFor(peer blobcache.PeerID) iter.Seq[schema.Link] {
	if !slices.Contains(p.Allow, peer) {
		return func(yield func(schema.Link) bool) {}
	}
	return func(yield func(schema.Link) bool) {
		yield(schema.Link{
			Target: blobcache.OID{},
			Rights: blobcache.Action_ALL,
		})
	}
}

func (p *AllOrNothingPolicy) Open(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	if !slices.Contains(p.Allow, peer) {
		return 0
	}
	return blobcache.Action_ALL
}

func (p *AllOrNothingPolicy) CanCreate(peer blobcache.PeerID) bool {
	return slices.Contains(p.Allow, peer)
}
