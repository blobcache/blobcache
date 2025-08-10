package bclocal

import (
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
)

type ACL struct {
	// For now just blanket allow or don't.
	Owners []blobcache.PeerID
}

// Mentions returns true if the peer is anywhere in the ACL.
func (acl *ACL) Mentions(peer blobcache.PeerID) bool {
	return slices.Contains(acl.Owners, peer)
}

func (acl *ACL) CanLook(peer blobcache.PeerID, name string) bool {
	return slices.Contains(acl.Owners, peer)
}

func (acl *ACL) CanTouch(peer blobcache.PeerID, name string) bool {
	return slices.Contains(acl.Owners, peer)
}

func (acl *ACL) CanCreate(peer blobcache.PeerID, name string) bool {
	return slices.Contains(acl.Owners, peer)
}

func (acl *ACL) CanDelete(peer blobcache.PeerID, name string) bool {
	return slices.Contains(acl.Owners, peer)
}

type ErrNotAllowed struct {
	Peer   blobcache.PeerID
	Action string
	Target blobcache.OID
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("access denied: %s %s %s", e.Peer, e.Action, e.Target)
}
