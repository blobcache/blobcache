package bclocal

import (
	"context"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
)

var _ blobcache.Service = (*PeerView)(nil)

type PeerView struct {
	*Service
	Peer blobcache.PeerID
}

func (s *Service) Access(peer blobcache.PeerID) blobcache.Service {
	return &PeerView{
		Service: s,
		Peer:    peer,
	}
}

func (pv *PeerView) Open(ctx context.Context, base blobcache.Handle, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return nil, ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "Open",
			Target: x,
		}
	}
	return pv.Service.Open(ctx, base, x, blobcache.Action_ALL)
}

type ACL struct {
	// For now just blanket allow or don't.
	Owners []blobcache.PeerID
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
