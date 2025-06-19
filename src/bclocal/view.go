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

func (pv *PeerView) Open(ctx context.Context, namespace blobcache.Handle, name string) (*blobcache.Handle, error) {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return nil, fmt.Errorf("access denied")
	}
	return pv.Service.Open(ctx, namespace, name)
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
