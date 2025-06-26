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

func (pv *PeerView) Open(ctx context.Context, x blobcache.OID) (*blobcache.Handle, error) {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return nil, ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "Open",
			Target: x,
		}
	}
	return pv.Service.Open(ctx, x)
}

func (pv *PeerView) OpenAt(ctx context.Context, namespace blobcache.Handle, name string) (*blobcache.Handle, error) {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return nil, ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "Open",
			Target: namespace.OID,
		}
	}
	return pv.Service.OpenAt(ctx, namespace, name)
}

func (pv *PeerView) PutEntry(ctx context.Context, namespace blobcache.Handle, name string, value blobcache.Handle) error {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "PutEntry",
			Target: namespace.OID,
		}
	}
	return pv.Service.PutEntry(ctx, namespace, name, value)
}

func (pv *PeerView) GetEntry(ctx context.Context, namespace blobcache.Handle, name string) (*blobcache.Entry, error) {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return nil, ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "GetEntry",
			Target: namespace.OID,
		}
	}
	return pv.Service.GetEntry(ctx, namespace, name)
}

func (pv *PeerView) DeleteEntry(ctx context.Context, namespace blobcache.Handle, name string) error {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "DeleteEntry",
			Target: namespace.OID,
		}
	}
	return pv.Service.DeleteEntry(ctx, namespace, name)
}

func (pv *PeerView) ListNames(ctx context.Context, namespace blobcache.Handle) ([]string, error) {
	if !slices.Contains(pv.env.ACL.Owners, pv.Peer) {
		return nil, ErrNotAllowed{
			Peer:   pv.Peer,
			Action: "ListNames",
			Target: namespace.OID,
		}
	}
	return pv.Service.ListNames(ctx, namespace)
}

func (pv *PeerView) CreateVolume(ctx context.Context, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	return nil, fmt.Errorf("this host does not allow remote peers to create volumes")
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
