package bclocal

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

var _ blobcache.Service = (*View)(nil)

type View struct {
	*Service
	Peer blobcache.PeerID
}

func (s *Service) Access(peer blobcache.PeerID) blobcache.Service {
	return &View{
		Service: s,
		Peer:    peer,
	}
}

func (v *View) Open(ctx context.Context, namespace blobcache.Handle, name string) (*blobcache.Handle, error) {
	return v.Service.Open(ctx, namespace, name)
}
