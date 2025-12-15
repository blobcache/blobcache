package bcsdk

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type Loader interface {
	Load(ctx context.Context, dst *[]byte) error
}

type Saver interface {
	Save(ctx context.Context, src []byte) error
}

// CreateOnSameHost creates a new subvolume on the same host as the base volume.
func CreateOnSameHost(ctx context.Context, s blobcache.Service, base blobcache.Handle, spec blobcache.VolumeSpec) (*blobcache.Handle, *blobcache.FQOID, error) {
	info, err := s.InspectVolume(ctx, base)
	if err != nil {
		return nil, nil, err
	}
	var host *blobcache.Endpoint
	if info.Backend.Remote != nil {
		host = &info.Backend.Remote.Endpoint
	}
	svolh, err := s.CreateVolume(ctx, host, spec)
	if err != nil {
		return nil, nil, err
	}
	if host != nil {
		svinfo, err := s.InspectVolume(ctx, *svolh)
		if err != nil {
			return nil, nil, err
		}
		fqoid := svinfo.GetRemoteFQOID()
		return svolh, &fqoid, err
	} else {
		ep, err := s.Endpoint(ctx)
		if err != nil {
			return nil, nil, err
		}
		return svolh, &blobcache.FQOID{
			Peer: ep.Peer,
			OID:  svolh.OID,
		}, nil
	}
}

func OpenURL(ctx context.Context, bc blobcache.Service, u blobcache.URL) (*blobcache.Handle, error) {
	ep, err := bc.Endpoint(ctx)
	if err != nil {
		return nil, err
	}
	if ep.Peer == u.Node {
		volh, err := bc.OpenFiat(ctx, u.Base, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		return volh, nil
	} else {
		return bc.CreateVolume(ctx, nil, blobcache.VolumeSpec{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: ep,
				Volume:   u.Base,
				HashAlgo: "", // TODO,
			},
		})
	}
}

// URLFor produces a URL for a Volume.
// If the Volume is a remote Volume than the OID of the Volume on the remote Node
// and the Endpoint of the remote Node will be included in the URL instead of the local Node.
func URLFor(ctx context.Context, bc blobcache.Service, volh blobcache.Handle) (*blobcache.URL, error) {
	vinfo, err := bc.InspectVolume(ctx, volh)
	if err != nil {
		return nil, err
	}
	host, err := bc.Endpoint(ctx)
	if err != nil {
		return nil, err
	}
	voloid := volh.OID
	switch {
	case vinfo.Backend.Remote != nil:
		host = vinfo.Backend.Remote.Endpoint
		voloid = vinfo.Backend.Remote.Volume
	}
	return &blobcache.URL{
		Node: host.Peer,
		Base: voloid,
	}, nil
}
