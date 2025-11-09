package bcsdk

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

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
