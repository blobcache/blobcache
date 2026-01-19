package bcsdk

import (
	"context"
	"errors"
	"fmt"

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
	localEP, err := bc.Endpoint(ctx)
	if err != nil {
		return nil, err
	}
	if localEP.Peer == u.Node {
		volh, err := bc.OpenFiat(ctx, u.Base, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		return volh, nil
	} else {
		// The Volume is not on the local peer.
		// check if the URL has an IP and port.
		if ep := u.Endpoint(); ep != nil {
			return bc.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Remote: &blobcache.VolumeBackend_Remote{
					Endpoint: *ep,
					Volume:   u.Base,
					HashAlgo: "",
				},
			})
		} else {
			// there was no IP and port, so we create a Peer Volume instead.
			return bc.CreateVolume(ctx, nil, blobcache.VolumeSpec{
				Peer: &blobcache.VolumeBackend_Peer{
					Peer:     u.Node,
					Volume:   u.Base,
					HashAlgo: "",
				},
			})
		}
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

// GetBytes gets a blob, but gracefully handles ErrTooSmall by resizing the buffer and retrying.
func GetBytes(ctx context.Context, bc blobcache.Service, txh blobcache.Handle, cid blobcache.CID, hardMax int) ([]byte, error) {
	buf := make([]byte, hardMax/2)
	n, err := bc.Get(ctx, txh, cid, buf, blobcache.GetOpts{})
	if err != nil {
		var tse blobcache.ErrTooSmall
		if errors.As(err, &tse) {
			if tse.BlobSize > int32(hardMax) {
				return nil, fmt.Errorf("blob size %d exceeds hard max %d", tse.BlobSize, hardMax)
			}
			buf = append(buf, make([]byte, 100)...)
			if n, err = bc.Get(ctx, txh, cid, buf, blobcache.GetOpts{}); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return buf[:n], nil
}
