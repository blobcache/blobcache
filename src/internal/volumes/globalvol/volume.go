package globalvol

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
)

var _ volumes.Volume = &Volume{}

// Volume manages state for a volume.
type Volume struct {
	id       blobcache.CID
	spec     blobcache.VolumeBackend_Global
	settled  volumes.Volume
	incoming chan *blobcache.Message
	sm       ConsensusSM

	mu      sync.Mutex
	pending map[blobcache.PeerID]volumes.Tx
}

func newVolume(spec blobcache.VolumeBackend_Global, settled volumes.Volume, incoming chan *blobcache.Message) *Volume {
	return &Volume{
		id:       spec.Essence.ID(),
		spec:     spec,
		settled:  settled,
		incoming: incoming,

		pending: make(map[blobcache.PeerID]volumes.Tx),
	}
}

func (v *Volume) Handle(out *[]blobcache.Message, msg blobcache.Message) error {
	return nil
}

// BeginTx implements volumes.Volume.
func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx, error) {
	if !spec.Modify {
		tx, err := v.settled.BeginTx(ctx, spec)
		if err != nil {
			return nil, err
		}
		return TxRO{Tx: tx}, nil
	}
	return nil, fmt.Errorf("")
}

func (v *Volume) AccessSubVolume(ctx context.Context, target blobcache.OID) (blobcache.ActionSet, error) {
	return 0, fmt.Errorf("consensus volumes cannot have links")
}

func (v *Volume) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	return blobcache.VolumeBackend[blobcache.OID]{
		Global: &v.spec,
	}
}

func (v *Volume) GetParams() blobcache.VolumeConfig {
	return v.GetBackend().Global.Config()
}

var _ volumes.Tx = TxRO{}

type TxRO struct {
	volumes.Tx
}

func (TxRO) Link(ctx context.Context, target blobcache.OID, rights blobcache.ActionSet, subvol volumes.Volume) error {
	return fmt.Errorf("global volumes do not support linking")
}

type Tx struct {
	vol    *Volume
	params blobcache.TxParams
	inner  volumes.Tx
}
