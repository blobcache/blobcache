package bccore

import (
	"context"
	"fmt"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var _ blobcache.VolumeAPI = &System{}

func (sys *System) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "InspectVolume"), zap.Stringer("oid", h.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "InspectVolume"), zap.Stringer("oid", h.OID))
	vol, _, err := sys.resolveVol(h)
	if err != nil {
		return nil, err
	}
	info := vol.info
	return &info, nil
}

func (sys *System) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	logctx.Info(ctx, "begin", zap.String("method", "OpenFrom"), zap.Stringer("oid", base.OID))
	defer logctx.Info(ctx, "done", zap.String("method", "OpenFrom"), zap.Stringer("oid", base.OID))
	baseVol, _, err := sys.resolveVol(base)
	if err != nil {
		return nil, err
	}
	rights, err := baseVol.backend.AccessSubVolume(ctx, ltok)
	if err != nil {
		return nil, err
	}
	if rights == 0 {
		return nil, blobcache.ErrNoLink{Base: base.OID, Target: ltok.Target}
	}

	_, err, _ = sys.setup.Do(ltok.Target, func() (AnyObject, error) {
		ures, err := sys.p.Up(ctx, ltok.Target)
		if err != nil {
			return AnyObject{}, err
		}
		switch {
		case ures.Volume != nil:
			if added := sys.addVolume(ltok.Target, ures.Volume); !added {
				return AnyObject{}, ures.Volume.VolumeDown(ctx)
			}
		case ures.Queue != nil:
			if added := sys.addQueue(ltok.Target, ures.Queue); !added {
				return AnyObject{}, ures.Queue.QueueDown(ctx)
			}
		default:
			return AnyObject{}, fmt.Errorf("volume=%v has link to subvolume=%v, but that subvolume was not found", base.OID, ltok.Target)
		}
		return ures, nil
	})
	if err != nil {
		return nil, err
	}

	rights = rights & mask
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultTxTTL)
	h := sys.handles.Create(ltok.Target, rights, createdAt, expiresAt)
	return &h, nil
}

func (sys *System) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "BeginTx"), zap.Stringer("oid", volh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "BeginTx"), zap.Stringer("oid", volh.OID))
	if err := txspec.Validate(); err != nil {
		return nil, err
	}
	vol, rights, err := sys.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	if !rights.Has(blobcache.Action_VOLUME_BEGIN_TX) {
		return nil, blobcache.ErrPermission{
			Handle:   volh,
			Rights:   rights,
			Requires: blobcache.Action_VOLUME_BEGIN_TX,
		}
	}
	if txspec.GCBlobs {
		gcBlobRights := blobcache.ActionSet(blobcache.Action_VOLUME_TX_VISIT | blobcache.Action_VOLUME_TX_IS_VISITED)
		if rights&gcBlobRights == 0 {
			return nil, blobcache.ErrPermission{
				Handle:   volh,
				Rights:   rights,
				Requires: gcBlobRights,
			}
		}
	}
	if txspec.Modify {
		mutatingRights := blobcache.ActionSet(blobcache.Action_VOLUME_TX_SAVE |
			blobcache.Action_VOLUME_TX_POST |
			blobcache.Action_VOLUME_TX_DELETE |
			blobcache.Action_VOLUME_TX_COPY_TO |
			blobcache.Action_VOLUME_TX_LINK_FROM |
			blobcache.Action_VOLUME_TX_UNLINK_FROM |
			blobcache.Action_VOLUME_TX_VISIT |
			blobcache.Action_VOLUME_TX_VISIT_LINKS)
		if rights&mutatingRights == 0 {
			return nil, blobcache.ErrPermission{
				Handle:   volh,
				Rights:   rights,
				Requires: mutatingRights,
			}
		}
	}
	tx, err := vol.backend.BeginTx(ctx, txspec)
	if err != nil {
		return nil, err
	}

	txoid := blobcache.RandomOID()
	sys.mu.Lock()
	if sys.txns == nil {
		sys.txns = make(map[blobcache.OID]transaction)
	}
	sys.txns[txoid] = transaction{
		backend: tx,
		volume:  &vol,
	}
	sys.mu.Unlock()
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultTxTTL)
	txRights := blobcache.Action_ACK | blobcache.Action_TX_INSPECT
	volTxRightsMask := blobcache.Action_VOLUME_TX_INSPECT |
		blobcache.Action_VOLUME_TX_LOAD |
		blobcache.Action_VOLUME_TX_SAVE |
		blobcache.Action_VOLUME_TX_POST |
		blobcache.Action_VOLUME_TX_GET |
		blobcache.Action_VOLUME_TX_EXISTS |
		blobcache.Action_VOLUME_TX_DELETE |
		blobcache.Action_VOLUME_TX_COPY_FROM |
		blobcache.Action_VOLUME_TX_COPY_TO |
		blobcache.Action_VOLUME_TX_LINK_FROM |
		blobcache.Action_VOLUME_TX_UNLINK_FROM |
		blobcache.Action_VOLUME_TX_VISIT |
		blobcache.Action_VOLUME_TX_IS_VISITED |
		blobcache.Action_VOLUME_TX_VISIT_LINKS
	txRights |= (rights & volTxRightsMask) >> 8
	txRights |= (rights & blobcache.SharedAction(volTxRightsMask)) >> 8
	h := sys.handles.Create(txoid, txRights, createdAt, expiresAt)
	return &h, nil
}
