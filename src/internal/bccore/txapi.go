package bccore

import (
	"context"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// This next section implements blobcache.TxAPI
var _ blobcache.TxAPI = &System{}

func (sys *System) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "InspectTx"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "InspectTx"), zap.Stringer("oid", txh.OID))
	tx, err := sys.resolveTx(txh, true, blobcache.Action_TX_INSPECT)
	if err != nil {
		return nil, err
	}
	params := tx.backend.Params()
	return &blobcache.TxInfo{
		ID:       txh.OID,
		Volume:   tx.volume.info.ID,
		MaxSize:  int64(tx.backend.MaxSize()),
		HashAlgo: tx.backend.HashAlgo(),
		Params:   params,
	}, nil
}

func (sys *System) Commit(ctx context.Context, txh blobcache.Handle) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Commit"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Commit"), zap.Stringer("oid", txh.OID))
	tx, err := sys.resolveTx(txh, true, 0)
	if err != nil {
		return err
	}
	if p := tx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "COMMIT"}
	}
	if err := tx.backend.Commit(ctx); err != nil {
		return setErrTxOID(err, txh.OID)
	}
	sys.mu.Lock()
	sys.handles.Drop(txh)
	sys.mu.Unlock()
	sys.hub.Publish(ctx, tx.volume.info.ID, tx.volume)
	return nil
}

func (sys *System) Abort(ctx context.Context, txh blobcache.Handle) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Abort"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Abort"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, false, 0)
	if err != nil {
		return err
	}
	if err := txn.backend.Abort(ctx); err != nil {
		return err
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	sys.handles.Drop(txh)
	return nil
}

func (sys *System) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Load"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Load"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_LOAD)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Load(ctx, dst), txh.OID)
}

func (sys *System) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Save"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Save"), zap.Stringer("oid", txh.OID))
	tx, err := sys.resolveTx(txh, true, blobcache.Action_TX_SAVE)
	if err != nil {
		return err
	}
	if p := tx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "SAVE"}
	}
	if sys.p.OnSave != nil {
		if err := sys.p.OnSave(ctx, tx.volume.backend, tx.backend, root); err != nil {
			return err
		}
	}
	return setErrTxOID(tx.backend.Save(ctx, root), txh.OID)
}

func (sys *System) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Post"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Post"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_POST)
	if err != nil {
		return blobcache.CID{}, err
	}
	if p := txn.backend.Params(); !p.Modify {
		return blobcache.CID{}, blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "POST"}
	}
	cid, err := txn.backend.Post(ctx, data, opts)
	if err != nil {
		return blobcache.CID{}, setErrTxOID(err, txh.OID)
	}
	return cid, nil
}

func (sys *System) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Exists"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Exists"), zap.Stringer("oid", txh.OID))
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_EXISTS)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Exists(ctx, cids, dst), txh.OID)
}

func (sys *System) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Get"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Get"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_GET)
	if err != nil {
		return 0, err
	}
	n, err := txn.backend.Get(ctx, cid, buf, opts)
	if err != nil {
		return 0, setErrTxOID(err, txh.OID)
	}
	if !opts.SkipVerify {
		cid2 := txn.backend.HashAlgo().KeyedHash(opts.Salt, buf[:n])
		if cid2 != cid {
			return -1, blobcache.ErrBadData{
				Salt:     opts.Salt,
				Expected: cid,
				Actual:   cid2,
				Len:      n,
			}
		}
	}
	return n, nil
}

func (sys *System) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Delete"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Delete"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_DELETE)
	if err != nil {
		return err
	}
	if p := txn.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "DELETE"}
	}
	return setErrTxOID(txn.backend.Delete(ctx, cids), txh.OID)
}

func (sys *System) Copy(ctx context.Context, txh blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, out []bool) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Copy"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Copy"), zap.Stringer("oid", txh.OID))
	if len(cids) != len(out) {
		return fmt.Errorf("cids and out must have the same length")
	}
	dstTx, err := sys.resolveTx(txh, true, blobcache.Action_TX_COPY_TO)
	if err != nil {
		return err
	}
	if p := dstTx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "COPY"}
	}

	type srcTxn struct {
		oid blobcache.OID
		tx  transaction
	}
	resolvedSrcs := make([]srcTxn, len(srcTxns))
	for i, srcH := range srcTxns {
		src, err := sys.resolveTx(srcH, true, blobcache.Action_TX_COPY_FROM)
		if err != nil {
			return err
		}
		resolvedSrcs[i] = srcTxn{oid: srcH.OID, tx: src}
	}

	for i := range cids {
		out[i] = false
	}

	var buf []byte
	var exists [1]bool
	for i, cid := range cids {
		if len(resolvedSrcs) == 0 {
			continue
		}
		start := int(cid[0]) % len(resolvedSrcs)
		for j := range resolvedSrcs {
			src := resolvedSrcs[(start+j)%len(resolvedSrcs)]
			exists[0] = false
			if err := src.tx.backend.Exists(ctx, []blobcache.CID{cid}, exists[:]); err != nil {
				return fmt.Errorf("copy from tx %v: %w", src.oid, err)
			}
			if !exists[0] {
				continue
			}
			srcMax := src.tx.backend.MaxSize()
			if cap(buf) < srcMax {
				buf = make([]byte, srcMax)
			}
			n, err := src.tx.backend.Get(ctx, cid, buf[:srcMax], blobcache.GetOpts{})
			if err != nil {
				if blobcache.IsErrNotFound(err) {
					continue
				}
				return fmt.Errorf("copy from tx %v: %w", src.oid, err)
			}
			data := buf[:n]
			if dstTx.backend.HashAlgo().Hash(data) != cid {
				continue
			}
			cid2, err := dstTx.backend.Post(ctx, data, blobcache.PostOpts{})
			if err != nil {
				var eTooLarge blobcache.ErrTooLarge
				if errors.As(err, &eTooLarge) {
					continue
				}
				return setErrTxOID(err, txh.OID)
			}
			if cid2 != cid {
				continue
			}
			out[i] = true
			break
		}
	}
	return nil
}

func (sys *System) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Visit"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Visit"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_VISIT)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Visit(ctx, cids), txh.OID)
}

func (sys *System) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	logctx.Debug(ctx, "begin", zap.String("method", "IsVisited"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "IsVisited"), zap.Stringer("oid", txh.OID))
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and out must have the same length")
	}
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_IS_VISITED)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.IsVisited(ctx, cids, dst), txh.OID)
}

func (sys *System) Link(ctx context.Context, txh blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Link"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Link"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_LINK_FROM)
	if err != nil {
		return nil, err
	}
	volTo, rights, err := sys.resolveVol(target)
	if err != nil {
		return nil, err
	}
	if err := sys.p.OnLink(ctx, blobcache.Info{Volume: &volTo.info}, AnyObject{Volume: volTo.backend}); err != nil {
		return nil, err
	}
	linkRights := rights.Share() & mask
	ltok, err := txn.backend.Link(ctx, volTo.info.ID, linkRights, volTo.backend)
	if err != nil {
		return nil, setErrTxOID(err, txh.OID)
	}
	return ltok, nil
}

func (sys *System) Unlink(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkTokenID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Unlink"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Unlink"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_UNLINK_FROM)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Unlink(ctx, targets), txh.OID)
}

func (sys *System) VisitLinks(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkTokenID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "VisitLinks"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "VisitLinks"), zap.Stringer("oid", txh.OID))
	txn, err := sys.resolveTx(txh, true, blobcache.Action_TX_VISIT_LINKS)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.VisitLinks(ctx, targets), txh.OID)
}

func (s *System) AbortAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for oid, txn := range s.txns {
		if err := txn.backend.Abort(ctx); err != nil {
			logctx.Warn(ctx, "aborting transaction", zap.Error(err))
		}
		s.handles.DropAllForOID(oid)
		delete(s.txns, oid)
	}
	return nil
}

func setErrTxOID(err error, oid blobcache.OID) error {
	switch e := err.(type) {
	case blobcache.ErrTxDone:
		e.ID = oid
		return e
	case blobcache.ErrTxReadOnly:
		e.Tx = oid
		return e
	default:
		return err
	}
}
