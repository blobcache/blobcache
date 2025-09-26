package bcnet

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

// AccessFun is called to get a service to access
type AccessFunc func(blobcache.PeerID) blobcache.Service

type Server struct {
	Access AccessFunc
}

func (s *Server) serve(ctx context.Context, ep blobcache.Endpoint, req *Message, resp *Message) {
	svc := s.Access(ep.Peer)
	if svc == nil {
		resp.SetError(fmt.Errorf("not allowed"))
		return
	}

	switch req.Header().Code() {
	case MT_PING:
		resp.SetCode(MT_OK)
		resp.SetBody(nil)

	// BEGIN HANDLE
	case MT_HANDLE_DROP:
		handleAsk(req, resp, &DropReq{}, func(req *DropReq) (*DropResp, error) {
			if err := svc.Drop(ctx, req.Handle); err != nil {
				return nil, err
			}
			return &DropResp{}, nil
		})
	case MT_HANDLE_INSPECT:
		handleAsk(req, resp, &InspectHandleReq{}, func(req *InspectHandleReq) (*InspectHandleResp, error) {
			info, err := svc.InspectHandle(ctx, req.Handle)
			if err != nil {
				return nil, err
			}
			return &InspectHandleResp{Info: *info}, nil
		})
	case MT_HANDLE_KEEP_ALIVE:
		handleAsk(req, resp, &KeepAliveReq{}, func(req *KeepAliveReq) (*KeepAliveResp, error) {
			if err := svc.KeepAlive(ctx, req.Handles); err != nil {
				return nil, err
			}
			return &KeepAliveResp{}, nil
		})
	case MT_HANDLE_SHARE:
		handleAsk(req, resp, &ShareReq{}, func(req *ShareReq) (*ShareResp, error) {
			h, err := svc.Share(ctx, req.Handle, req.Peer, req.Mask)
			if err != nil {
				return nil, err
			}
			return &ShareResp{Handle: *h}, nil
		})
	// END HANDLE

	// BEGIN VOLUME
	case MT_OPEN_AS:
		handleAsk(req, resp, &OpenFiatReq{}, func(req *OpenFiatReq) (*OpenFiatResp, error) {
			h, err := svc.OpenFiat(ctx, req.Target, req.Mask)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &OpenFiatResp{Handle: *h, Info: *info}, nil
		})
	case MT_OPEN_FROM:
		handleAsk(req, resp, &OpenFromReq{}, func(req *OpenFromReq) (*OpenFromResp, error) {
			h, err := svc.OpenFrom(ctx, req.Base, req.Target, req.Mask)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &OpenFromResp{Handle: *h, Info: *info}, nil
		})
	case MT_CREATE_VOLUME:
		handleAsk(req, resp, &CreateVolumeReq{}, func(req *CreateVolumeReq) (*CreateVolumeResp, error) {
			h, err := svc.CreateVolume(ctx, nil, req.Spec)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &CreateVolumeResp{Handle: *h, Info: *info}, nil
		})
	case MT_VOLUME_CLONE:
		handleAsk(req, resp, &CloneVolumeReq{}, func(req *CloneVolumeReq) (*CloneVolumeResp, error) {
			h, err := svc.CloneVolume(ctx, &ep.Peer, req.Volume)
			if err != nil {
				return nil, err
			}
			return &CloneVolumeResp{Handle: *h}, nil
		})
	case MT_VOLUME_INSPECT:
		handleAsk(req, resp, &InspectVolumeReq{}, func(req *InspectVolumeReq) (*InspectVolumeResp, error) {
			info, err := svc.InspectVolume(ctx, req.Volume)
			if err != nil {
				return nil, err
			}
			return &InspectVolumeResp{Info: *info}, nil
		})
	case MT_VOLUME_AWAIT:
		handleAsk(req, resp, &AwaitReq{}, func(req *AwaitReq) (*AwaitResp, error) {
			if err := svc.Await(ctx, req.Cond); err != nil {
				return nil, err
			}
			return &AwaitResp{}, nil
		})
	case MT_VOLUME_BEGIN_TX:
		handleAsk(req, resp, &BeginTxReq{}, func(req *BeginTxReq) (*BeginTxResp, error) {
			h, err := svc.BeginTx(ctx, req.Volume, req.Params)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, req.Volume)
			if err != nil {
				return nil, err
			}
			return &BeginTxResp{Tx: *h, VolumeInfo: *info}, nil
		})
	// END VOLUME

	// BEGIN TX
	case MT_TX_COMMIT:
		handleAsk(req, resp, &CommitReq{}, func(req *CommitReq) (*CommitResp, error) {
			if req.Root != nil {
				if err := svc.Save(ctx, req.Tx, *req.Root); err != nil {
					return nil, err
				}
			}
			if err := svc.Commit(ctx, req.Tx); err != nil {
				return nil, err
			}
			return &CommitResp{}, nil
		})
	case MT_TX_INSPECT:
		handleAsk(req, resp, &InspectTxReq{}, func(req *InspectTxReq) (*InspectTxResp, error) {
			info, err := svc.InspectTx(ctx, req.Tx)
			if err != nil {
				return nil, err
			}
			return &InspectTxResp{Info: *info}, nil
		})
	case MT_TX_ABORT:
		handleAsk(req, resp, &AbortReq{}, func(req *AbortReq) (*AbortResp, error) {
			if err := svc.Abort(ctx, req.Tx); err != nil {
				return nil, err
			}
			return &AbortResp{}, nil
		})
	case MT_TX_LOAD:
		handleAsk(req, resp, &LoadReq{}, func(req *LoadReq) (*LoadResp, error) {
			var root []byte
			if err := svc.Load(ctx, req.Tx, &root); err != nil {
				return nil, err
			}
			return &LoadResp{Root: root}, nil
		})
	case MT_TX_SAVE:
		handleAsk(req, resp, &SaveReq{}, func(req *SaveReq) (*SaveResp, error) {
			if err := svc.Save(ctx, req.Tx, req.Root); err != nil {
				return nil, err
			}
			return &SaveResp{}, nil
		})
	case MT_TX_EXISTS:
		handleAsk(req, resp, &ExistsReq{}, func(req *ExistsReq) (*ExistsResp, error) {
			exists := make([]bool, len(req.CIDs))
			if err := svc.Exists(ctx, req.Tx, req.CIDs, exists); err != nil {
				return nil, err
			}
			return &ExistsResp{Exists: exists}, nil
		})
	case MT_TX_DELETE:
		handleAsk(req, resp, &DeleteReq{}, func(req *DeleteReq) (*DeleteResp, error) {
			if err := svc.Delete(ctx, req.Tx, req.CIDs); err != nil {
				return nil, err
			}
			return &DeleteResp{}, nil
		})
	case MT_TX_POST:
		h, body, err := readHandle(req.Body())
		if err != nil {
			resp.SetError(err)
			return
		}
		cid, err := svc.Post(ctx, *h, body, blobcache.PostOpts{})
		if err != nil {
			resp.SetError(err)
			return
		}
		resp.SetCode(MT_OK)
		resp.SetBody(cid[:])
	case MT_TX_POST_SALT:
		h, body, err := readHandle(req.Body())
		if err != nil {
			resp.SetError(err)
			return
		}
		if len(body) < blobcache.CIDSize {
			resp.SetError(fmt.Errorf("invalid request body length: %d", len(body)))
			return
		}
		salt := blobcache.CID(body[:blobcache.CIDSize])
		body = body[blobcache.CIDSize:]
		cid, err := svc.Post(ctx, *h, body, blobcache.PostOpts{Salt: &salt})
		if err != nil {
			resp.SetError(err)
			return
		}
		resp.SetCode(MT_OK)
		resp.SetBody(cid[:])
	case MT_TX_ADD_FROM:
		handleAsk(req, resp, &AddFromReq{}, func(req *AddFromReq) (*AddFromResp, error) {
			success := make([]bool, len(req.CIDs))
			if err := svc.Copy(ctx, req.Tx, req.CIDs, req.Srcs, success); err != nil {
				return nil, err
			}
			return &AddFromResp{Added: success}, nil
		})
	case MT_TX_GET:
		h, body, err := readHandle(req.Body())
		if err != nil {
			resp.SetError(err)
			return
		}
		if len(body) != blobcache.CIDSize {
			resp.SetError(fmt.Errorf("invalid request body length: %d", len(body)))
			return
		}
		var cid blobcache.CID
		copy(cid[:], body)

		info, err := svc.InspectTx(ctx, *h)
		if err != nil {
			resp.SetError(err)
			return
		}
		buf := make([]byte, info.MaxSize)
		n, err := svc.Get(ctx, *h, cid, buf, blobcache.GetOpts{SkipVerify: true})
		if err != nil {
			resp.SetError(err)
			return
		}
		resp.SetCode(MT_OK)
		resp.SetBody(buf[:n])
	case MT_TX_ALLOW_LINK:
		handleAsk(req, resp, &AllowLinkReq{}, func(req *AllowLinkReq) (*AllowLinkResp, error) {
			if err := svc.AllowLink(ctx, req.Tx, req.Subvol); err != nil {
				return nil, err
			}
			return &AllowLinkResp{}, nil
		})
	case MT_TX_VISIT:
		handleAsk(req, resp, &VisitReq{}, func(req *VisitReq) (*VisitResp, error) {
			if err := svc.Visit(ctx, req.Tx, req.CIDs); err != nil {
				return nil, err
			}
			return &VisitResp{}, nil
		})
	case MT_TX_IS_VISITED:
		handleAsk(req, resp, &IsVisitedReq{}, func(req *IsVisitedReq) (*IsVisitedResp, error) {
			visited := make([]bool, len(req.CIDs))
			if err := svc.IsVisited(ctx, req.Tx, req.CIDs, visited); err != nil {
				return nil, err
			}
			return &IsVisitedResp{Visited: visited}, nil
		})
	// END TX

	default:
		resp.SetError(fmt.Errorf("unknown message type: %v", req.Header().Code()))
	}
}

func readHandle(body []byte) (*blobcache.Handle, []byte, error) {
	if len(body) < blobcache.HandleSize {
		return nil, nil, fmt.Errorf("invalid request body length: %d", len(body))
	}
	var h blobcache.Handle
	if err := h.Unmarshal(body[:blobcache.HandleSize]); err != nil {
		return nil, nil, err
	}
	body = body[blobcache.HandleSize:]
	return &h, body, nil
}

type Marshaller interface {
	Marshal(out []byte) []byte
}

type Unmarshaller interface {
	Unmarshal(data []byte) error
}

func handleAsk[Req Unmarshaller, Resp Marshaller](req *Message, resp *Message, zeroReq Req, fn func(Req) (*Resp, error)) {
	var reqR = zeroReq
	if err := reqR.Unmarshal(req.Body()); err != nil {
		resp.SetError(err)
		return
	}
	respR, err := fn(reqR)
	if err != nil {
		resp.SetError(err)
		return
	}
	data := (*respR).Marshal(nil)
	resp.SetCode(MT_OK)
	resp.SetBody(data)
}
