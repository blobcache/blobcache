package bcnet

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// AccessFun is called to get a service to access
type AccessFunc func(blobcache.PeerID) blobcache.Service

type TopicMessage = blobcache.Message

type Server struct {
	Access  AccessFunc
	Deliver func(ctx context.Context, from blobcache.Endpoint, ttm bcp.TopicTellMsg) error
}

func (s *Server) serve(ctx context.Context, ep blobcache.Endpoint, req *Message, resp *Message) {
	svc := s.Access(ep.Peer)
	if svc == nil {
		resp.SetError(fmt.Errorf("not allowed"))
		return
	}

	switch req.Header().Code() {
	case bcp.MT_PING:
		resp.SetCode(bcp.MT_OK)
		resp.SetBody(nil)

	// BEGIN HANDLE
	case bcp.MT_HANDLE_DROP:
		handleAsk(req, resp, &bcp.DropReq{}, func(req *bcp.DropReq) (*bcp.DropResp, error) {
			if err := svc.Drop(ctx, req.Handle); err != nil {
				return nil, err
			}
			return &bcp.DropResp{}, nil
		})
	case bcp.MT_HANDLE_INSPECT:
		handleAsk(req, resp, &bcp.InspectHandleReq{}, func(req *bcp.InspectHandleReq) (*bcp.InspectHandleResp, error) {
			info, err := svc.InspectHandle(ctx, req.Handle)
			if err != nil {
				return nil, err
			}
			return &bcp.InspectHandleResp{Info: *info}, nil
		})
	case bcp.MT_HANDLE_KEEP_ALIVE:
		handleAsk(req, resp, &bcp.KeepAliveReq{}, func(req *bcp.KeepAliveReq) (*bcp.KeepAliveResp, error) {
			if err := svc.KeepAlive(ctx, req.Handles); err != nil {
				return nil, err
			}
			return &bcp.KeepAliveResp{}, nil
		})
	case bcp.MT_HANDLE_SHARE:
		handleAsk(req, resp, &bcp.ShareReq{}, func(req *bcp.ShareReq) (*bcp.ShareResp, error) {
			h, err := svc.Share(ctx, req.Handle, req.Peer, req.Mask)
			if err != nil {
				return nil, err
			}
			return &bcp.ShareResp{Handle: *h}, nil
		})
	// END HANDLE

	// BEGIN VOLUME
	case bcp.MT_OPEN_AS:
		handleAsk(req, resp, &bcp.OpenFiatReq{}, func(req *bcp.OpenFiatReq) (*bcp.OpenFiatResp, error) {
			h, err := svc.OpenFiat(ctx, req.Target, req.Mask)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &bcp.OpenFiatResp{Handle: *h, Info: *info}, nil
		})
	case bcp.MT_OPEN_FROM:
		handleAsk(req, resp, &bcp.OpenFromReq{}, func(req *bcp.OpenFromReq) (*bcp.OpenFromResp, error) {
			h, err := svc.OpenFrom(ctx, req.Base, req.Target, req.Mask)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &bcp.OpenFromResp{Handle: *h, Info: *info}, nil
		})
	case bcp.MT_CREATE_VOLUME:
		handleAsk(req, resp, &bcp.CreateVolumeReq{}, func(req *bcp.CreateVolumeReq) (*bcp.CreateVolumeResp, error) {
			h, err := svc.CreateVolume(ctx, nil, req.Spec)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &bcp.CreateVolumeResp{Handle: *h, Info: *info}, nil
		})
	case bcp.MT_VOLUME_CLONE:
		handleAsk(req, resp, &bcp.CloneVolumeReq{}, func(req *bcp.CloneVolumeReq) (*bcp.CloneVolumeResp, error) {
			h, err := svc.CloneVolume(ctx, &ep.Peer, req.Volume)
			if err != nil {
				return nil, err
			}
			return &bcp.CloneVolumeResp{Handle: *h}, nil
		})
	case bcp.MT_VOLUME_INSPECT:
		handleAsk(req, resp, &bcp.InspectVolumeReq{}, func(req *bcp.InspectVolumeReq) (*bcp.InspectVolumeResp, error) {
			info, err := svc.InspectVolume(ctx, req.Volume)
			if err != nil {
				return nil, err
			}
			return &bcp.InspectVolumeResp{Info: *info}, nil
		})
	case bcp.MT_VOLUME_BEGIN_TX:
		handleAsk(req, resp, &bcp.BeginTxReq{}, func(req *bcp.BeginTxReq) (*bcp.BeginTxResp, error) {
			h, err := svc.BeginTx(ctx, req.Volume, req.Params)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectTx(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &bcp.BeginTxResp{Tx: *h, Info: *info}, nil
		})
	// END VOLUME

	// BEGIN TX
	case bcp.MT_TX_COMMIT:
		handleAsk(req, resp, &bcp.CommitReq{}, func(req *bcp.CommitReq) (*bcp.CommitResp, error) {
			if req.Root != nil {
				if err := svc.Save(ctx, req.Tx, *req.Root); err != nil {
					return nil, err
				}
			}
			if err := svc.Commit(ctx, req.Tx); err != nil {
				return nil, err
			}
			return &bcp.CommitResp{}, nil
		})
	case bcp.MT_TX_INSPECT:
		handleAsk(req, resp, &bcp.InspectTxReq{}, func(req *bcp.InspectTxReq) (*bcp.InspectTxResp, error) {
			info, err := svc.InspectTx(ctx, req.Tx)
			if err != nil {
				return nil, err
			}
			return &bcp.InspectTxResp{Info: *info}, nil
		})
	case bcp.MT_TX_ABORT:
		handleAsk(req, resp, &bcp.AbortReq{}, func(req *bcp.AbortReq) (*bcp.AbortResp, error) {
			if err := svc.Abort(ctx, req.Tx); err != nil {
				return nil, err
			}
			return &bcp.AbortResp{}, nil
		})
	case bcp.MT_TX_LOAD:
		handleAsk(req, resp, &bcp.LoadReq{}, func(req *bcp.LoadReq) (*bcp.LoadResp, error) {
			var root []byte
			if err := svc.Load(ctx, req.Tx, &root); err != nil {
				return nil, err
			}
			return &bcp.LoadResp{Root: root}, nil
		})
	case bcp.MT_TX_SAVE:
		handleAsk(req, resp, &bcp.SaveReq{}, func(req *bcp.SaveReq) (*bcp.SaveResp, error) {
			if err := svc.Save(ctx, req.Tx, req.Root); err != nil {
				return nil, err
			}
			return &bcp.SaveResp{}, nil
		})
	case bcp.MT_TX_EXISTS:
		handleAsk(req, resp, &bcp.ExistsReq{}, func(req *bcp.ExistsReq) (*bcp.ExistsResp, error) {
			exists := make([]bool, len(req.CIDs))
			if err := svc.Exists(ctx, req.Tx, req.CIDs, exists); err != nil {
				return nil, err
			}
			return &bcp.ExistsResp{Exists: exists}, nil
		})
	case bcp.MT_TX_DELETE:
		handleAsk(req, resp, &bcp.DeleteReq{}, func(req *bcp.DeleteReq) (*bcp.DeleteResp, error) {
			if err := svc.Delete(ctx, req.Tx, req.CIDs); err != nil {
				return nil, err
			}
			return &bcp.DeleteResp{}, nil
		})
	case bcp.MT_TX_POST:
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
		resp.SetCode(bcp.MT_OK)
		resp.SetBody(cid[:])
	case bcp.MT_TX_POST_SALT:
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
		resp.SetCode(bcp.MT_OK)
		resp.SetBody(cid[:])
	case bcp.MT_TX_ADD_FROM:
		handleAsk(req, resp, &bcp.AddFromReq{}, func(req *bcp.AddFromReq) (*bcp.AddFromResp, error) {
			success := make([]bool, len(req.CIDs))
			if err := svc.Copy(ctx, req.Tx, req.Srcs, req.CIDs, success); err != nil {
				return nil, err
			}
			return &bcp.AddFromResp{Added: success}, nil
		})
	case bcp.MT_TX_GET:
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
		resp.SetCode(bcp.MT_OK)
		resp.SetBody(buf[:n])
	case bcp.MT_TX_LINK:
		handleAsk(req, resp, &bcp.LinkReq{}, func(req *bcp.LinkReq) (*bcp.LinkResp, error) {
			if err := svc.Link(ctx, req.Tx, req.Subvol, req.Mask); err != nil {
				return nil, err
			}
			return &bcp.LinkResp{}, nil
		})
	case bcp.MT_TX_UNLINK:
		handleAsk(req, resp, &bcp.UnlinkReq{}, func(req *bcp.UnlinkReq) (*bcp.UnlinkResp, error) {
			if err := svc.Unlink(ctx, req.Tx, req.Targets); err != nil {
				return nil, err
			}
			return &bcp.UnlinkResp{}, nil
		})
	case bcp.MT_TX_VISIT_LINKS:
		handleAsk(req, resp, &bcp.VisitLinksReq{}, func(req *bcp.VisitLinksReq) (*bcp.VisitLinksResp, error) {
			if err := svc.VisitLinks(ctx, req.Tx, req.Targets); err != nil {
				return nil, err
			}
			return &bcp.VisitLinksResp{}, nil
		})
	case bcp.MT_TX_VISIT:
		handleAsk(req, resp, &bcp.VisitReq{}, func(req *bcp.VisitReq) (*bcp.VisitResp, error) {
			if err := svc.Visit(ctx, req.Tx, req.CIDs); err != nil {
				return nil, err
			}
			return &bcp.VisitResp{}, nil
		})
	case bcp.MT_TX_IS_VISITED:
		handleAsk(req, resp, &bcp.IsVisitedReq{}, func(req *bcp.IsVisitedReq) (*bcp.IsVisitedResp, error) {
			visited := make([]bool, len(req.CIDs))
			if err := svc.IsVisited(ctx, req.Tx, req.CIDs, visited); err != nil {
				return nil, err
			}
			return &bcp.IsVisitedResp{Visited: visited}, nil
		})
	// END TX

	// BEGIN TOPIC
	case bcp.MT_TOPIC_TELL:
		if err := func() error {
			var ttm bcp.TopicTellMsg
			if err := ttm.Unmarshal(req.Body()); err != nil {
				return err
			}
			return s.Deliver(ctx, ep, ttm)
		}(); err != nil {
			logctx.Error(ctx, "handling topic tell", zap.Error(err))
			return
		}
	// END TOPIC

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
	resp.SetCode(bcp.MT_OK)
	resp.SetBody(data)
}
