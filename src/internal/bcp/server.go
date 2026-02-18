package bcp

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

// AccessFunc is called to get a Service to access
// If the returned Service is nil, then the connection
// is disconnected as quickly as possible, and no further
// information should be sent to the initiator.
//
// "Get off the phone" https://i.gifer.com/2Yv2.gif
type AccessFunc func(blobcache.PeerID) blobcache.Service

type TopicMessage = blobcache.Message

var _ Handler = &Server{}

type Server struct {
	Access  AccessFunc
	Deliver func(ctx context.Context, from blobcache.Endpoint, ttm TopicTellMsg) error
}

func (s *Server) ServeBCP(ctx context.Context, ep blobcache.Endpoint, req Message, resp *Message) bool {
	svc := s.Access(ep.Peer)
	if svc == nil {
		return false
	}

	switch req.Header().Code() {
	case MT_PING:
		resp.SetCode(MT_OK)
		resp.SetBody(nil)
	case MT_ENDPOINT:
		handleAsk(req, resp, &EndpointReq{}, func(req *EndpointReq) (*EndpointResp, error) {
			ep, err := svc.Endpoint(ctx)
			if err != nil {
				return nil, err
			}
			return &EndpointResp{Endpoint: ep}, nil
		})

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
	case MT_OPEN_FIAT:
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
			h, err := svc.OpenFrom(ctx, req.Base, req.Token, req.Mask)
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
	case MT_VOLUME_BEGIN_TX:
		handleAsk(req, resp, &BeginTxReq{}, func(req *BeginTxReq) (*BeginTxResp, error) {
			h, err := svc.BeginTx(ctx, req.Volume, req.Params)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectTx(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &BeginTxResp{Tx: *h, Info: *info}, nil
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
			return true
		}
		cid, err := svc.Post(ctx, *h, body, blobcache.PostOpts{})
		if err != nil {
			resp.SetError(err)
			return true
		}
		resp.SetCode(MT_OK)
		resp.SetBody(cid[:])
	case MT_TX_POST_SALT:
		h, body, err := readHandle(req.Body())
		if err != nil {
			resp.SetError(err)
			return true
		}
		if len(body) < blobcache.CIDSize {
			resp.SetError(fmt.Errorf("invalid request body length: %d", len(body)))
			return true
		}
		salt := blobcache.CID(body[:blobcache.CIDSize])
		body = body[blobcache.CIDSize:]
		cid, err := svc.Post(ctx, *h, body, blobcache.PostOpts{Salt: &salt})
		if err != nil {
			resp.SetError(err)
			return true
		}
		resp.SetCode(MT_OK)
		resp.SetBody(cid[:])
	case MT_TX_ADD_FROM:
		handleAsk(req, resp, &AddFromReq{}, func(req *AddFromReq) (*AddFromResp, error) {
			success := make([]bool, len(req.CIDs))
			if err := svc.Copy(ctx, req.Tx, req.Srcs, req.CIDs, success); err != nil {
				return nil, err
			}
			return &AddFromResp{Added: success}, nil
		})
	case MT_TX_GET:
		h, body, err := readHandle(req.Body())
		if err != nil {
			resp.SetError(err)
			return true
		}
		if len(body) != blobcache.CIDSize {
			resp.SetError(fmt.Errorf("invalid request body length: %d", len(body)))
			return true
		}
		var cid blobcache.CID
		copy(cid[:], body)

		info, err := svc.InspectTx(ctx, *h)
		if err != nil {
			resp.SetError(err)
			return true
		}
		buf := make([]byte, info.MaxSize)
		n, err := svc.Get(ctx, *h, cid, buf, blobcache.GetOpts{SkipVerify: true})
		if err != nil {
			resp.SetError(err)
			return true
		}
		resp.SetCode(MT_OK)
		resp.SetBody(buf[:n])
	case MT_TX_LINK:
		handleAsk(req, resp, &LinkReq{}, func(req *LinkReq) (*LinkResp, error) {
			ltok, err := svc.Link(ctx, req.Tx, req.Subvol, req.Mask)
			if err != nil {
				return nil, err
			}
			return &LinkResp{Token: *ltok}, nil
		})
	case MT_TX_UNLINK:
		handleAsk(req, resp, &UnlinkReq{}, func(req *UnlinkReq) (*UnlinkResp, error) {
			if err := svc.Unlink(ctx, req.Tx, req.Targets); err != nil {
				return nil, err
			}
			return &UnlinkResp{}, nil
		})
	case MT_TX_VISIT_LINKS:
		handleAsk(req, resp, &VisitLinksReq{}, func(req *VisitLinksReq) (*VisitLinksResp, error) {
			if err := svc.VisitLinks(ctx, req.Tx, req.Targets); err != nil {
				return nil, err
			}
			return &VisitLinksResp{}, nil
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

	// BEGIN QUEUE
	case MT_QUEUE_CREATE:
		handleAsk(req, resp, &CreateQueueReq{}, func(req *CreateQueueReq) (*CreateQueueResp, error) {
			h, err := svc.CreateQueue(ctx, nil, req.Spec)
			if err != nil {
				return nil, err
			}
			return &CreateQueueResp{Handle: *h}, nil
		})
	case MT_QUEUE_INSPECT:
		handleAsk(req, resp, &InspectQueueReq{}, func(req *InspectQueueReq) (*InspectQueueResp, error) {
			info, err := svc.InspectQueue(ctx, req.Queue)
			if err != nil {
				return nil, err
			}
			return &InspectQueueResp{Info: info}, nil
		})
	case MT_QUEUE_DEQUEUE:
		handleAsk(req, resp, &DequeueReq{}, func(req *DequeueReq) (*DequeueResp, error) {
			if req.Max <= 0 {
				return &DequeueResp{}, nil
			}
			buf := make([]blobcache.Message, req.Max)
			n, err := svc.Dequeue(ctx, req.Queue, buf, req.Opts)
			if err != nil {
				return nil, err
			}
			return &DequeueResp{Messages: buf[:n]}, nil
		})
	case MT_QUEUE_ENQUEUE:
		handleAsk(req, resp, &EnqueueReq{}, func(req *EnqueueReq) (*EnqueueResp, error) {
			ins, err := svc.Enqueue(ctx, req.Queue, req.Messages)
			if err != nil {
				return nil, err
			}
			return &EnqueueResp{Success: ins.Success}, nil
		})
	case MT_QUEUE_SUB_TO_VOLUME:
		handleAsk(req, resp, &SubToVolumeReq{}, func(req *SubToVolumeReq) (*SubToVolumeResp, error) {
			if err := svc.SubToVolume(ctx, req.Queue, req.Volume, req.Spec); err != nil {
				return nil, err
			}
			return &SubToVolumeResp{}, nil
		})
	// END QUEUE

	default:
		resp.SetError(fmt.Errorf("unknown message type: %v", req.Header().Code()))
	}
	return true
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

func handleAsk[Req Unmarshaller, Resp Marshaller](req Message, resp *Message, zeroReq Req, fn func(Req) (*Resp, error)) {
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
