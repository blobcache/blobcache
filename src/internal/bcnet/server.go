package bcnet

import (
	"context"
	"encoding/json"
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
	case MT_OPEN:
		handleJSON(req, resp, func(req *OpenReq) (*OpenResp, error) {
			h, err := svc.Open(ctx, req.OID)
			if err != nil {
				return nil, err
			}
			info, err := svc.InspectVolume(ctx, *h)
			if err != nil {
				return nil, err
			}
			return &OpenResp{Handle: *h, Info: *info}, nil
		})
	case MT_HANDLE_DROP:
		handleJSON(req, resp, func(req *DropReq) (*DropResp, error) {
			if err := svc.Drop(ctx, req.Handle); err != nil {
				return nil, err
			}
			return &DropResp{}, nil
		})
	case MT_HANDLE_KEEP_ALIVE:
		handleJSON(req, resp, func(req *KeepAliveReq) (*KeepAliveResp, error) {
			if err := svc.KeepAlive(ctx, req.Handles); err != nil {
				return nil, err
			}
			return &KeepAliveResp{}, nil
		})

	case MT_NAMESPACE_OPEN_AT:
		handleJSON(req, resp, func(req *OpenAtReq) (*OpenAtResp, error) {
			h, err := svc.OpenAt(ctx, req.Namespace, req.Name)
			if err != nil {
				return nil, err
			}
			return &OpenAtResp{Handle: *h}, nil
		})
	case MT_NAMESPACE_CREATE_AT:
		handleJSON(req, resp, func(req *CreateVolumeAtReq) (*CreateVolumeAtResp, error) {
			h, err := svc.CreateVolumeAt(ctx, req.Namespace, req.Name, req.Spec)
			if err != nil {
				return nil, err
			}
			return &CreateVolumeAtResp{Handle: *h}, nil
		})
	case MT_NAMESPACE_LIST_NAMES:
		handleJSON(req, resp, func(req *ListNamesReq) (*ListNamesResp, error) {
			names, err := svc.ListNames(ctx, req.Namespace)
			if err != nil {
				return nil, err
			}
			return &ListNamesResp{Names: names}, nil
		})
	case MT_NAMESPACE_GET_ENTRY:
		handleJSON(req, resp, func(req *GetEntryReq) (*GetEntryResp, error) {
			entry, err := svc.GetEntry(ctx, req.Namespace, req.Name)
			if err != nil {
				return nil, err
			}
			return &GetEntryResp{Entry: *entry}, nil
		})

	case MT_VOLUME_INSPECT:
		handleJSON(req, resp, func(req *InspectVolumeReq) (*InspectVolumeResp, error) {
			info, err := svc.InspectVolume(ctx, req.Volume)
			if err != nil {
				return nil, err
			}
			return &InspectVolumeResp{Info: info}, nil
		})
	case MT_VOLUME_BEGIN_TX:
		handleJSON(req, resp, func(req *BeginTxReq) (*BeginTxResp, error) {
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

	case MT_TX_COMMIT:
		handleJSON(req, resp, func(req *CommitReq) (*CommitResp, error) {
			if err := svc.Commit(ctx, req.Tx, req.Root); err != nil {
				return nil, err
			}
			return &CommitResp{}, nil
		})
	case MT_TX_ABORT:
		handleJSON(req, resp, func(req *AbortReq) (*AbortResp, error) {
			if err := svc.Abort(ctx, req.Tx); err != nil {
				return nil, err
			}
			return &AbortResp{}, nil
		})
	case MT_TX_LOAD:
		handleJSON(req, resp, func(req *LoadReq) (*LoadResp, error) {
			var root []byte
			if err := svc.Load(ctx, req.Tx, &root); err != nil {
				return nil, err
			}
			return &LoadResp{Root: root}, nil
		})
	case MT_TX_EXISTS:
		handleJSON(req, resp, func(req *ExistsReq) (*ExistsResp, error) {
			exists := make([]bool, len(req.CIDs))
			for i, cid := range req.CIDs {
				var err error
				exists[i], err = svc.Exists(ctx, req.Tx, cid)
				if err != nil {
					return nil, err
				}
			}
			return &ExistsResp{Exists: exists}, nil
		})
	case MT_TX_DELETE:
		handleJSON(req, resp, func(req *DeleteReq) (*DeleteResp, error) {
			if err := svc.Delete(ctx, req.Tx, req.CID); err != nil {
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
		cid, err := svc.Post(ctx, *h, nil, body)
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
		cid, err := svc.Post(ctx, *h, nil, body)
		if err != nil {
			resp.SetError(err)
			return
		}
		resp.SetCode(MT_OK)
		resp.SetBody(cid[:])
	case MT_TX_GET:
		h, body, err := readHandle(req.Body())
		if err != nil {
			resp.SetError(err)
			return
		}
		if len(body) != blobcache.CIDBytes {
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
		n, err := svc.Get(ctx, *h, cid, nil, buf)
		if err != nil {
			resp.SetError(err)
			return
		}
		resp.SetCode(MT_OK)
		resp.SetBody(buf[:n])
	default:
		resp.SetError(fmt.Errorf("unknown message type: %v", req.Header().Code()))
	}
}

func readHandle(body []byte) (*blobcache.Handle, []byte, error) {
	const handleSize = 32
	if len(body) < handleSize {
		return nil, nil, fmt.Errorf("invalid request body length: %d", len(body))
	}
	var h blobcache.Handle
	if err := h.UnmarshalBinary(body[:handleSize]); err != nil {
		return nil, nil, err
	}
	body = body[handleSize:]
	return &h, body, nil
}

func handleJSON[Req, Resp any](req *Message, resp *Message, fn func(Req) (*Resp, error)) {
	var reqR Req
	if err := json.Unmarshal(req.Body(), &reqR); err != nil {
		resp.SetError(err)
		return
	}
	respR, err := fn(reqR)
	if err != nil {
		resp.SetError(err)
		return
	}
	data, err := json.Marshal(respR)
	if err != nil {
		resp.SetError(err)
		return
	}
	resp.SetCode(MT_OK)
	resp.SetBody(data)
}
