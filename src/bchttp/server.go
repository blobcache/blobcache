package bchttp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var _ http.Handler = &Server{}

type Server struct {
	Service blobcache.Service
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/OpenFrom":
		handleRequest(w, r, func(ctx context.Context, req OpenFromReq) (*OpenFromResp, error) {
			handle, err := s.Service.OpenFrom(ctx, req.Base, req.Target, req.Mask)
			if err != nil {
				return nil, err
			}
			volInfo, err := s.Service.InspectVolume(ctx, *handle)
			if err != nil {
				return nil, err
			}
			return &OpenFromResp{Handle: *handle, Info: *volInfo}, nil
		})
	case r.URL.Path == "/OpenFiat":
		handleRequest(w, r, func(ctx context.Context, req OpenFiatReq) (*OpenFiatResp, error) {
			handle, err := s.Service.OpenFiat(ctx, req.Target, req.Mask)
			if err != nil {
				return nil, err
			}
			volInfo, err := s.Service.InspectVolume(ctx, *handle)
			if err != nil {
				return nil, err
			}
			return &OpenFiatResp{Handle: *handle, Info: *volInfo}, nil
		})
	case r.URL.Path == "/Endpoint":
		handleRequest(w, r, func(ctx context.Context, req EndpointReq) (*EndpointResp, error) {
			ep, err := s.Service.Endpoint(ctx)
			if err != nil {
				return nil, err
			}
			return &EndpointResp{Endpoint: ep}, nil
		})
	case r.URL.Path == "/KeepAlive":
		handleRequest(w, r, func(ctx context.Context, req KeepAliveReq) (*KeepAliveResp, error) {
			err := s.Service.KeepAlive(ctx, req.Handles)
			if err != nil {
				return nil, err
			}
			return &KeepAliveResp{}, nil
		})
	case r.URL.Path == "/Drop":
		handleRequest(w, r, func(ctx context.Context, req DropReq) (*DropResp, error) {
			err := s.Service.Drop(ctx, req.Handle)
			if err != nil {
				return nil, err
			}
			return &DropResp{}, nil
		})
	case r.URL.Path == "/Share":
		handleRequest(w, r, func(ctx context.Context, req ShareReq) (*ShareResp, error) {
			handle, err := s.Service.Share(ctx, req.Handle, req.Peer, req.Mask)
			if err != nil {
				return nil, err
			}
			return &ShareResp{Handle: *handle}, nil
		})
	case strings.HasPrefix(r.URL.Path, "/queue/"):
		s.handleQueue(w, r)
	case strings.HasPrefix(r.URL.Path, "/volume/"):
		s.handleVolume(w, r)
	case strings.HasPrefix(r.URL.Path, "/tx/"):
		s.handleTx(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleVolume(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/volume/":
		handleRequest(w, r, func(ctx context.Context, req CreateVolumeReq) (*CreateVolumeResp, error) {
			vol, err := s.Service.CreateVolume(ctx, req.Host, req.Spec)
			if err != nil {
				return nil, err
			}
			return &CreateVolumeResp{Handle: *vol}, nil
		})
		return
	}
	var volIDStr string
	var method string
	if _, err := fmt.Sscanf(r.URL.Path, "/volume/%32s.%s", &volIDStr, &method); err != nil {
		http.Error(w, "could not parse path "+r.URL.Path, http.StatusBadRequest)
		return
	}
	var h blobcache.Handle
	if _, err := hex.Decode(h.OID[:], []byte(volIDStr)); err != nil {
		http.Error(w, "could not decode volume id", http.StatusBadRequest)
		return
	}
	secretStr := r.Header.Get("X-Secret")
	if _, err := hex.Decode(h.Secret[:], []byte(secretStr)); err != nil {
		http.Error(w, "could not decode secret", http.StatusBadRequest)
		return
	}
	switch method {
	case "Inspect":
		info, err := s.Service.InspectVolume(r.Context(), h)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(info); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}
}

func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/tx/" {
		handleRequest(w, r, func(ctx context.Context, req BeginTxReq) (*BeginTxResp, error) {
			txh, err := s.Service.BeginTx(ctx, req.Volume, req.Params)
			if err != nil {
				return nil, err
			}
			return &BeginTxResp{Tx: *txh}, nil
		})
		return
	}
	var txIDStr string
	var method string
	if _, err := fmt.Sscanf(r.URL.Path, "/tx/%32s.%s", &txIDStr, &method); err != nil {
		http.Error(w, "could not parse path "+r.URL.Path, http.StatusBadRequest)
		return
	}
	var h blobcache.Handle
	if _, err := hex.Decode(h.OID[:], []byte(txIDStr)); err != nil {
		http.Error(w, "could not decode tx id", http.StatusBadRequest)
		return
	}
	secretStr := r.Header.Get("X-Secret")
	if _, err := hex.Decode(h.Secret[:], []byte(secretStr)); err != nil {
		http.Error(w, "could not decode secret", http.StatusBadRequest)
		return
	}
	switch method {
	case "Inspect":
		handleRequest(w, r, func(ctx context.Context, req InspectTxReq) (*InspectTxResp, error) {
			info, err := s.Service.InspectTx(ctx, h)
			if err != nil {
				return nil, err
			}
			return &InspectTxResp{Info: *info}, nil
		})
	case "Commit":
		handleRequest(w, r, func(ctx context.Context, req CommitReq) (*CommitResp, error) {
			if err := s.Service.Commit(ctx, h); err != nil {
				return nil, err
			}
			return &CommitResp{}, nil
		})
	case "Abort":
		handleRequest(w, r, func(ctx context.Context, req AbortReq) (*AbortResp, error) {
			if err := s.Service.Abort(ctx, h); err != nil {
				return nil, err
			}
			return &AbortResp{}, nil
		})
	case "Load":
		handleRequest(w, r, func(ctx context.Context, req LoadReq) (*LoadResp, error) {
			var root []byte
			if err := s.Service.Load(ctx, h, &root); err != nil {
				return nil, err
			}
			return &LoadResp{Root: root}, nil
		})
	case "Save":
		handleRequest(w, r, func(ctx context.Context, req SaveReq) (*SaveResp, error) {
			if err := s.Service.Save(ctx, h, req.Root); err != nil {
				return nil, err
			}
			return &SaveResp{}, nil
		})
	// Post and Get are special cases that does not use JSON.
	case "Post":
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		salt, err := getSaltFromRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cid, err := s.Service.Post(r.Context(), h, data, blobcache.PostOpts{Salt: salt})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(cid[:])
		return
	case "Get":
		var req GetReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		buf := make([]byte, 1<<21)
		n, err := s.Service.Get(r.Context(), h, req.CID, buf, blobcache.GetOpts{Salt: req.Salt})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(buf[:n])
		return
	case "Delete":
		handleRequest(w, r, func(ctx context.Context, req DeleteReq) (*DeleteResp, error) {
			if err := s.Service.Delete(ctx, h, req.CIDs); err != nil {
				return nil, err
			}
			return &DeleteResp{}, nil
		})
	case "Exists":
		handleRequest(w, r, func(ctx context.Context, req ExistsReq) (*ExistsResp, error) {
			exists := make([]bool, len(req.CIDs))
			if err := s.Service.Exists(ctx, h, req.CIDs, exists); err != nil {
				return nil, err
			}
			return &ExistsResp{Exists: exists}, nil
		})
	case "Visit":
		handleRequest(w, r, func(ctx context.Context, req VisitReq) (*VisitResp, error) {
			if err := s.Service.Visit(ctx, h, req.CIDs); err != nil {
				return nil, err
			}
			return &VisitResp{}, nil
		})
	case "IsVisited":
		handleRequest(w, r, func(ctx context.Context, req IsVisitedReq) (*IsVisitedResp, error) {
			visited := make([]bool, len(req.CIDs))
			if err := s.Service.IsVisited(ctx, h, req.CIDs, visited); err != nil {
				return nil, err
			}
			return &IsVisitedResp{Visited: visited}, nil
		})
	case "Link":
		handleRequest(w, r, func(ctx context.Context, req LinkReq) (*LinkResp, error) {
			if err := s.Service.Link(ctx, h, req.Target, req.Mask); err != nil {
				return nil, err
			}
			return &LinkResp{}, nil
		})
	case "Unlink":
		handleRequest(w, r, func(ctx context.Context, req UnlinkReq) (*UnlinkResp, error) {
			if err := s.Service.Unlink(ctx, h, req.Targets); err != nil {
				return nil, err
			}
			return &UnlinkResp{}, nil
		})
	case "VisitLinks":
		handleRequest(w, r, func(ctx context.Context, req VisitLinksReq) (*VisitLinksResp, error) {
			if err := s.Service.VisitLinks(ctx, h, req.Targets); err != nil {
				return nil, err
			}
			return &VisitLinksResp{}, nil
		})
	default:
		http.Error(w, fmt.Sprintf("unsupported method %v", method), http.StatusNotFound)
	}
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/queue/":
		handleRequest(w, r, func(ctx context.Context, req CreateQueueReq) (*CreateQueueResp, error) {
			qh, err := s.Service.CreateQueue(ctx, req.Host, req.Spec)
			if err != nil {
				return nil, err
			}
			return &CreateQueueResp{Handle: *qh}, nil
		})
		return
	}
	var queueIDStr string
	var method string
	if _, err := fmt.Sscanf(r.URL.Path, "/queue/%32s.%s", &queueIDStr, &method); err != nil {
		http.Error(w, "could not parse path "+r.URL.Path, http.StatusBadRequest)
		return
	}
	var h blobcache.Handle
	if _, err := hex.Decode(h.OID[:], []byte(queueIDStr)); err != nil {
		http.Error(w, "could not decode queue id", http.StatusBadRequest)
		return
	}
	secretStr := r.Header.Get("X-Secret")
	if _, err := hex.Decode(h.Secret[:], []byte(secretStr)); err != nil {
		http.Error(w, "could not decode secret", http.StatusBadRequest)
		return
	}
	switch method {
	case "Next":
		handleRequest(w, r, func(ctx context.Context, req NextReq) (*NextResp, error) {
			if req.Max < 0 {
				return nil, fmt.Errorf("max cannot be negative")
			}
			buf := make([]blobcache.Message, req.Max)
			n, err := s.Service.Next(ctx, h, buf, req.Opts)
			if err != nil {
				return nil, err
			}
			return &NextResp{Messages: buf[:n]}, nil
		})
	case "Insert":
		handleRequest(w, r, func(ctx context.Context, req InsertReq) (*blobcache.InsertResp, error) {
			resp, err := s.Service.Insert(ctx, req.From, h, req.Messages)
			if err != nil {
				return nil, err
			}
			return resp, nil
		})
	case "SubToVolume":
		handleRequest(w, r, func(ctx context.Context, req SubToVolumeReq) (*SubToVolumeResp, error) {
			if err := s.Service.SubToVolume(ctx, h, req.Volume); err != nil {
				return nil, err
			}
			return &SubToVolumeResp{}, nil
		})
	default:
		http.Error(w, fmt.Sprintf("unsupported method %v", method), http.StatusNotFound)
	}
}

func handleRequest[Req, Resp any](w http.ResponseWriter, r *http.Request, fn func(context.Context, Req) (*Resp, error)) {
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := fn(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respData, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(respData); err != nil {
		logctx.Warn(r.Context(), "writing http response", zap.Error(err))
	}
}

func getSaltFromRequest(r *http.Request) (*blobcache.CID, error) {
	saltStr := r.Header.Get("X-Salt")
	if saltStr == "" {
		return nil, nil
	}
	cid, err := blobcache.ParseCID(saltStr)
	if err != nil {
		return nil, fmt.Errorf("could not parse salt: %w", err)
	}
	return &cid, nil
}
