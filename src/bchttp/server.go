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
	case r.URL.Path == "/Await":
		handleRequest(w, r, func(ctx context.Context, req AwaitReq) (*AwaitResp, error) {
			err := s.Service.Await(ctx, req.Conditions)
			if err != nil {
				return nil, err
			}
			return &AwaitResp{}, nil
		})
	case r.URL.Path == "/KeepAlive":
		handleRequest(w, r, func(ctx context.Context, req KeepAliveReq) (*KeepAliveResp, error) {
			err := s.Service.KeepAlive(ctx, req.Targets)
			if err != nil {
				return nil, err
			}
			return &KeepAliveResp{}, nil
		})
	case r.URL.Path == "/Anchor":
		handleRequest(w, r, func(ctx context.Context, req AnchorReq) (*AnchorResp, error) {
			err := s.Service.Anchor(ctx, req.Target)
			if err != nil {
				return nil, err
			}
			return &AnchorResp{}, nil
		})
	case r.URL.Path == "/Drop":
		handleRequest(w, r, func(ctx context.Context, req DropReq) (*DropResp, error) {
			err := s.Service.Drop(ctx, req.Target)
			if err != nil {
				return nil, err
			}
			return &DropResp{}, nil
		})
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
			vol, err := s.Service.CreateVolume(ctx, req.Spec)
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
			txh, err := s.Service.BeginTx(ctx, req.Volume, req.Mutate)
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
	case "Commit":
		handleRequest(w, r, func(ctx context.Context, req CommitReq) (*CommitResp, error) {
			if err := s.Service.Commit(ctx, h, req.Root); err != nil {
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
		cid, err := s.Service.Post(r.Context(), h, salt, data)
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
		n, err := s.Service.Get(r.Context(), h, req.CID, req.Salt, buf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(buf[:n])
		return
	case "Load":
		handleRequest(w, r, func(ctx context.Context, req LoadReq) (*LoadResp, error) {
			var root []byte
			if err := s.Service.Load(ctx, h, &root); err != nil {
				return nil, err
			}
			return &LoadResp{Root: root}, nil
		})
	case "Delete":
		handleRequest(w, r, func(ctx context.Context, req DeleteReq) (*DeleteResp, error) {
			if err := s.Service.Delete(ctx, h, req.CID); err != nil {
				return nil, err
			}
			return &DeleteResp{}, nil
		})
	case "Exists":
		handleRequest(w, r, func(ctx context.Context, req ExistsReq) (*ExistsResp, error) {
			exists, err := s.Service.Exists(ctx, h, req.CID)
			if err != nil {
				return nil, err
			}
			return &ExistsResp{Exists: exists}, nil
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

func setSaltHeader(req *http.Request, salt *blobcache.CID) {
	if salt == nil {
		return
	}
	b64, _ := salt.MarshalBase64()
	req.Header.Set("X-Salt", string(b64))
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
