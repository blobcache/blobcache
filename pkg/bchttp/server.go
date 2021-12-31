package bchttp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/blobcache/blobcache/pkg/bcpool"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const headerHandleSecret = "X-Handle-Secret"

type Server struct {
	n blobcache.Service
	r chi.Router
}

func NewServer(n blobcache.Service, log *logrus.Logger) *Server {
	s := &Server{n: n}
	r := chi.NewRouter()
	r.Route("/s", func(r chi.Router) {
		r.Post("/", unaryHandler(log, s.createPinSet))
		r.Get("/{pinSetID}", unaryHandler(log, s.getPinSet))

		r.Post("/{pinSetID}", unaryHandler(log, s.post))
		r.Get("/{pinSetID}/{blobID}", streamHandler(log, s.getBlob))
		r.Get("/{pinSetID}/list", unaryHandler(log, s.list))
		r.Put("/{pinSetID}/{blobID}", unaryHandler(log, s.addPin))
		r.Delete("/{pinSetID}/{blobID}", unaryHandler(log, s.deletePin))
	})
	s.r = r
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.r.ServeHTTP(w, r)
}

func (s *Server) createPinSet(r *http.Request) ([]byte, error) {
	ctx := r.Context()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var opts blobcache.PinSetOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		return nil, err
	}
	psh, err := s.n.CreatePinSet(ctx, opts)
	if err != nil {
		return nil, err
	}
	return json.Marshal(psh)
}

func (s *Server) getPinSet(r *http.Request) ([]byte, error) {
	ctx := r.Context()
	psh, err := getHandle(r)
	if err != nil {
		return nil, err
	}
	pinset, err := s.n.GetPinSet(ctx, *psh)
	if err != nil {
		return nil, err
	}
	return json.Marshal(pinset)
}

func (s *Server) post(r *http.Request) ([]byte, error) {
	ctx := r.Context()
	maxSize := s.n.MaxSize()
	total := 0
	buf := make([]byte, maxSize)
	for total < maxSize {
		n, err := r.Body.Read(buf[total:])
		total += int(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	psh, err := getHandle(r)
	if err != nil {
		return nil, err
	}
	id, err := s.n.Post(ctx, *psh, buf[:total])
	if err != nil {
		return nil, err
	}
	return id.MarshalBase64()
}

func (s *Server) getBlob(w io.Writer, r *http.Request) error {
	ctx := r.Context()
	psh, err := getHandle(r)
	if err != nil {
		return err
	}
	blobID, err := getBlobID(r)
	if err != nil {
		return err
	}
	buf := bcpool.Acquire()
	defer bcpool.Release(buf)
	n, err := s.n.Get(ctx, *psh, blobID, buf[:])
	if err != nil {
		return err
	}
	_, err = w.Write(buf[:n])
	return err
}

func (s *Server) addPin(r *http.Request) ([]byte, error) {
	ctx := r.Context()
	psh, err := getHandle(r)
	if err != nil {
		return nil, err
	}
	blobID, err := getBlobID(r)
	if err != nil {
		return nil, err
	}
	if err := s.n.Add(ctx, *psh, blobID); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Server) deletePin(r *http.Request) ([]byte, error) {
	return nil, nil
}

func (s *Server) list(r *http.Request) ([]byte, error) {
	ctx := r.Context()
	psh, err := getHandle(r)
	if err != nil {
		return nil, err
	}
	first, err := base64.URLEncoding.DecodeString(r.URL.Query().Get("first"))
	if err != nil {
		return nil, err
	}
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err != nil {
			return nil, err
		}
		if l < 1024 {
			limit = l
		}
	}
	ids := make([]cadata.ID, limit)
	n, err := s.n.List(ctx, *psh, first, ids)
	if err != nil && err != cadata.ErrEndOfList {
		return nil, err
	}
	buf := bytes.Buffer{}
	for _, id := range ids[:n] {
		data, _ := id.MarshalBase64()
		buf.Write(data)
		buf.WriteString("\n")
	}
	if err == cadata.ErrEndOfList {
		buf.WriteString(endOfList + "\n")
	}
	return buf.Bytes(), nil
}

func getHandle(r *http.Request) (*blobcache.PinSetHandle, error) {
	idStr := chi.URLParam(r, "pinSetID")
	idInt, err := strconv.Atoi(string(idStr))
	if err != nil {
		return nil, err
	}
	var psh blobcache.PinSetHandle
	psh.ID = blobcache.PinSetID(idInt)
	if _, err := base64.URLEncoding.Decode(psh.Secret[:], []byte(r.Header.Get(headerHandleSecret))); err != nil {
		return nil, err
	}
	return &psh, nil
}

func getBlobID(r *http.Request) (cadata.ID, error) {
	idStr := chi.URLParam(r, "blobID")
	data, err := base64.URLEncoding.DecodeString(idStr)
	if err != nil {
		return cadata.ID{}, err
	}
	return cadata.IDFromBytes(data), nil
}

func streamHandler(log *logrus.Logger, fn func(w io.Writer, r *http.Request) error) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			log.Warn(err)
			w.WriteHeader(codeForError(err))
			w.Write([]byte(err.Error()))
			return
		}
	})
}

func unaryHandler(log *logrus.Logger, fn func(r *http.Request) ([]byte, error)) http.HandlerFunc {
	return streamHandler(log, func(w io.Writer, r *http.Request) error {
		resData, err := fn(r)
		if err != nil {
			return err
		}
		if _, err := w.Write(resData); err != nil {
			log.Error(err)
		}
		return nil
	})
}

func codeForError(err error) int {
	switch {
	case errors.Is(err, cadata.ErrNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
