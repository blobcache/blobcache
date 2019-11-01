package bchttp

import (
	"context"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobcache"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
)

type Server struct {
	n     *blobcache.Node
	hs    http.Server
	laddr string
	ctx   context.Context
}

func NewServer(n *blobcache.Node, laddr string) *Server {
	s := &Server{
		n: n,
		hs: http.Server{
			Addr:           laddr,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
	}
	s.hs.Handler = s
	return s
}

func (s *Server) Run(ctx context.Context) error {
	s.ctx = ctx
	return s.hs.ListenAndServe()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := s.ctx
	switch r.Method {
	case http.MethodPost:
		s.post(ctx, w, r)
	case http.MethodGet:
		s.get(ctx, w, r)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (s *Server) post(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	maxSize := s.n.MaxBlobSize()

	total := 0
	buf := make([]byte, maxSize)

	for total < maxSize {
		n, err := r.Body.Read(buf[total:])
		total += int(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	id, err := s.n.Post(ctx, buf[:total])
	if err != nil {
		log.Println(err)
		return
	}

	idb64 := make([]byte, base64.URLEncoding.EncodedLen(len(id)))
	base64.URLEncoding.Encode(idb64, id[:])

	_, err = w.Write(idb64)
	if err != nil {
		log.Println(err)
	}
}

func (s *Server) get(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if len(p) < 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id := blobs.ID{}
	idBytes, err := base64.URLEncoding.DecodeString(p[1:])
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	copy(id[:], idBytes[:])
	b, err := s.n.Get(ctx, id)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if b == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}
