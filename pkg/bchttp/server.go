package bchttp

import (
	"context"
	"encoding/base64"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobcache"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/go-chi/chi"
)

type Server struct {
	n     *blobcache.Node
	r     chi.Router
	hs    http.Server
	laddr string
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
		laddr: laddr,
	}
	r := chi.NewRouter()

	r.Route("/s", func(r chi.Router) {
		r.Post("/", s.createPinSet)

		r.Put("/{pinSetName}", s.addPin)
		r.Get("/{pinSetName}/{blobID}", s.getBlob)
		r.Delete("/{pinSetName}/{blobID}", s.deletePin)
	})

	r.Get("/{blobID}", s.getBlob)

	s.r = r
	s.hs.Handler = s.r
	return s
}

func (s *Server) Run(ctx context.Context) error {
	return s.hs.ListenAndServe()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.post(w, r)
	case http.MethodGet:
		s.getBlob(w, r)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (s *Server) post(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	id, err := s.n.Post(ctx, "", buf[:total])
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

func (s *Server) addPin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pinSetName, ok := ctx.Value("pinSetName").(string)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	idb64, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	idBytes := make([]byte, base64.URLEncoding.DecodedLen(len(idb64)))
	n, err := base64.URLEncoding.Decode(idBytes, idb64)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	idBytes = idBytes[:n]
	id := blobs.ID{}
	copy(id[:], idBytes)
	if err := s.n.Pin(r.Context(), pinSetName, id); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getBlob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr, ok := ctx.Value("blobID").(string)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id := blobs.ID{}
	idBytes, err := base64.URLEncoding.DecodeString(idStr)
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

func (s *Server) createPinSet(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	ctx := r.Context()
	if err := s.n.CreatePinSet(ctx, string(data)); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deletePin(w http.ResponseWriter, r *http.Request) {

}
