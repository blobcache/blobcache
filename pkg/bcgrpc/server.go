package bcgrpc

import (
	"context"
	"errors"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type server struct {
	s blobcache.Service
	UnimplementedBlobcacheServer
}

func NewServer(s blobcache.Service) BlobcacheServer {
	return &server{
		s: s,
	}
}

func (s *server) CreateDir(ctx context.Context, req *CreateDirReq) (*HandleRes, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	h2, err := s.s.CreateDir(ctx, *h, req.GetName())
	if err != nil {
		return nil, err
	}
	return &HandleRes{Handle: h2.String()}, nil
}

func (s *server) Open(ctx context.Context, req *OpenReq) (*HandleRes, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	h2, err := s.s.Open(ctx, *h, req.GetPath())
	if err != nil {
		return nil, err
	}
	return &HandleRes{Handle: h2.String()}, nil
}

func (s *server) ListEntries(req *ListEntriesReq, srv Blobcache_ListEntriesServer) error {
	ctx := srv.Context()
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return err
	}
	ents, err := s.s.ListEntries(ctx, *h)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		if err := srv.Send(&Entry{
			Name: ent.Name,
			Oid:  uint64(ent.ID),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) DeleteEntry(ctx context.Context, req *DeleteEntryReq) (*empty.Empty, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	if err := s.s.DeleteEntry(ctx, *h, req.GetName()); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *server) CreatePinSet(ctx context.Context, req *CreatePinSetReq) (*HandleRes, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	h2, err := s.s.CreatePinSet(ctx, *h, req.GetName(), blobcache.PinSetOptions{})
	if err != nil {
		return nil, err
	}
	return &HandleRes{Handle: h2.String()}, nil
}

func (s *server) GetPinSet(ctx context.Context, req *GetPinSetReq) (*PinSet, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	_, err = s.s.GetPinSet(ctx, *h)
	if err != nil {
		return nil, err
	}
	return &PinSet{}, nil
}

func (s *server) Post(ctx context.Context, req *PostReq) (*wrapperspb.BytesValue, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	id, err := s.s.Post(ctx, *h, req.Data)
	if err != nil {
		return nil, err
	}
	return wrapperspb.Bytes(id[:]), nil
}

func (s *server) Get(ctx context.Context, req *GetReq) (*wrapperspb.BytesValue, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	id := cadata.IDFromBytes(req.Id)
	buf := make([]byte, s.s.MaxSize())
	n, err := s.s.Get(ctx, *h, id, buf)
	if err != nil {
		return nil, err
	}
	return wrapperspb.Bytes(buf[:n]), nil
}

func (s *server) Add(ctx context.Context, req *AddReq) (*empty.Empty, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	id := cadata.IDFromBytes(req.Id)
	if err := s.s.Add(ctx, *h, id); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *server) Delete(ctx context.Context, req *DeleteReq) (*empty.Empty, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	id := cadata.IDFromBytes(req.Id)
	if err := s.s.Delete(ctx, *h, id); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *server) List(ctx context.Context, req *ListReq) (*ListRes, error) {
	h, err := dirserv.ParseHandle([]byte(req.GetHandle()))
	if err != nil {
		return nil, err
	}
	limit := 100
	if int(req.Limit) < limit {
		limit = int(req.Limit)
	}
	ids := make([]cadata.ID, limit)
	n, err := s.s.List(ctx, *h, cadata.IDFromBytes(req.First), ids)
	if err != nil && !errors.Is(err, cadata.ErrEndOfList) {
		return nil, err
	}
	var end bool
	if errors.Is(err, cadata.ErrEndOfList) {
		end = true
	}
	ids2 := make([][]byte, n)
	for i := range ids[:n] {
		ids2[i] = ids[i][:]
	}
	return &ListRes{Ids: ids2, End: end}, nil
}
