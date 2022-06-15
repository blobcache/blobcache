package bcgrpc

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/brendoncarroll/go-state/cadata"
)

type client struct {
	c BlobcacheClient
}

func NewClient(c BlobcacheClient) blobcache.Service {
	return client{c: c}
}

func (c client) CreateDir(ctx context.Context, h blobcache.Handle, name string) (*blobcache.Handle, error) {
	res, err := c.c.CreateDir(ctx, &CreateDirReq{Handle: h.String(), Name: name})
	if err != nil {
		return nil, err
	}
	return dirserv.ParseHandle([]byte(res.Handle))
}

func (c client) Open(ctx context.Context, h blobcache.Handle, p []string) (*blobcache.Handle, error) {
	res, err := c.c.Open(ctx, &OpenReq{Handle: h.String(), Path: p})
	if err != nil {
		return nil, err
	}
	return dirserv.ParseHandle([]byte(res.Handle))
}

func (c client) ListEntries(ctx context.Context, h blobcache.Handle) ([]blobcache.Entry, error) {
	res, err := c.c.ListEntries(ctx, &ListEntriesReq{Handle: h.String()})
	if err != nil {
		return nil, err
	}
	var ents []blobcache.Entry
	for {
		ent, err := res.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		ents = append(ents, blobcache.Entry{Name: ent.Name, ID: dirserv.OID(ent.Oid)})
	}
	return ents, nil
}

func (c client) DeleteEntry(ctx context.Context, h blobcache.Handle, name string) error {
	_, err := c.c.DeleteEntry(ctx, &DeleteEntryReq{Handle: h.String(), Name: name})
	return err
}

func (c client) CreatePinSet(ctx context.Context, h blobcache.Handle, name string, opts blobcache.PinSetOptions) (*blobcache.Handle, error) {
	res, err := c.c.CreatePinSet(ctx, &CreatePinSetReq{Handle: h.String(), Name: name})
	if err != nil {
		return nil, err
	}
	return dirserv.ParseHandle([]byte(res.Handle))
}

func (c client) GetPinSet(ctx context.Context, h blobcache.Handle) (*blobcache.PinSet, error) {
	_, err := c.c.GetPinSet(ctx, &GetPinSetReq{Handle: h.String()})
	if err != nil {
		return nil, err
	}
	return &blobcache.PinSet{}, nil
}

func (c client) Post(ctx context.Context, h blobcache.Handle, buf []byte) (cadata.ID, error) {
	res, err := c.c.Post(ctx, &PostReq{Handle: h.String(), Data: buf})
	if err != nil {
		err = transformError(err)
		return cadata.ID{}, err
	}
	return cadata.IDFromBytes(res.Value), nil
}

func (c client) Get(ctx context.Context, h blobcache.Handle, id cadata.ID, buf []byte) (int, error) {
	res, err := c.c.Get(ctx, &GetReq{Handle: h.String(), Id: id[:]})
	if err != nil {
		return 0, err
	}
	if len(buf) < len(res.Value) {
		return 0, io.ErrShortBuffer
	}
	return copy(buf, res.Value), nil
}

func (c client) Add(ctx context.Context, h blobcache.Handle, id cadata.ID) error {
	_, err := c.c.Add(ctx, &AddReq{
		Handle: h.String(),
		Id:     id[:],
	})
	return err
}

func (c client) Delete(ctx context.Context, h blobcache.Handle, id cadata.ID) error {
	_, err := c.c.Delete(ctx, &DeleteReq{
		Handle: h.String(),
		Id:     id[:],
	})
	return err
}

func (c client) Exists(ctx context.Context, h blobcache.Handle, id cadata.ID) (bool, error) {
	res, err := c.c.List(ctx, &ListReq{Handle: h.String(), First: id[:], Limit: 1})
	if err != nil {
		return false, err
	}
	return len(res.Ids) > 0 && cadata.IDFromBytes(res.Ids[0]) == id, nil
}

func (c client) List(ctx context.Context, h blobcache.Handle, span cadata.Span, ids []cadata.ID) (int, error) {
	begin := cadata.BeginFromSpan(span)
	res, err := c.c.List(ctx, &ListReq{Handle: h.String(), First: begin[:], Limit: 1})
	if err != nil {
		return 0, err
	}
	if len(res.Ids) > len(ids) {
		return 0, errors.New("too many ids in response")
	}
	for i := range res.Ids {
		ids[i] = cadata.IDFromBytes(res.Ids[i])
	}
	return len(res.Ids), nil
}

func (c client) WaitOK(ctx context.Context, h blobcache.Handle) error {
	_, err := c.c.WaitOK(ctx, &WaitReq{Handle: h.String()})
	return err
}

func (c client) MaxSize() int {
	return blobcache.MaxSize
}

func transformError(err error) error {
	switch {
	case strings.Contains(err.Error(), cadata.ErrTooLarge.Error()):
		return cadata.ErrTooLarge
	}
	return err
}
