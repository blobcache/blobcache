package bchttp

import (
	"context"
	"net/http"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/brendoncarroll/go-state/cadata"
)

var _ blobcache.Service = &Client{}

type Client struct {
	hc       *http.Client
	endpoint string
}

func NewClient(endpoint string) *Client {
	return &Client{
		hc:       http.DefaultClient,
		endpoint: endpoint,
	}
}

func (c Client) CreatePinSet(ctx context.Context, opts blobcache.PinSetOptions) (*blobcache.PinSetHandle, error) {
	panic("not implemented")
}

func (c Client) DeletePinSet(ctx context.Context, pinset blobcache.PinSetHandle) error {
	panic("not implemented")
}

func (c Client) GetPinSet(ctx context.Context, pinset blobcache.PinSetHandle) (*blobcache.PinSet, error) {
	panic("not implemented")
}

func (c Client) Add(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID) error {
	panic("not implemented")
}

func (c Client) Delete(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID) error {
	panic("not implemented")
}

func (c Client) Post(ctx context.Context, pinset blobcache.PinSetHandle, data []byte) (cadata.ID, error) {
	panic("not implemented")
}

func (c Client) Exists(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID) (bool, error) {
	panic("not implemented")
}

func (c Client) Get(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID, buf []byte) (int, error) {
	panic("not implemented")
}

func (c Client) List(ctx context.Context, pinset blobcache.PinSetHandle, first []byte, ids []cadata.ID) (int, error) {
	panic("not implemented")
}

func (c Client) MaxBlobSize() int {
	panic("not implemented")
}
