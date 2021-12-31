package bchttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"

	"github.com/blobcache/blobcache/pkg/blobcache"
)

const endOfList = "END OF LIST"

var _ blobcache.Service = &Client{}

type Client struct {
	hc       *http.Client
	endpoint string
}

func NewClient(endpoint string) *Client {
	return &Client{
		hc:       http.DefaultClient,
		endpoint: strings.Trim(endpoint, "/"),
	}
}

func (c Client) CreatePinSet(ctx context.Context, opts blobcache.PinSetOptions) (*blobcache.PinSetHandle, error) {
	var psh blobcache.PinSetHandle
	if err := c.jsonRPC(ctx, http.MethodPost, "/s", nil, opts, &psh); err != nil {
		return nil, err
	}
	return &psh, nil
}

func (c Client) GetPinSet(ctx context.Context, pinset blobcache.PinSetHandle) (*blobcache.PinSet, error) {
	u, headers := makePinSetURLHeaders(pinset)
	var psh blobcache.PinSet
	if err := c.jsonRPC(ctx, http.MethodGet, u, headers, nil, &psh); err != nil {
		return nil, err
	}
	return &psh, nil
}

func (c Client) DeletePinSet(ctx context.Context, pinset blobcache.PinSetHandle) error {
	u, headers := makePinSetURLHeaders(pinset)
	if _, err := c.unaryRequest(ctx, http.MethodPost, u, headers, nil); err != nil {
		return err
	}
	return nil
}

func (c Client) Add(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID) error {
	u, headers := makePinSetURLHeaders(pinset)
	u += "/" + id.String()
	if _, err := c.unaryRequest(ctx, http.MethodPut, u, headers, nil); err != nil {
		return err
	}
	return nil
}

func (c Client) Delete(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID) error {
	u, headers := makePinSetURLHeaders(pinset)
	u += "/" + id.String()
	if _, err := c.unaryRequest(ctx, http.MethodDelete, u, headers, nil); err != nil {
		return err
	}
	return nil
}

func (c Client) Post(ctx context.Context, pinset blobcache.PinSetHandle, data []byte) (cadata.ID, error) {
	if len(data) > c.MaxSize() {
		return cadata.ID{}, cadata.ErrTooLarge
	}
	u, headers := makePinSetURLHeaders(pinset)
	resData, err := c.unaryRequest(ctx, http.MethodPost, u, headers, data)
	if err != nil {
		return cadata.ID{}, err
	}
	id := cadata.ID{}
	return id, id.UnmarshalBase64(resData)
}

func (c Client) Exists(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID) (bool, error) {
	ids := [1]cadata.ID{}
	n, err := c.List(ctx, pinset, id[:], ids[:])
	if err != nil && err != cadata.ErrEndOfList {
		return false, err
	}
	if n == 1 && ids[0] == id {
		return true, nil
	}
	return false, nil
}

func (c Client) Get(ctx context.Context, pinset blobcache.PinSetHandle, id cadata.ID, buf []byte) (int, error) {
	u, headers := makePinSetURLHeaders(pinset)
	u += "/" + base64.URLEncoding.EncodeToString(id[:])
	rc, err := c.streamResponse(ctx, http.MethodGet, u, headers, nil)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, buf)
	if err == io.ErrUnexpectedEOF {
		err = nil
	}
	return n, err
}

func (c Client) List(ctx context.Context, pinset blobcache.PinSetHandle, first []byte, ids []cadata.ID) (int, error) {
	u, headers := makePinSetURLHeaders(pinset)
	u += "/list?" + url.Values{
		"first": []string{base64.URLEncoding.EncodeToString(first)},
		"limit": []string{strconv.Itoa(len(ids))},
	}.Encode()
	rc, err := c.streamResponse(ctx, http.MethodGet, u, headers, nil)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	sc := bufio.NewScanner(rc)
	var n int
	var eol bool
	for sc.Scan() && n < len(ids) {
		if bytes.Equal([]byte(endOfList), sc.Bytes()) {
			eol = true
			break
		}
		if err := ids[n].UnmarshalBase64(sc.Bytes()); err != nil {
			return n, err
		}
		n++
	}
	if sc.Err() != nil {
		return n, err
	}
	var retErr error
	if eol {
		retErr = cadata.ErrEndOfList
	}
	return n, retErr
}

func (c Client) WaitOK(ctx context.Context, pinset blobcache.PinSetHandle) error {
	panic("not implemented")
}

func (c Client) MaxSize() int {
	return blobcache.MaxSize
}

func (c Client) jsonRPC(ctx context.Context, method, path string, headers map[string]string, req, res interface{}) error {
	var data []byte
	if req != nil {
		var err error
		data, err = json.Marshal(req)
		if err != nil {
			return err
		}
	}
	resData, err := c.unaryRequest(ctx, method, path, headers, data)
	if err != nil {
		return err
	}
	if res != nil {
		return json.Unmarshal(resData, res)
	}
	return nil
}

func (c Client) unaryRequest(ctx context.Context, method, path string, headers map[string]string, body []byte) ([]byte, error) {
	rc, err := c.streamBoth(ctx, method, path, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (c Client) streamResponse(ctx context.Context, method, path string, headers map[string]string, body []byte) (io.ReadCloser, error) {
	return c.streamBoth(ctx, method, path, headers, bytes.NewReader(body))
}

func (c Client) streamBoth(ctx context.Context, method, path string, headers map[string]string, body io.Reader) (io.ReadCloser, error) {
	res, err := c.do(ctx, method, path, headers, body)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (c Client) do(ctx context.Context, method, path string, headers map[string]string, body io.Reader) (*http.Response, error) {
	u := c.endpoint + path
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		data, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		return nil, errors.Errorf("non-OK status: %s %q", res.Status, data)
	}
	return res, nil
}

func makePinSetURLHeaders(psh blobcache.PinSetHandle) (string, map[string]string) {
	headers := map[string]string{
		headerHandleSecret: base64.URLEncoding.EncodeToString(psh.Secret[:]),
	}
	return fmt.Sprintf("/s/%d", psh.ID), headers
}
