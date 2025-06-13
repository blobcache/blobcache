package bchttp

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"blobcache.io/blobcache/src/blobcache"
)

var _ blobcache.Service = &Client{}

type Client struct {
	hc *http.Client
	ep string
}

func NewClient(hc *http.Client, ep string) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{hc: hc, ep: ep}
}

func (c *Client) CreateVolume(ctx context.Context, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	req := CreateVolumeReq{Spec: vspec}
	var resp CreateVolumeResp
	if err := c.doJSON(ctx, "POST", "/volume/", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func (c *Client) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	p := fmt.Sprintf("/volume/%s.Inspect", h.OID.String())
	headers := map[string]string{
		"X-Secret": hex.EncodeToString(h.Secret[:]),
	}
	body, err := c.do(ctx, "GET", p, headers, nil)
	if err != nil {
		return nil, err
	}
	var info blobcache.VolumeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("unmarshaling response: %w", err)
	}
	return &info, nil
}

func (c *Client) Anchor(ctx context.Context, h blobcache.Handle) error {
	req := AnchorReq{Target: h}
	var resp AnchorResp
	return c.doJSON(ctx, "POST", "/Anchor", &h.Secret, req, &resp)
}

func (c *Client) Drop(ctx context.Context, h blobcache.Handle) error {
	req := DropReq{Target: h}
	var resp DropResp
	return c.doJSON(ctx, "POST", "/Drop", &h.Secret, req, &resp)
}

func (c *Client) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	req := KeepAliveReq{Targets: hs}
	var resp KeepAliveResp
	return c.doJSON(ctx, "POST", "/KeepAlive", nil, req, &resp)
}

func (c *Client) Await(ctx context.Context, cond blobcache.Conditions) error {
	req := AwaitReq{Conditions: cond}
	var resp AwaitResp
	return c.doJSON(ctx, "POST", "/Await", nil, req, &resp)
}

func (c *Client) BeginTx(ctx context.Context, vol blobcache.Handle, mutate bool) (*blobcache.Handle, error) {
	req := BeginTxReq{Volume: vol, Mutate: mutate}
	var resp BeginTxResp
	if err := c.doJSON(ctx, "POST", "/tx/", &vol.Secret, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Tx, nil
}

func (c *Client) Commit(ctx context.Context, tx blobcache.Handle, root []byte) error {
	req := CommitReq{Root: root}
	var resp CommitResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Commit", tx.OID.String()), &tx.Secret, req, &resp)
}

func (c *Client) Abort(ctx context.Context, tx blobcache.Handle) error {
	req := AbortReq{}
	var resp AbortResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Abort", tx.OID.String()), &tx.Secret, req, &resp)
}

func (c *Client) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	req := LoadReq{}
	var resp LoadResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Load", tx.OID.String()), &tx.Secret, req, &resp); err != nil {
		return err
	}
	*dst = resp.Root
	return nil
}

func (c *Client) Post(ctx context.Context, tx blobcache.Handle, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	headers := map[string]string{
		"X-Secret": hex.EncodeToString(tx.Secret[:]),
	}
	if salt != nil {
		headers["X-Salt"] = salt.String()
	}
	respBody, err := c.do(ctx, "POST", fmt.Sprintf("/tx/%s.Post", tx.OID.String()), headers, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	var cid blobcache.CID
	if len(respBody) != len(cid) {
		return blobcache.CID{}, fmt.Errorf("invalid CID length: got %d, want %d", len(respBody), len(cid))
	}
	copy(cid[:], respBody)
	return cid, nil
}

func (c *Client) Exists(ctx context.Context, tx blobcache.Handle, cid blobcache.CID) (bool, error) {
	req := ExistsReq{CID: cid}
	var resp ExistsResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Exists", tx.OID.String()), &tx.Secret, req, &resp); err != nil {
		return false, err
	}
	return resp.Exists, nil
}

func (c *Client) Delete(ctx context.Context, tx blobcache.Handle, cid blobcache.CID) error {
	req := DeleteReq{CID: cid}
	var resp DeleteResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Delete", tx.OID.String()), &tx.Secret, req, &resp)
}

func (c *Client) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	req := GetReq{CID: cid}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshaling request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/tx/%s.Get", c.ep, tx.OID.String()), bytes.NewReader(reqBody))
	if err != nil {
		return 0, fmt.Errorf("creating request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Secret", hex.EncodeToString(tx.Secret[:]))

	httpResp, err := c.hc.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("sending request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return 0, fmt.Errorf("request failed with status %d: %s", httpResp.StatusCode, string(body))
	}

	n, err := io.ReadFull(httpResp.Body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, fmt.Errorf("reading response: %w", err)
	}
	return n, nil
}

func (c *Client) do(ctx context.Context, method, path string, headers map[string]string, reqBody []byte) ([]byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, method, c.ep+path, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}
	httpResp, err := c.hc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("request failed with status %d: %s", httpResp.StatusCode, string(body))
	}
	return io.ReadAll(httpResp.Body)
}

func (c *Client) doJSON(ctx context.Context, method, path string, secret *[16]byte, req, resp interface{}) error {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshaling request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, c.ep+path, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if secret != nil {
		httpReq.Header.Set("X-Secret", hex.EncodeToString(secret[:]))
	}

	httpResp, err := c.hc.Do(httpReq)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("request failed with status %d: %s", httpResp.StatusCode, string(body))
	}

	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}
