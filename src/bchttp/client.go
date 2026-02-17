package bchttp

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
)

var _ blobcache.Service = &Client{}

type Client struct {
	hc *http.Client
	ep string
}

func NewClient(hc *http.Client, ep string) *Client {
	switch {
	case strings.HasPrefix(ep, "http://"):
		ep = strings.TrimPrefix(ep, "http://")
		if hc == nil {
			hc = http.DefaultClient
		}
	case strings.HasPrefix(ep, "tcp://"):
		ep = strings.TrimPrefix(ep, "tcp://")
		if hc == nil {
			hc = http.DefaultClient
		}
	}
	return &Client{hc: hc, ep: "http://" + ep}
}

func (c *Client) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	var req EndpointReq
	var resp EndpointResp
	if err := c.doJSON(ctx, "POST", "/Endpoint", nil, req, &resp); err != nil {
		return blobcache.Endpoint{}, err
	}
	return resp.Endpoint, nil
}

func (c *Client) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	req := InspectHandleReq{Handle: h}
	var resp InspectHandleResp
	if err := c.doJSON(ctx, "POST", "/InspectHandle", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func (c *Client) Drop(ctx context.Context, h blobcache.Handle) error {
	req := DropReq{Handle: h}
	var resp DropResp
	return c.doJSON(ctx, "POST", "/Drop", &h.Secret, req, &resp)
}

func (c *Client) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	req := KeepAliveReq{Handles: hs}
	var resp KeepAliveResp
	return c.doJSON(ctx, "POST", "/KeepAlive", nil, req, &resp)
}

func (c *Client) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	req := ShareReq{Handle: h, Peer: to, Mask: mask}
	var resp ShareResp
	if err := c.doJSON(ctx, "POST", "/Share", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func (c *Client) OpenFiat(ctx context.Context, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	req := OpenFiatReq{Target: target, Mask: mask}
	var resp OpenFiatResp
	if err := c.doJSON(ctx, "POST", "/OpenFiat", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func (c *Client) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	req := OpenFromReq{Base: base, Token: ltok, Mask: mask}
	var resp OpenFromResp
	if err := c.doJSON(ctx, "POST", "/OpenFrom", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func (c *Client) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	req := CreateVolumeReq{Host: host, Spec: vspec}
	var resp CreateVolumeResp
	if err := c.doJSON(ctx, "POST", "/volume/", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func (c *Client) CloneVolume(ctx context.Context, caller *blobcache.PeerID, vol blobcache.Handle) (*blobcache.Handle, error) {
	req := CloneVolumeReq{Volume: vol}
	var resp CloneVolumeResp
	if err := c.doJSON(ctx, "POST", "/volume/Clone", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Clone, nil
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

func (c *Client) BeginTx(ctx context.Context, vol blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	req := BeginTxReq{Volume: vol, Params: txp}
	var resp BeginTxResp
	if err := c.doJSON(ctx, "POST", "/tx/", &vol.Secret, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Tx, nil
}

func (c *Client) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	req := InspectTxReq{Tx: tx}
	var resp InspectTxResp
	if err := c.doJSON(ctx, "POST", "/tx/", &tx.Secret, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func (c *Client) Commit(ctx context.Context, tx blobcache.Handle) error {
	req := CommitReq{}
	var resp CommitResp
	return c.doJSON(ctx, "POST", c.mkTxURL(tx, "Commit"), &tx.Secret, req, &resp)
}

func (c *Client) Abort(ctx context.Context, tx blobcache.Handle) error {
	req := AbortReq{}
	var resp AbortResp
	return c.doJSON(ctx, "POST", c.mkTxURL(tx, "Abort"), &tx.Secret, req, &resp)
}

func (c *Client) Save(ctx context.Context, tx blobcache.Handle, root []byte) error {
	req := SaveReq{Root: root}
	var resp SaveResp
	return c.doJSON(ctx, "POST", c.mkTxURL(tx, "Save"), &tx.Secret, req, &resp)
}

func (c *Client) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	req := LoadReq{}
	var resp LoadResp
	if err := c.doJSON(ctx, "POST", c.mkTxURL(tx, "Load"), &tx.Secret, req, &resp); err != nil {
		return err
	}
	*dst = resp.Root
	return nil
}

func (c *Client) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	headers := map[string]string{
		"X-Secret": hex.EncodeToString(tx.Secret[:]),
	}
	if opts.Salt != nil {
		headers["X-Salt"] = opts.Salt.String()
	}
	respBody, err := c.do(ctx, "POST", c.mkTxURL(tx, "Post"), headers, data)
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

func (c *Client) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	req := ExistsReq{CIDs: cids}
	var resp ExistsResp
	if err := c.doJSON(ctx, "POST", c.mkTxURL(tx, "Exists"), &tx.Secret, req, &resp); err != nil {
		return err
	}
	copy(dst, resp.Exists)
	return nil
}

func (c *Client) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	req := DeleteReq{CIDs: cids}
	var resp DeleteResp
	return c.doJSON(ctx, "POST", c.mkTxURL(tx, "Delete"), &tx.Secret, req, &resp)
}

func (c *Client) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
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
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, fmt.Errorf("reading response: %w", err)
	}
	return n, nil
}

func (c *Client) Copy(ctx context.Context, tx blobcache.Handle, srcs []blobcache.Handle, cids []blobcache.CID, out []bool) error {
	if len(cids) != len(out) {
		return fmt.Errorf("cids and out must have the same length")
	}
	req := AddFromReq{CIDs: cids, Srcs: srcs}
	var resp AddFromResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.AddFrom", tx.OID.String()), &tx.Secret, req, &resp); err != nil {
		return err
	}
	copy(out, resp.Added)
	return nil
}

func (c *Client) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	req := VisitReq{CIDs: cids}
	var resp VisitResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Visit", tx.OID.String()), &tx.Secret, req, &resp)
}

func (c *Client) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, out []bool) error {
	req := IsVisitedReq{CIDs: cids}
	var resp IsVisitedResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.IsVisited", tx.OID.String()), &tx.Secret, req, &resp); err != nil {
		return err
	}
	copy(out, resp.Visited)
	return nil
}

func (c *Client) Link(ctx context.Context, tx blobcache.Handle, subvol blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	req := LinkReq{Target: subvol, Mask: mask}
	var resp LinkResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Link", tx.OID.String()), &tx.Secret, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Token, nil
}

func (c *Client) Unlink(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	req := UnlinkReq{Targets: targets}
	var resp UnlinkResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.Unlink", tx.OID.String()), &tx.Secret, req, &resp)
}

func (c *Client) VisitLinks(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	req := VisitLinksReq{Targets: targets}
	var resp VisitLinksResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/tx/%s.VisitLinks", tx.OID.String()), &tx.Secret, req, &resp)
}

func (c *Client) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	req := CreateQueueReq{Host: host, Spec: qspec}
	var resp CreateQueueResp
	if err := c.doJSON(ctx, "POST", "/queue/", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func (c *Client) Dequeue(ctx context.Context, q blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	req := NextReq{Opts: opts, Max: len(buf)}
	var resp NextResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/queue/%s.Dequeue", q.OID.String()), &q.Secret, req, &resp); err != nil {
		return 0, err
	}
	n := copy(buf, resp.Messages)
	return n, nil
}

func (c *Client) Enqueue(ctx context.Context, from *blobcache.Endpoint, q blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	req := InsertReq{From: from, Messages: msgs}
	var resp blobcache.InsertResp
	if err := c.doJSON(ctx, "POST", fmt.Sprintf("/queue/%s.Enqueue", q.OID.String()), &q.Secret, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) SubToVolume(ctx context.Context, q blobcache.Handle, vol blobcache.Handle) error {
	req := SubToVolumeReq{Volume: vol}
	var resp SubToVolumeResp
	return c.doJSON(ctx, "POST", fmt.Sprintf("/queue/%s.SubToVolume", q.OID.String()), &q.Secret, req, &resp)
}

func (c *Client) mkTxURL(tx blobcache.Handle, method string) string {
	return fmt.Sprintf("/tx/%s.%s", tx.OID.String(), method)
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
