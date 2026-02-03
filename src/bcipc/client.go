package bcipc

import (
	"context"
	"fmt"
	"net"
	"runtime"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/pools"
)

var _ blobcache.Service = &Client{}

type Client struct {
	raddr net.UnixAddr

	pool pools.OpenClose[*net.UnixConn]
	tp   clientTransport
}

func NewClient(sockPath string) *Client {
	raddr := net.UnixAddr{Name: sockPath}
	open := func(ctx context.Context) (*net.UnixConn, error) {
		return net.DialUnix("", &net.UnixAddr{}, &raddr)
	}
	close := func(conn *net.UnixConn) error {
		return conn.Close()
	}
	pool := pools.NewOpenClose[*net.UnixConn](runtime.GOMAXPROCS(0), open, close)
	return &Client{
		raddr: raddr,
		pool:  pool,
		tp:    clientTransport{pool: &pool},
	}
}

func (c *Client) Close() error {
	return c.pool.CloseAll()
}

// Endpoint returns the endpoint of the remote Client.
func (c *Client) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return bcp.Endpoint(ctx, &c.tp)
}

func (c *Client) Drop(ctx context.Context, h blobcache.Handle) error {
	return bcp.Drop(ctx, &c.tp, blobcache.Endpoint{}, h)
}

func (c *Client) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return bcp.KeepAlive(ctx, &c.tp, blobcache.Endpoint{}, hs)
}

func (c *Client) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return bcp.Share(ctx, &c.tp, blobcache.Endpoint{}, h, to, mask)
}

func (c *Client) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return bcp.InspectHandle(ctx, &c.tp, blobcache.Endpoint{}, h)
}

func (c *Client) OpenFiat(ctx context.Context, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	h, _, err := bcp.OpenFiat(ctx, &c.tp, blobcache.Endpoint{}, target, mask)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (s *Client) OpenFrom(ctx context.Context, base blobcache.Handle, token blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	h, c, err := bcp.OpenFrom(ctx, &c.tp, blobcache.Endpoint{}, base, token, mask)
	return h, err
}

func (s *Client) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	h, info, err := bcp.BeginTx(ctx, &c.tp, blobcache.Endpoint{}, volh, txp)
	if err != nil {
		return nil, err
	}
	s.cache.Add(h.OID, info)
	return h, nil
}

// CreateVolume creates a new volume.
func (s *Client) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if host != nil && *host != s.ep {
		return nil, fmt.Errorf("bcremote: caller cannot be different from the node ID")
	}
	return bcp.CreateVolume(ctx, &c.tp, blobcache.Endpoint{}, vspec)
}

// InspectVolume returns info about a Volume.
func (c *Client) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return bcp.InspectVolume(ctx, &c.tp, blobcache.Endpoint{}, h)
}

func (s *Client) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	return bcp.CloneVolume(ctx, &c.tp, blobcache.Endpoint{}, caller, volh)
}

// InspectTx returns info about a transaction.
func (s *Client) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return bcp.InspectTx(ctx, &c.tp, blobcache.Endpoint{}, tx)
}

// Commit commits a transaction.
func (s *Client) Commit(ctx context.Context, tx blobcache.Handle) error {
	return bcp.Commit(ctx, &c.tp, blobcache.Endpoint{}, tx, nil)
}

// Abort aborts a transaction.
func (s *Client) Abort(ctx context.Context, tx blobcache.Handle) error {
	return bcp.Abort(ctx, &c.tp, blobcache.Endpoint{}, tx)
}

// Load loads the volume root into dst
func (s *Client) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return bcp.Load(ctx, &c.tp, blobcache.Endpoint{}, tx, dst)
}

// Save writes to the volume root.
func (s *Client) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return bcp.Save(ctx, &c.tp, blobcache.Endpoint{}, tx, src)
}

// Post posts data to the volume
func (s *Client) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return bcp.Post(ctx, &c.tp, blobcache.Endpoint{}, tx, opts.Salt, data)
}

// Exists checks if a CID exists in the volume
func (s *Client) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	return bcp.Exists(ctx, &c.tp, blobcache.Endpoint{}, tx, cids, dst)
}

// Delete deletes a CID from the volume
func (c *Client) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcp.Delete(ctx, &c.tp, blobcache.Endpoint{}, tx, cids)
}

func (s *Client) Copy(ctx context.Context, tx blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, success []bool) error {
	if len(cids) != len(success) {
		return fmt.Errorf("cids and success must have the same length")
	}
	return bcp.AddFrom(ctx, &c.tp, blobcache.Endpoint{}, tx, cids, srcTxns, success)
}

// Get returns the data for a CID.
func (s *Client) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	hf, err := s.getHashFunc(ctx, tx)
	if err != nil {
		return c, err
	}
	return bcp.Get(ctx, &c.tp, blobcache.Endpoint{}, tx, hf, cid, opts.Salt, buf)
}

func (c *Client) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcp.Visit(ctx, &c.tp, blobcache.Endpoint{}, tx, cids)
}

func (s *Client) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return bcp.IsVisited(ctx, &c.tp, blobcache.Endpoint{}, tx, cids, dst)
}

// Link allows the Volume to reference another volume.
func (c *Client) Link(ctx context.Context, tx blobcache.Handle, subvol blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	return bcp.Link(ctx, &c.tp, blobcache.Endpoint{}, tx, subvol, mask)
}

func (c *Client) Unlink(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	return bcp.Unlink(ctx, &c.tp, blobcache.Endpoint{}, tx, targets)
}

func (c *Client) VisitLinks(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	return bcp.VisitLinks(ctx, &c.tp, blobcache.Endpoint{}, tx, targets)
}

func (c *Client) CreateQueue(ctx context.Context, _ *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	return bcp.CreateQueue(ctx, &c.tp, blobcache.Endpoint{}, qspec)
}

func (c *Client) Next(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.NextOpts) (int, error) {
	return bcp.Next(ctx, &c.tp, blobcache.Endpoint{}, qh, buf, opts)
}

func (c *Client) Insert(ctx context.Context, from *blobcache.Endpoint, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return bcp.Insert(ctx, c.node, c.ep, from, qh, msgs)
}

func (c *Client) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle) error {
	return bcp.SubToVolume(ctx, c.node, c.ep, qh, volh)
}

// getHashFunc finds the hash function for a transaction.
func (s *Client) getHashFunc(ctx context.Context, txh blobcache.Handle) (blobcache.HashFunc, error) {
	txinfo, ok := s.cache.Get(txh.OID)
	if ok {
		return txinfo.HashAlgo.HashFunc(), nil
	}
	txinfo, err := bcp.InspectTx(ctx, &c.tp, blobcache.Endpoint{}, txh)
	if err != nil {
		return nil, err
	}
	s.cache.Add(txh.OID, txinfo)
	return txinfo.HashAlgo.HashFunc(), nil
}
