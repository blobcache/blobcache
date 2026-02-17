package bcipc

import (
	"context"
	"fmt"
	"net"
	"runtime"

	lru "github.com/hashicorp/golang-lru/v2"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/pools"
)

var _ blobcache.Service = &Client{}

type Client struct {
	raddr net.UnixAddr

	pool pools.OpenClose[*net.UnixConn]
	tp   clientTransport

	cache *lru.Cache[blobcache.OID, *blobcache.TxInfo]
}

func NewClient(sockPath string) *Client {
	raddr := net.UnixAddr{Name: sockPath, Net: "unix"}
	open := func(ctx context.Context) (*net.UnixConn, error) {
		return net.DialUnix("unix", nil, &raddr)
	}
	closeConn := func(conn *net.UnixConn) error {
		return conn.Close()
	}
	pool := pools.NewOpenClose[*net.UnixConn](runtime.GOMAXPROCS(0), open, closeConn)
	cache, _ := lru.New[blobcache.OID, *blobcache.TxInfo](128)
	return &Client{
		raddr: raddr,
		pool:  pool,
		tp:    clientTransport{pool: &pool},
		cache: cache,
	}
}

func (c *Client) Close() error {
	return c.pool.CloseAll()
}

// Endpoint returns the endpoint of the remote service.
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

func (c *Client) OpenFrom(ctx context.Context, base blobcache.Handle, token blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	h, _, err := bcp.OpenFrom(ctx, &c.tp, blobcache.Endpoint{}, base, token, mask)
	return h, err
}

func (c *Client) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	h, info, err := bcp.BeginTx(ctx, &c.tp, blobcache.Endpoint{}, volh, txp)
	if err != nil {
		return nil, err
	}
	c.cache.Add(h.OID, info)
	return h, nil
}

// CreateVolume creates a new volume.
func (c *Client) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if host != nil {
		ep, err := c.Endpoint(ctx)
		if err != nil {
			return nil, err
		}
		if *host != ep {
			return nil, fmt.Errorf("bcipc: caller cannot be different from the node ID")
		}
	}
	return bcp.CreateVolume(ctx, &c.tp, blobcache.Endpoint{}, vspec)
}

// InspectVolume returns info about a Volume.
func (c *Client) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return bcp.InspectVolume(ctx, &c.tp, blobcache.Endpoint{}, h)
}

func (c *Client) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	return bcp.CloneVolume(ctx, &c.tp, blobcache.Endpoint{}, caller, volh)
}

// InspectTx returns info about a transaction.
func (c *Client) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return bcp.InspectTx(ctx, &c.tp, blobcache.Endpoint{}, tx)
}

// Commit commits a transaction.
func (c *Client) Commit(ctx context.Context, tx blobcache.Handle) error {
	return bcp.Commit(ctx, &c.tp, blobcache.Endpoint{}, tx, nil)
}

// Abort aborts a transaction.
func (c *Client) Abort(ctx context.Context, tx blobcache.Handle) error {
	return bcp.Abort(ctx, &c.tp, blobcache.Endpoint{}, tx)
}

// Load loads the volume root into dst
func (c *Client) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return bcp.Load(ctx, &c.tp, blobcache.Endpoint{}, tx, dst)
}

// Save writes to the volume root.
func (c *Client) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return bcp.Save(ctx, &c.tp, blobcache.Endpoint{}, tx, src)
}

// Post posts data to the volume
func (c *Client) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return bcp.Post(ctx, &c.tp, blobcache.Endpoint{}, tx, opts.Salt, data)
}

// Exists checks if a CID exists in the volume
func (c *Client) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	return bcp.Exists(ctx, &c.tp, blobcache.Endpoint{}, tx, cids, dst)
}

// Delete deletes a CID from the volume
func (c *Client) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcp.Delete(ctx, &c.tp, blobcache.Endpoint{}, tx, cids)
}

func (c *Client) Copy(ctx context.Context, tx blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, success []bool) error {
	if len(cids) != len(success) {
		return fmt.Errorf("cids and success must have the same length")
	}
	return bcp.AddFrom(ctx, &c.tp, blobcache.Endpoint{}, tx, cids, srcTxns, success)
}

// Get returns the data for a CID.
func (c *Client) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	hf, err := c.getHashFunc(ctx, tx)
	if err != nil {
		return 0, err
	}
	return bcp.Get(ctx, &c.tp, blobcache.Endpoint{}, tx, hf, cid, opts.Salt, buf)
}

func (c *Client) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcp.Visit(ctx, &c.tp, blobcache.Endpoint{}, tx, cids)
}

func (c *Client) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
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

func (c *Client) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	return bcp.InspectQueue(ctx, &c.tp, blobcache.Endpoint{}, qh)
}

func (c *Client) Dequeue(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return bcp.Dequeue(ctx, &c.tp, blobcache.Endpoint{}, qh, buf, opts)
}

func (c *Client) Enqueue(ctx context.Context, from *blobcache.Endpoint, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return bcp.Enqueue(ctx, &c.tp, blobcache.Endpoint{}, from, qh, msgs)
}

func (c *Client) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle) error {
	return bcp.SubToVolume(ctx, &c.tp, blobcache.Endpoint{}, qh, volh)
}

// getHashFunc finds the hash function for a transaction.
func (c *Client) getHashFunc(ctx context.Context, txh blobcache.Handle) (blobcache.HashFunc, error) {
	txinfo, ok := c.cache.Get(txh.OID)
	if ok {
		return txinfo.HashAlgo.HashFunc(), nil
	}
	txinfo, err := bcp.InspectTx(ctx, &c.tp, blobcache.Endpoint{}, txh)
	if err != nil {
		return nil, err
	}
	c.cache.Add(txh.OID, txinfo)
	return txinfo.HashAlgo.HashFunc(), nil
}
