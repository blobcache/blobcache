package simplens

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

// Client provides methods for interacting with a Namespace using a Service.
type Client struct {
	Service blobcache.Service
	Schema  Schema
	PeerID  *blobcache.PeerID
}

// CreateAt creates a new Volume using spec, and links it to volh.
func (c *Client) CreateAt(ctx context.Context, nsh blobcache.Handle, name string, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	nsh, err := c.resolve(ctx, nsh)
	if err != nil {
		return nil, err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, nsh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	volh, err := c.Service.CreateVolume(ctx, c.PeerID, spec)
	if err != nil {
		return nil, err
	}
	if err := txn.AllowLink(ctx, *volh); err != nil {
		return nil, err
	}
	nstx := Tx{Tx: txn}
	if err := nstx.PutEntry(ctx, name, volh.OID); err != nil {
		return nil, err
	}
	if err := nstx.Commit(ctx); err != nil {
		return nil, err
	}
	return volh, nil
}

func (c *Client) OpenAt(ctx context.Context, nsh blobcache.Handle, name string, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	nsh, err := c.resolve(ctx, nsh)
	if err != nil {
		return nil, err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, nsh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)
	nstx := Tx{Tx: txn}
	ent, err := nstx.GetEntry(ctx, name)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, fmt.Errorf("entry not found")
	}
	return c.Service.OpenFrom(ctx, nsh, ent.Target, mask)
}

func (c *Client) PutEntry(ctx context.Context, volh blobcache.Handle, name string, target blobcache.Handle) error {
	volh, err := c.resolve(ctx, volh)
	if err != nil {
		return err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	if err := txn.AllowLink(ctx, target); err != nil {
		return err
	}
	nstx := Tx{Tx: txn}
	if err := nstx.PutEntry(ctx, name, target.OID); err != nil {
		return err
	}
	return nstx.Commit(ctx)
}

func (c *Client) DeleteEntry(ctx context.Context, volh blobcache.Handle, name string) error {
	volh, err := c.resolve(ctx, volh)
	if err != nil {
		return err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	nstx := Tx{Tx: txn}
	if err := nstx.DeleteEntry(ctx, name); err != nil {
		return err
	}
	return nstx.Commit(ctx)
}

func (c *Client) ListNames(ctx context.Context, volh blobcache.Handle) ([]string, error) {
	volh, err := c.resolve(ctx, volh)
	if err != nil {
		return nil, err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)
	nstx := Tx{Tx: txn}
	return nstx.ListNames(ctx)
}

func (c *Client) GetEntry(ctx context.Context, volh blobcache.Handle, name string) (*Entry, error) {
	volh, err := c.resolve(ctx, volh)
	if err != nil {
		return nil, err
	}
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)
	nstx := Tx{Tx: txn}
	return nstx.GetEntry(ctx, name)
}

func (c *Client) resolve(ctx context.Context, volh blobcache.Handle) (blobcache.Handle, error) {
	if volh == (blobcache.Handle{}) {
		volh2, err := c.Service.OpenAs(ctx, c.PeerID, blobcache.OID{}, blobcache.Action_ALL)
		if err != nil {
			return blobcache.Handle{}, err
		}
		volh = *volh2
	}
	return volh, nil
}
