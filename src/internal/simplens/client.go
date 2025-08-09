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
}

// CreateAt creates a new Volume using spec, and links it to volh.
func (c *Client) CreateAt(ctx context.Context, volh blobcache.Handle, name string, spec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, err
	}
	volInfo, err := txn.CreateSubVolume(ctx, spec)
	if err != nil {
		return nil, err
	}
	nstx := Tx{Tx: txn}
	if err := nstx.PutEntry(ctx, name, volInfo.ID); err != nil {
		return nil, err
	}
	if err := nstx.Commit(ctx); err != nil {
		return nil, err
	}
	return c.Service.Open(ctx, volh, volInfo.ID, blobcache.Action_ALL)
}

func (c *Client) OpenAt(ctx context.Context, volh blobcache.Handle, name string, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{})
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
	return c.Service.Open(ctx, volh, ent.Target, mask)
}

func (c *Client) PutEntry(ctx context.Context, volh blobcache.Handle, name string, target blobcache.Handle) error {
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
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)
	nstx := Tx{Tx: txn}
	return nstx.ListNames(ctx)
}

func (c *Client) GetEntry(ctx context.Context, volh blobcache.Handle, name string) (*Entry, error) {
	txn, err := blobcache.BeginTx(ctx, c.Service, volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)
	nstx := Tx{Tx: txn}
	ent, err := nstx.GetEntry(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Entry{Name: name, Target: ent.Target, Rights: ent.Rights}, nil
}

type Entry struct {
	Name   string
	Target blobcache.OID
	Rights blobcache.ActionSet
}
