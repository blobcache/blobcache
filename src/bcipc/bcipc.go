// Package bcipc implements the Blobcache Protocol over UNIX sockets
package bcipc

import (
	"context"
	"net"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/pools"
)

const MaxMessageLen = 1<<24 + bcp.HeaderLen

var _ bcp.Asker = &clientTransport{}

type clientTransport struct {
	pool   *pools.OpenClose[*net.UnixConn]
	bufLen int
}

func (ct *clientTransport) Ask(ctx context.Context, remEp blobcache.Endpoint, req bcp.Message, resp *bcp.Message) error {
	conn, err := ct.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer ct.pool.Take(ctx)
	_, err = req.WriteTo(conn)
	if err != nil {
		return err
	}
	return resp.ReadDatagramFrom(conn, MaxMessageLen)
}
