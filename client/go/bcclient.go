// Package bcclient provides a client for the Blobcache API.
package bcclient

import (
	"context"
	"net"
	"net/http"
	"os"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/blobcache"
)

const (
	// EnvBlobcacheAPI is the name of the environment variable used
	// as the endpoint for the BLOBCACHE_API
	EnvBlobcacheAPI = "BLOBCACHE_API"
)

// NewClient creates a Client backed by the server at endpoint
func NewClient(endpoint string) blobcache.Service {
	hc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
	return bchttp.NewClient(hc, endpoint)
}

// NewClientFromEnv creates a new client from environment variables
func NewClientFromEnv() blobcache.Service {
	value, ok := os.LookupEnv(EnvBlobcacheAPI)
	if !ok {
		value = DefaultEndpoint
	}
	return NewClient(value)
}
