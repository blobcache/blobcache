package bcclient

import (
	"os"

	"github.com/blobcache/blobcache/pkg/bchttp"
	"github.com/blobcache/blobcache/pkg/blobcachecmd"
)

// EnvBlobcacheAPI is the name of the environment variable used
// as the endpoint for the BLOBCACHE_API
const (
	EnvBlobcacheAPI = "BLOBCACHE_API"
	DefaultEndpoint = blobcachecmd.DefaultAPIAddr
)

type Client = bchttp.Client

func NewClient(endpoint string) *Client {
	return bchttp.NewClient(endpoint)
}

// NewEnvClient creates a new client from environment variables
func NewEnvClient() *Client {
	value, ok := os.LookupEnv(EnvBlobcacheAPI)
	if !ok {
		value = DefaultEndpoint
	}
	return bchttp.NewClient(value)
}
