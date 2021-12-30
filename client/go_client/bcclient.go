package bcclient

import (
	"os"

	"github.com/blobcache/blobcache/pkg/bchttp"
)

const (
	// EnvBlobcacheAPI is the name of the environment variable used
	// as the endpoint for the BLOBCACHE_API
	EnvBlobcacheAPI = "BLOBCACHE_API"
	// DefaultEndpoint is the endpoint assumed if the environment variable
	// defined by EnvBlobcacheAPI (BLOBCACHE_API) is not set.
	DefaultEndpoint = "http://127.0.0.1:6025"
)

// Client implements blobcache.Service
// by connecting to a daemon over HTTP
type Client = bchttp.Client

// NewClient creates a Client backed by the server at endpoint
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
