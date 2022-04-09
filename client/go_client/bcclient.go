package bcclient

import (
	"os"

	"github.com/blobcache/blobcache/pkg/bcgrpc"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"google.golang.org/grpc"
)

const (
	// EnvBlobcacheAPI is the name of the environment variable used
	// as the endpoint for the BLOBCACHE_API
	EnvBlobcacheAPI = "BLOBCACHE_API"
	// DefaultEndpoint is the endpoint assumed if the environment variable
	// defined by EnvBlobcacheAPI (BLOBCACHE_API) is not set.
	DefaultEndpoint = "127.0.0.1:6025"
)

type Client struct {
	blobcache.Service
}

// NewClient creates a Client backed by the server at endpoint
func NewClient(endpoint string) (*Client, error) {
	gc, err := grpc.Dial(endpoint, grpc.WithInsecure())
	return &Client{bcgrpc.NewClient(bcgrpc.NewBlobcacheClient(gc))}, err
}

// NewEnvClient creates a new client from environment variables
func NewEnvClient() (*Client, error) {
	value, ok := os.LookupEnv(EnvBlobcacheAPI)
	if !ok {
		value = DefaultEndpoint
	}
	return NewClient(value)
}
