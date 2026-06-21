// Package bcclient provides a client for the Blobcache API.
package bcclient

import (
	"net/http"
	"os"
	"strings"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bcipc"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
)

const (
	// EnvBlobcacheAPI is the name of the environment variable used
	// as the endpoint for the BLOBCACHE_API
	EnvBlobcacheAPI = "BLOBCACHE_API"
)

// NewClient creates a Client backed by the server at endpoint
func NewClient(endpoint string) blobcache.Service {
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		unixAddr, _ := strings.CutPrefix(endpoint, "unix://")
		return bcipc.NewClient(unixAddr)
	case strings.HasPrefix(endpoint, "http://"):
		hc := http.DefaultClient
		return bchttp.NewClient(hc, endpoint)
	default:
		return bcipc.NewClient(endpoint)
	}
}

// NewClientFromEnv creates a new client from environment variables
func NewClientFromEnv() blobcache.Service {
	value, ok := os.LookupEnv(EnvBlobcacheAPI)
	if !ok {
		value = DefaultEndpoint
	}
	return NewClient(value)
}

const (
	// EnvBlobcacheNSRoot configures the namespace root.
	// It is expected to be an OID on the local Node.
	EnvBlobcacheNSRoot = "BLOBCACHE_NS_ROOT"
)

// EnvNSRoot parses an OID read from the BLOBCACHE_NS_ROOT
// If the environment variable does not exist, then it returns the root OID
// If the variable cannot be parsed into an OID than an error is returned.
func EnvNSRoot() (blobcache.OID, error) {
	val, ok := os.LookupEnv(EnvBlobcacheNSRoot)
	if !ok {
		return blobcache.OID{}, nil
	}
	return blobcache.ParseOID(val)
}

// NewNSClientFromEnv returns an bcns.Client configured from the environment.
func NewNSClientFromEnv() (bcns.Client, error) {
	root, err := EnvNSRoot()
	if err != nil {
		return bcns.Client{}, err
	}
	return bcns.NewClient(NewClientFromEnv(), root), nil
}
