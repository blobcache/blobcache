// Package bcclient provides a client for the Blobcache API.
package bcclient

import (
	"context"
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
	EnvBlobcacheAPI    = "BLOBCACHE_API"
	EnvBlobcacheNSRoot = "BLOBCACHE_NS_ROOT"
)

// NewClient creates a Client backed by the server at endpoint
func NewClient(endpoint string) blobcache.Service {
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		unixAddr, _ := strings.CutPrefix(endpoint, "unix://")
		return bcipc.NewClient(unixAddr)
	default:
		hc := http.DefaultClient
		return bchttp.NewClient(hc, endpoint)
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

// EnvNSRoot parses a handle or OID read from the BLOBCACHE_NS_ROOT
// environment variable into a bcns.ObjectExpr
// If the environment variable does not exist, then it returns the root OID
// If the variable cannot be parsed into an ObjectExpr than an error is returned.
func EnvNSRoot() (bcns.ObjectExpr, error) {
	val, ok := os.LookupEnv(EnvBlobcacheNSRoot)
	if !ok {
		return bcns.ObjectExpr{}, nil
	}
	return bcns.ParseObjectish(val)
}

// OpenNSRoot calls EnvNSRoot to get the NS Root from the environment
// Then it uses the Service to find and open the root namespace volume, and
// setup a namespace Client to view and modify the namespace.
func OpenNSRoot(ctx context.Context, bc blobcache.Service) (rootVol *blobcache.Handle, ncs *bcns.Client, _ error) {
	expr, err := EnvNSRoot()
	if err != nil {
		return nil, nil, err
	}
	rootVol, err = expr.Open(ctx, bc)
	if err != nil {
		return nil, nil, err
	}
	bnsc, err := bcns.ClientForVolume(ctx, bc, *rootVol)
	if err != nil {
		return nil, nil, err
	}
	return rootVol, bnsc, nil
}
