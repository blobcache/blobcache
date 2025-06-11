//go:build darwin

package bcclient

// DefaultEndpoint is the endpoint assumed if the environment variable
// defined by EnvBlobcacheAPI (BLOBCACHE_API) is not set.
const DefaultEndpoint = "unix:///var/run/blobcache/blobcache.sock"
