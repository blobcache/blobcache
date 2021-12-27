package bcclient

import "github.com/blobcache/blobcache/pkg/bchttp"

type Client = bchttp.Client

func NewClient(endpoint string) *Client {
	return bchttp.NewClient(endpoint)
}
