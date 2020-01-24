package blobcache

import (
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	bolt "go.etcd.io/bbolt"
)

type Params struct {
	MetadataDB *bolt.DB
	DataDB     *bolt.DB
	Swarm      p2p.Swarm
	PrivateKey p2p.PrivateKey

	ExternalSources []blobs.Getter

	Capacity      uint64
	AutoPeerLocal bool
}
