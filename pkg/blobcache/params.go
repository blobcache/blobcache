package blobcache

import (
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	bolt "go.etcd.io/bbolt"
)

type Params struct {
	MetadataDB *bolt.DB
	Cache      Cache

	Mux        simplemux.Muxer
	PrivateKey p2p.PrivateKey
	PeerStore  PeerStore

	ExternalSources []blobs.Getter
}
