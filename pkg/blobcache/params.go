package blobcache

import (
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
<<<<<<< HEAD
	"github.com/brendoncarroll/go-p2p/p/simplemux"
=======
>>>>>>> 99f857d... Clean up some stuff
	bolt "go.etcd.io/bbolt"
)

type Params struct {
	MetadataDB *bolt.DB
<<<<<<< HEAD
	Cache      Cache

	Mux        simplemux.Muxer
	PrivateKey p2p.PrivateKey
	PeerStore  PeerStore

	ExternalSources []blobs.Getter
=======
	DataDB     *bolt.DB
	Swarm      p2p.Swarm
	PrivateKey p2p.PrivateKey

	ExternalSources []blobs.Getter

	Capacity      uint64
	AutoPeerLocal bool
>>>>>>> 99f857d... Clean up some stuff
}
