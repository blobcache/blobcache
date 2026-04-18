package bcsys

import (
	"blobcache.io/blobcache/src/blobcache"
)

type Policy interface {
	OpenFiat(peer blobcache.NodeID, target blobcache.OID) blobcache.ActionSet
	// CanCreate returns true if the peer can create a new volume.
	CanCreate(peer blobcache.NodeID) bool
	CanConnect(peer blobcache.NodeID) bool
}
