package bcsys

import (
	"blobcache.io/blobcache/src/blobcache"
)

type Policy interface {
	OpenFiat(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet
	// CanCreate returns true if the peer can create a new volume.
	CanCreate(peer blobcache.PeerID) bool
	CanConnect(peer blobcache.PeerID) bool
}
