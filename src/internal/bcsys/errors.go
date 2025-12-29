// TODO: eventually remove this file, all errors should be in src/blobcache
package bcsys

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type ErrNotAllowed struct {
	Peer blobcache.PeerID
	// TODO: this should be a blobcache.ActionSet
	Action string
	Target blobcache.OID
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("access denied: %s %s %s", e.Peer, e.Action, e.Target)
}
