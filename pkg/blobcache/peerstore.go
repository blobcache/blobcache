package blobcache

import "github.com/brendoncarroll/go-p2p"

type PeerStore interface {
	TrustFor(id p2p.PeerID) (int64, error)
	ListPeers() []p2p.PeerID
}
