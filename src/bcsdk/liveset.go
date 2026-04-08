package bcsdk

import (
	"blobcache.io/blobcache/src/blobcache"
)

// LiveSet is a set of handles which are being kept alive.
type LiveSet struct {
	svc blobcache.Service
	// handles is a map from handles
	handles map[blobcache.Handle]struct{}
}

func NewLiveSet(svc blobcache.Service) *LiveSet {
	ls := &LiveSet{
		svc:     svc,
		handles: make(map[blobcache.Handle]struct{}),
	}
	return ls
}

func (ls LiveSet) Close() error {
	return nil
}

func (ls LiveSet) Add(h blobcache.Handle) {

}

func (ls LiveSet) Remove(h blobcache.Handle) {

}
