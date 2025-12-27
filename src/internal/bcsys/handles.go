package bcsys

import (
	"crypto/rand"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
)

// handleSystem manages handles for a blobcache instance.
type handleSystem struct {
	mu      sync.RWMutex
	handles map[[32]byte]handle
	active  map[blobcache.OID]uint32
}

// Create creates a new handle.
func (hs *handleSystem) Create(target blobcache.OID, rights blobcache.ActionSet, createdAt, expiresAt time.Time) blobcache.Handle {
	secret := [16]byte{}
	if _, err := rand.Read(secret[:]); err != nil {
		panic(err)
	}
	ret := blobcache.Handle{OID: target, Secret: secret}
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.handles == nil {
		hs.handles = make(map[[32]byte]handle)
	}
	hs.handles[handleKey(ret)] = handle{
		target:    target,
		createdAt: createdAt,
		expiresAt: expiresAt,
		rights:    rights,
	}
	if hs.active == nil {
		hs.active = make(map[blobcache.OID]uint32)
	}
	hs.active[target] += 1
	return ret
}

func (hs *handleSystem) Drop(h blobcache.Handle) {
	k := handleKey(h)
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.dropHandleNoLock(k)
}

func (hs *handleSystem) dropHandleNoLock(k [32]byte) {
	if hs.handles == nil {
		return
	}
	if h, exists := hs.handles[k]; exists {
		hs.active[h.target] -= 1
		if hs.active[h.target] == 0 {
			delete(hs.active, h.target)
		}
	}
	delete(hs.handles, k)
}

func (hs *handleSystem) Resolve(h blobcache.Handle) (blobcache.OID, blobcache.ActionSet) {
	k := handleKey(h)
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	handle, exists := hs.handles[k]
	if !exists {
		return blobcache.OID{}, 0
	}
	return handle.target, handle.rights
}

func (hs *handleSystem) KeepAlive(h blobcache.Handle, expiresAt time.Time) {
	k := handleKey(h)
	hs.mu.Lock()
	defer hs.mu.Unlock()
	handle, exists := hs.handles[k]
	if !exists {
		return
	}
	handle.expiresAt = expiresAt
	hs.handles[k] = handle
}

func (hs *handleSystem) Inspect(h blobcache.Handle) (handle, bool) {
	k := handleKey(h)
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	handle, exists := hs.handles[k]
	return handle, exists
}

func (hs *handleSystem) DropAllForOID(oid blobcache.OID) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	for k, handle := range hs.handles {
		if handle.target == oid {
			hs.dropHandleNoLock(k)
		}
	}
}

func (hs *handleSystem) filter(keep func(handle) bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	for k, handle := range hs.handles {
		if !keep(handle) {
			hs.dropHandleNoLock(k)
		}
	}
}

// isAlive returns true if there is a handle to the object.
func (hs *handleSystem) isAlive(x blobcache.OID) bool {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	_, exists := hs.active[x]
	return exists
}

type handle struct {
	target    blobcache.OID
	createdAt time.Time
	expiresAt time.Time // zero value means no expiration
	rights    blobcache.ActionSet
}
