package bcsys

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type VolumeEntry struct {
	OID blobcache.OID

	MaxSize  int64
	Schema   blobcache.SchemaSpec
	HashAlgo blobcache.HashAlgo
	Salted   bool
	Backend  blobcache.VolumeBackend[blobcache.OID]

	Deps []blobcache.OID
}

func (ve VolumeEntry) Info() *blobcache.VolumeInfo {
	return &blobcache.VolumeInfo{
		ID:           ve.OID,
		VolumeConfig: ve.Config(),
		Backend:      ve.Backend,
	}
}

func (ve VolumeEntry) Config() blobcache.VolumeConfig {
	return blobcache.VolumeConfig{
		MaxSize:  ve.MaxSize,
		Schema:   ve.Schema,
		HashAlgo: ve.HashAlgo,
		Salted:   ve.Salted,
	}
}

type MetadataStore interface {
	Put(ctx context.Context, oid blobcache.OID, entry VolumeEntry) error
	Delete(ctx context.Context, oid blobcache.OID) error
	Get(ctx context.Context, oid blobcache.OID, dst *VolumeEntry) (bool, error)
}
