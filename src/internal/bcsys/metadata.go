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
	Backend  blobcache.VolumeSpec

	Deps []blobcache.OID
}

func (ve VolumeEntry) Info() *blobcache.VolumeInfo {
	return &blobcache.VolumeInfo{}
}

func (ve VolumeEntry) Config() blobcache.VolumeConfig {
	return blobcache.VolumeConfig{}
}

type MetadataStore interface {
	Put(ctx context.Context, oid blobcache.OID, entry VolumeEntry) error
	Delete(ctx context.Context, oid blobcache.OID) error
	Get(ctx context.Context, oid blobcache.OID, dst *VolumeEntry) (bool, error)
}
