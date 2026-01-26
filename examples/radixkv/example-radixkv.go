package main

import (
	"context"
	"fmt"
	"log"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/radixkv"
)

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	bc := bcclient.NewClientFromEnv()

	// Create a Volume
	vspec := blobcache.DefaultLocalSpec()
	volh, err := bc.CreateVolume(ctx, nil, vspec)
	if err != nil {
		return err
	}
	// prepare a Machine, it holds configuration and caches
	// for the operations we are going to perform.
	kvmach := radixkv.New(new(blobcache.CID), vspec.Local.HashAlgo.HashFunc())

	// Put some keys
	if err := radixkv.Modify(ctx, bc, *volh, kvmach, func(tx *radixkv.Tx) error {
		for i := range 10 {
			s := fmt.Sprintf("%d", i)
			key := []byte(s + "-key")
			val := []byte(s + "-value")
			if err := tx.Put(ctx, key, val); err != nil {
				return err
			}
			log.Printf("PUT %q => %q", key, val)
		}
		return nil
	}); err != nil {
		return err
	}

	// Get some keys
	if err := radixkv.View(ctx, bc, *volh, kvmach, func(tx *radixkv.Tx) error {
		for i := range 10 {
			s := fmt.Sprintf("%d", i)
			key := []byte(s + "-key")
			var val []byte
			if _, err := tx.Get(ctx, key, &val); err != nil {
				return err
			}
			log.Printf("GET %q => %q", key, val)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
