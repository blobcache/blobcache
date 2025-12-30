package main

import (
	"context"
	"log"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

// This example:
// 1. Creates a Volume
// 2. Performs a modifying transaction
// 3. Performs a read-only transaction
func main() {
	ctx := context.Background()
	bc := bcclient.NewClientFromEnv()
	// Print the Endpoint, this is the information needed
	// for other Nodes to connect to the local Node.
	ep, err := bc.Endpoint(ctx)
	log.Println("ENDPOINT", ep, err)

	// Create a Volume
	volh, err := bc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	if err != nil {
		log.Fatal(err)
	}
	// Modify the Volume
	if err := bcsdk.Modify(ctx, bc, *volh, func(s bcsdk.RW, root []byte) ([]byte, error) {
		// post some data to the store
		cid1, err := s.Post(ctx, []byte("hello world 1\n"))
		if err != nil {
			return nil, err
		}
		cid2, err := s.Post(ctx, []byte("hello world 2\n"))
		if err != nil {
			return nil, err
		}
		// the root will be both CIDs concatenated
		root = root[:0]
		root = append(root, cid1[:]...)
		root = append(root, cid2[:]...)
		// The transaction will be committed if we return a nil error
		return root, nil
	}); err != nil {
		log.Fatal(err)
	}

	// View opens a RO transaction on the Volume
	// The transaction is automatically cleaned up when the callback returns.
	if err := bcsdk.View(ctx, bc, *volh, func(s bcsdk.RO, root []byte) error {
		log.Printf("VOLUME CELL: %q", root)
		cid1, cid2 := blobcache.CID(root[0:32]), blobcache.CID(root[32:64])

		// read the first blob into buf
		buf := make([]byte, 1024)
		n, err := s.Get(ctx, cid1, buf)
		if err != nil {
			return err
		}
		log.Printf("BLOB 1: %q", buf[:n])

		// read the second blob into buf
		n, err = s.Get(ctx, cid2, buf)
		if err != nil {
			return err
		}
		log.Printf("BLOB 2: %q", buf[:n])
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
