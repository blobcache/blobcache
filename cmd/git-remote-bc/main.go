package main

import (
	"context"
	"log"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcgit"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
)

func main() {
	ctx := context.Background()
	gitrh.Main(ctx, func(ghctx gitrh.Ctx) (*gitrh.Server, error) {
		u, err := blobcache.ParseURL(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		log.Println("successfully parsed blobcache URL:", u)
		bc := bcclient.NewClientFromEnv()
		ep, err := bc.Endpoint(context.TODO())
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("connected to blobcache node %v", ep)

		rem, err := bcgit.OpenRemoteHelper(ctx, bc, *u)
		if err != nil {
			log.Fatal(err)
		}
		srv := bcgit.NewRemoteHelper(rem)
		return &srv, nil
	})
}
