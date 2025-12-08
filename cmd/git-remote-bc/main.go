package main

import (
	"context"
	"log"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcgit"
)

func main() {
	defer os.Stderr.Sync()
	log.Println("args:", os.Args)
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

	srv := bcgit.NewRemoteHelper(bc, *u)
	if err := srv.Serve(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
	log.Println("OK")
	os.Exit(0)
}
