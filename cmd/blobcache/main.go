package main

import (
	"log"

	"github.com/brendoncarroll/blobcache/pkg/blobcachecmd"
)

func main() {
	if err := blobcachecmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
