package main

import (
	"context"
	"log"
	"os"
	"strings"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcgit"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	l, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	if val := os.Getenv("BLOBCACHE_LOG"); val != "" {
		switch strings.ToLower(val) {
		case "debug":
			l = l.WithOptions(zap.IncreaseLevel(zap.DebugLevel))
		}
	}
	ctx = logctx.NewContext(ctx, l)

	gitrh.Main(ctx, func(ghctx gitrh.Ctx) (*gitrh.Server, error) {
		u, err := blobcache.ParseURL(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		logctx.Debugf(ctx, "successfully parsed blobcache URL: %v", u)
		bc := bcclient.NewClientFromEnv()
		ep, err := bc.Endpoint(context.TODO())
		if err != nil {
			log.Fatal(err)
		}
		logctx.Debugf(ctx, "connected to blobcache node %v", ep)

		rem, err := bcgit.OpenRemoteHelper(ctx, bc, *u)
		if err != nil {
			log.Fatal(err)
		}
		srv := bcgit.NewRemoteHelper(rem)
		return &srv, nil
	})
}
