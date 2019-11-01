package main

import (
	"context"
	"crypto/x509"
	"errors"
	"log"
	"os"

	"github.com/brendoncarroll/blobcache/pkg/p2p"

	"github.com/brendoncarroll/blobcache/pkg/blobcache"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	if len(os.Args) < 2 {
		return errors.New("must provide path to config")
	}

	configPath := os.Args[1]
	if configPath == "" {
		return errors.New("bad config path")
	}

	cf := blobcache.NewConfigFile(configPath)
	config, err := cf.Load()
	if err != nil {
		return err
	}
	if len(config.PrivateKey) < 1 {
		privateKey := p2p.GeneratePrivateKey()
		data, err := x509.MarshalPKCS8PrivateKey(privateKey)
		if err != nil {
			return err
		}
		config.PrivateKey = data
		if err := cf.Save(config); err != nil {
			return err
		}
	}

	n, err := blobcache.NewNode(config)
	if err != nil {
		return err
	}
	return n.Run(ctx)
}
