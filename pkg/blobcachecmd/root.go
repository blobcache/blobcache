package blobcachecmd

import (
	"context"
	"log"

	"github.com/brendoncarroll/blobcache/pkg/bchttp"
	"github.com/brendoncarroll/blobcache/pkg/blobcache"
	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
)

func Execute() error {
	return rootCmd.Execute()
}

var node *blobcache.Node

var rootCmd = &cobra.Command{
	Short: "Blobcache",
	Use:   "blobcache",
}

var runCmd = &cobra.Command{
	Short: "runs the blobcache server",
	Use:   "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		const laddr = "127.0.0.1:"
		dataDB, err := bolt.Open("./data.db", 0666, nil)
		if err != nil {
			return err
		}
		metadataDB, err := bolt.Open("./metadata.db", 0666, nil)
		if err != nil {
			return err
		}
		params := blobcache.Params{
			DataDB:     dataDB,
			MetadataDB: metadataDB,
		}
		node, err = blobcache.NewNode(params)
		if err != nil {
			return err
		}
		defer func() {
			if err := node.Shutdown(); err != nil {
				log.Println(err)
			}
		}()
		server := bchttp.NewServer(node, laddr)
		if err := server.Run(context.Background()); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}
