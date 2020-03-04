package blobcachecmd

import (
	"context"
	"log"

	"github.com/brendoncarroll/blobcache/pkg/bchttp"
	"github.com/brendoncarroll/blobcache/pkg/blobcache"
	"github.com/spf13/cobra"
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
		const laddr = "127.0.0.1:8026"
		configPath := "./blobcache.yml"
		configFile := blobcache.NewConfigFile(configPath)
		config, err := configFile.Load()
		if err != nil {
			return err
		}
		params, err := config.Params()
		if err != nil {
			return err
		}
		node = blobcache.NewNode(*params)
		defer func() {
			if err := node.Shutdown(); err != nil {
				log.Println(err)
			}
		}()
		s := bchttp.NewServer(node, laddr)
		return s.Run(context.Background())

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
