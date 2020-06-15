package blobcachecmd

import (
	"context"
	"errors"
	"log"

	"github.com/blobcache/blobcache/pkg/bchttp"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/spf13/cobra"
)

var (
	configPath string
)

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringVar(&configPath, "config", defaultConfigPath, "")
}

var runCmd = &cobra.Command{
	Short: "runs the blobcache server",
	Use:   "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := cmd.ParseFlags(args); err != nil {
			return err
		}
		if configPath == "" {
			return errors.New("must specify config path")
		}
		cf := NewConfigFile(configPath)
		config, err := cf.Load()
		if err != nil {
			return err
		}
		params, err := buildParams(configPath, config)
		if err != nil {
			return err
		}
		node := blobcache.NewNode(*params)

		defer func() {
			if err := node.Shutdown(); err != nil {
				log.Println(err)
			}
		}()
		s := bchttp.NewServer(node, config.APIAddr)
		return s.Run(context.Background())
	},
}
