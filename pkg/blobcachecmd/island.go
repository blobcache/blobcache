package blobcachecmd

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	islandCmd.Flags().StringVar(&apiAddr, "api-addr", DefaultAPIAddr, "--api-addr=192.168.1.100:8080")
}

var islandCmd = &cobra.Command{
	Use:   "island",
	Short: "run a node in memory without any connections",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := cmd.ParseFlags(args); err != nil {
			return err
		}
		d := NewDaemon(DaemonParams{
			Logger:     logrus.StandardLogger(),
			APIAddr:    *&apiAddr,
			NodeParams: blobcache.NewMemParams(),
		})
		return d.Run(context.Background())
	},
}
