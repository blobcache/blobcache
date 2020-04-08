package blobcachecmd

import (
	"github.com/spf13/cobra"
)

func Execute() error {
	return rootCmd.Execute()
}

var rootCmd = &cobra.Command{
	Short: "Blobcache",
	Use:   "blobcache",
}
