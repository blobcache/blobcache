package blobcachecmd

import (
	"github.com/spf13/cobra"
)

const defaultConfigPath = "blobcache.yml"

func init() {
	rootCmd.AddCommand(createConfigCmd)
}

var createConfigCmd = &cobra.Command{
	Use:   "create-config",
	Short: "writes a new default config to stdout",
	RunE: func(cmd *cobra.Command, args []string) error {
		config := DefaultConfig()
		_, err := cmd.OutOrStdout().Write(config.Marshal())
		return err
	},
}
