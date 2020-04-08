package blobcachecmd

import (
	"errors"

	"github.com/spf13/cobra"
)

const defaultConfigPath = "blobcache.yml"

func init() {
	rootCmd.AddCommand(createConfigCmd)
	createConfigCmd.Flags().StringVar(&configPath, "config", defaultConfigPath, "--config=./myconfigfile.yml")
}

var createConfigCmd = &cobra.Command{
	Use:   "create-config",
	Short: "creates a new config file",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := cmd.ParseFlags(args); err != nil {
			return err
		}
		if configPath == "" {
			return errors.New("missing config path")
		}
		cmd.Println("creating config")
		config := DefaultConfig()

		cmd.Printf("saving config to %s\n", configPath)
		cf := NewConfigFile(configPath)
		return cf.Save(*config)
	},
}
