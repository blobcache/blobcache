package blobcachecmd

import (
	"context"
	"errors"

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
		nodeParams, err := buildParams(configPath, config)
		if err != nil {
			return err
		}
		d := NewDaemon(DaemonParams{
			NodeParams: *nodeParams,
			APIAddr:    config.APIAddr,
		})
		return d.Run(context.Background())
	},
}
