package blobcachecmd

import (
	"github.com/brendoncarroll/go-p2p/d/celltracker"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(createTrackerCmd)
	createConfigCmd.Flags().StringVar(&configPath, "config", defaultConfigPath, "--config=./myconfigfile.yml")
}

var createTrackerCmd = &cobra.Command{
	Use:   "create-tracker",
	Short: "creates a new tracker config",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("must provide endpoint")
		}
		endpoint := args[0]
		token := celltracker.GenerateToken(endpoint)
		cmd.Println(token)
		return nil
	},
}
