package blobcachecmd

import (
	"context"
	"errors"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/sirupsen/logrus"
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
		swarm, err := setupSwarm(params.PrivateKey, config.INet256API)
		if err != nil {
			return err
		}
		logrus.Info("LOCAL ID: ", swarm.LocalAddrs()[0].(p2p.PeerID))
		mux := simplemux.MultiplexSwarm(swarm)
		params.Mux = mux
		pstore, err := newPeerStore(swarm, config.Peers)
		if err != nil {
			return err
		}
		params.PeerStore = pstore
		d := NewDaemon(DaemonParams{
			BlobcacheParams: *params,
			APIAddr:         config.APIAddr,
			PeerStore:       pstore,
			Swarm:           swarm,
		})
		return d.Run(context.Background())
	},
}
