package blobcachecmd

import (
	"go.brendoncarroll.net/star"
)

func Main() {
	star.Main(rootCmd)
}

var rootCmd = star.NewDir(
	star.Metadata{
		Short: "blobcache is content-addressable storage",
	}, map[star.Symbol]star.Command{
		"daemon":           daemonCmd,
		"daemon-ephemeral": daemonEphemeralCmd,
	},
)
