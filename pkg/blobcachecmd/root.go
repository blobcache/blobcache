package blobcachecmd

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"

	"github.com/inet256/inet256/pkg/serde"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(keygenCmd)
}

func Execute() error {
	return rootCmd.Execute()
}

var rootCmd = &cobra.Command{
	Short: "blobcache",
	Use:   "blobcache",
}

var keygenCmd = &cobra.Command{
	Short: "generate a private key and write it to stdout",
	Use:   "keygen",
	RunE: func(cmd *cobra.Command, args []string) error {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return err
		}
		data, err := serde.MarshalPrivateKeyPEM(priv)
		if err != nil {
			return err
		}
		w := bufio.NewWriter(cmd.OutOrStdout())
		if _, err := w.Write(data); err != nil {
			return err
		}
		return w.Flush()
	},
}
