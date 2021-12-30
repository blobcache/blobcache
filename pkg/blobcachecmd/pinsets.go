package blobcachecmd

import (
	"encoding/base64"
	"encoding/json"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:     "create",
	Short:   "creates a pinset",
	PreRunE: setupClient,
	RunE: func(cmd *cobra.Command, args []string) error {
		psh, err := client.CreatePinSet(ctx, blobcache.PinSetOptions{})
		if err != nil {
			return err
		}
		data, err := json.Marshal(psh)
		if err != nil {
			return err
		}
		w := cmd.OutOrStdout()
		_, err = w.Write([]byte(base64.URLEncoding.EncodeToString(data)))
		return err
	},
}
