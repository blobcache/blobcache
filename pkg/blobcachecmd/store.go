package blobcachecmd

import (
	"io"

	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/spf13/cobra"
)

var postCmd = &cobra.Command{
	Use:     "post",
	Short:   "posts data to a pinset",
	PreRunE: setupClient,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		h, err := dirserv.ParseHandle([]byte(args[0]))
		if err != nil {
			return err
		}
		r := cmd.InOrStdin()
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		id, err := client.Post(ctx, *h, data)
		if err != nil {
			return err
		}
		w := cmd.OutOrStdout()
		_, err = w.Write([]byte(id.String() + "\n"))
		return err
	},
}

var getCmd = &cobra.Command{
	Use:     "get",
	Short:   "get retrieves data from a pinset",
	PreRunE: setupClient,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		h, err := dirserv.ParseHandle([]byte(args[0]))
		if err != nil {
			return err
		}
		r := cmd.InOrStdin()
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var id cadata.ID
		if err := id.UnmarshalBase64(data); err != nil {
			return err
		}
		buf := make([]byte, client.MaxSize())
		if n, err := client.Get(ctx, *h, id, buf); err != nil {
			return err
		} else {
			buf = buf[:n]
		}
		w := cmd.OutOrStdout()
		_, err = w.Write(buf)
		return err
	},
}
