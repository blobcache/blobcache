package blobcachecmd

import (
	"bufio"
	"fmt"

	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:     "create",
	Short:   "creates a pinset",
	PreRunE: setupClient,
	Args:    cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		h, err := dirserv.ParseHandle([]byte(args[0]))
		if err != nil {
			return err
		}
		name := args[1]
		psh, err := client.CreatePinSet(ctx, *h, name, blobcache.PinSetOptions{})
		if err != nil {
			return err
		}
		w := cmd.OutOrStdout()
		_, err = w.Write([]byte(psh.String() + "\n"))
		return err
	},
}

var mkdirCmd = &cobra.Command{
	Use:     "mkdir",
	Short:   "creates a directory",
	PreRunE: setupClient,
	Args:    cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		h, err := dirserv.ParseHandle([]byte(args[0]))
		if err != nil {
			return err
		}
		name := args[1]
		psh, err := client.CreateDir(ctx, *h, name)
		if err != nil {
			return err
		}
		w := cmd.OutOrStdout()
		_, err = w.Write([]byte(psh.String() + "\n"))
		return err
	},
}

var lsCmd = &cobra.Command{
	Use:     "ls",
	Short:   "lists entries beneath a directory",
	PreRunE: setupClient,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		h, err := dirserv.ParseHandle([]byte(args[0]))
		if err != nil {
			return err
		}
		ents, err := client.ListEntries(ctx, *h)
		if err != nil {
			return err
		}
		w := bufio.NewWriter(cmd.OutOrStdout())
		fmtStr := "%-20s\t%s\n"
		fmt.Fprintf(w, fmtStr, "NAME", "ID")
		for _, ent := range ents {
			fmt.Fprintf(w, fmtStr, ent.Name, ent.ID.String())
		}
		return w.Flush()
	},
}
