package blobcachecmd

import (
	"fmt"
	"io"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/star"
)

var beginTxCmd = star.Command{
	Metadata: star.Metadata{
		Short: "begins a new transaction",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}

		txp := blobcache.TxParams{}
		for _, arg := range c.Extra {
			switch arg {
			case "--mutate":
				txp.Mutate = true
			case "--gc":
				txp.GC = true
			default:
				return fmt.Errorf("unknown argument: %s", arg)
			}
		}
		if err := txp.Validate(); err != nil {
			return err
		}
		volh := volHParam.Load(c)
		txh, err := svc.BeginTx(c.Context, volh, txp)
		if err != nil {
			return err
		}
		printOK(c, "BEGIN TX")
		fmt.Fprintf(c.StdOut, "%s\n", txh.String())
		return nil
	},
}

var txCmd = star.NewDir(star.Metadata{
	Short: "perform operations on a transaction",
}, map[star.Symbol]star.Command{

	"abort":   txAbortCmd,
	"commit":  txCommitCmd,
	"inspect": txInspectCmd,

	"load": txLoadCmd,
	"save": txSaveCmd,

	"post":   txPostCmd,
	"get":    txGetCmd,
	"exists": txExistsCmd,

	"delete":     txDeleteCmd,
	"visit":      txVisitCmd,
	"is-visited": txIsVisitedCmd,

	"allow-link": txAllowLinkCmd,
})

var txInspectCmd = star.Command{
	Metadata: star.Metadata{
		Short: "inspects a transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		ti, err := svc.InspectTx(c.Context, txHParam.Load(c))
		if err != nil {
			return err
		}
		c.Printf("%v\n", *ti)
		return nil
	},
}

var txAbortCmd = star.Command{
	Metadata: star.Metadata{
		Short: "aborts a transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.Abort(c.Context, txHParam.Load(c)); err != nil {
			return err
		}
		printOK(c, "ABORT")
		return nil
	},
}

var txCommitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "commits a transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.Commit(c.Context, txHParam.Load(c)); err != nil {
			return err
		}
		printOK(c, "COMMIT")
		return nil
	},
}

var txLoadCmd = star.Command{
	Metadata: star.Metadata{
		Short: "loads the transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		var root []byte
		if err := svc.Load(c.Context, txHParam.Load(c), &root); err != nil {
			return err
		}
		_, err = c.StdOut.Write(root)
		return err
	},
}

var txSaveCmd = star.Command{
	Metadata: star.Metadata{
		Short: "saves the transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		root, err := io.ReadAll(c.StdIn)
		if err != nil {
			return err
		}
		if err := svc.Save(c.Context, txHParam.Load(c), root); err != nil {
			return err
		}
		printOK(c, "SAVE")
		return nil
	},
}

var txPostCmd = star.Command{
	Metadata: star.Metadata{
		Short: "posts data to the transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		blob, err := io.ReadAll(c.StdIn)
		if err != nil {
			return err
		}
		cid, err := svc.Post(c.Context, txHParam.Load(c), blob, blobcache.PostOpts{})
		if err != nil {
			return err
		}
		printOK(c, "POST")
		fmt.Fprintf(c.StdOut, "CID: %s\n", cid.String())
		return nil
	},
}

var txGetCmd = star.Command{
	Metadata: star.Metadata{
		Short: "gets data from the transaction",
	},
	Pos: []star.Positional{txHParam, cidParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}

		buf := make([]byte, bclocal.MaxMaxBlobSize)
		n, err := svc.Get(c.Context, txHParam.Load(c), cidParam.Load(c), buf, blobcache.GetOpts{})
		if err != nil {
			return err
		}
		_, err = c.StdOut.Write(buf[:n])
		if err != nil {
			return err
		}
		return nil
	},
}

var txExistsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "checks if data exists in the transaction",
	},
	Pos: []star.Positional{txHParam, cidsParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		cids := cidsParam.Load(c)
		exists := make([]bool, len(cids))
		if err := svc.Exists(c.Context, txHParam.Load(c), cids, exists); err != nil {
			return err
		}
		c.Printf(checkmark + " EXISTS OK\n")
		for i, cid := range cids {
			if exists[i] {
				c.Printf("%s YES\n", cid.String())
			} else {
				c.Printf("%s NO\n", cid.String())
			}
		}
		return nil
	},
}

var txDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "deletes data from the transaction",
	},
	Pos: []star.Positional{txHParam, cidsParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.Delete(c.Context, txHParam.Load(c), cidsParam.Load(c)); err != nil {
			return err
		}
		printOK(c, "DELETE")
		return nil
	},
}

var txVisitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "visits data in the transaction",
	},
	Pos: []star.Positional{txHParam, cidsParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.Visit(c.Context, txHParam.Load(c), cidsParam.Load(c)); err != nil {
			return err
		}
		printOK(c, "VISIT")
		return nil
	},
}

var txIsVisitedCmd = star.Command{
	Metadata: star.Metadata{
		Short: "checks if data is visited in the transaction",
	},
	Pos: []star.Positional{txHParam, cidsParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		cids := cidsParam.Load(c)
		visited := make([]bool, len(cids))
		if err := svc.IsVisited(c.Context, txHParam.Load(c), cids, visited); err != nil {
			return err
		}
		printOK(c, "IS VISITED")
		for i, cid := range cidsParam.Load(c) {
			if visited[i] {
				c.Printf("%s YES\n", cid.String())
			} else {
				c.Printf("%s NO\n", cid.String())
			}
		}
		return nil
	},
}

var txAllowLinkCmd = star.Command{
	Metadata: star.Metadata{
		Short: "allows the transaction to link to another transaction",
	},
	Pos: []star.Positional{txHParam, subvolHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		txh := txHParam.Load(c)
		subvolh := subvolHParam.Load(c)
		if err := svc.AllowLink(c.Context, txh, subvolh); err != nil {
			return err
		}
		printOK(c, "ALLOW-LINK")
		c.Printf("linked -> %s\n", subvolh.OID.String())
		return nil
	},
}

func printOK(c star.Context, method string) {
	c.Printf("%s %s OK\n", checkmark, method)
}

var volHParam = star.Required[blobcache.Handle]{
	Name:  "volh",
	Parse: blobcache.ParseHandle,
}

var txHParam = star.Required[blobcache.Handle]{
	Name:  "txh",
	Parse: blobcache.ParseHandle,
}

var cidParam = star.Required[blobcache.CID]{
	Name:  "cid",
	Parse: blobcache.ParseCID,
}

var cidsParam = star.Repeated[blobcache.CID]{
	Name:  "cid",
	Parse: blobcache.ParseCID,
}

var subvolHParam = star.Required[blobcache.Handle]{
	Name:  "subvol",
	Parse: blobcache.ParseHandle,
}

const checkmark = "✓"
