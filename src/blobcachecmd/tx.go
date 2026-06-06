package blobcachecmd

import (
	"bufio"
	"fmt"
	"io"
	"strings"

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
			case "--modify":
				txp.Modify = true
			case "--gc":
				txp.GCBlobs = true
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
}, map[string]star.Command{

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

	"link":        txLinkCmd,
	"unlink":      txUnlinkCmd,
	"visit-links": txVisitLinksCmd,
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
		Short: "loads data from the Volume cell",
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
		Short: "saves data to the Volume cell",
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
		Short: "posts a blob to the Volume's store",
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
		Short: "gets data from the Volume store",
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
		Short: "checks if a blob exists in the Volume's store",
	},
	Pos: []star.Positional{txHParam, cidsParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		cids := cidsParam.Load(c)
		var existsBM blobcache.BitMap
		exists := make([]bool, len(cids))
		if err := svc.Exists(c.Context, txHParam.Load(c), cids, &existsBM); err != nil {
			return err
		}
		for i := range cids {
			exists[i] = existsBM.IsSet(i)
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
		Short: "deletes a blob from the Volume",
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
		Short: "mark a blob as visited in a GC transaction",
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
		Short: "checks if blobs from stdin have been visited in the transaction",
	},
	Pos: []star.Positional{txHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		var cids []blobcache.CID
		sc := bufio.NewScanner(c.StdIn)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			cid, err := blobcache.ParseCID(line)
			if err != nil {
				return err
			}
			cids = append(cids, cid)
		}
		if err := sc.Err(); err != nil {
			return err
		}
		var visited blobcache.BitMap
		if err := svc.IsVisited(c.Context, txHParam.Load(c), cids, &visited); err != nil {
			return err
		}
		printOK(c, "IS VISITED")
		for i, cid := range cids {
			if visited.IsSet(i) {
				c.Printf("%s YES\n", cid.String())
			} else {
				c.Printf("%s NO\n", cid.String())
			}
		}
		return nil
	},
}

var txLinkCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a link to another Volume",
	},
	Pos: []star.Positional{txHParam, subvolHParam},
	Flags: map[string]star.Flag{
		"mask": maskParam,
	},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		txh := txHParam.Load(c)
		mask, maskOK := maskParam.LoadOpt(c)
		if !maskOK {
			mask = blobcache.Action_ALL
		}
		subvolh := subvolHParam.Load(c)
		ltok, err := svc.Link(c.Context, txh, subvolh, mask)
		if err != nil {
			return err
		}
		printOK(c, "LINK")
		c.Printf("TOKEN: %s\n", ltok.String())
		return nil
	},
}

var txUnlinkCmd = star.Command{
	Metadata: star.Metadata{
		Short: "removes a link from the Volume",
	},
	Pos: []star.Positional{txHParam, linkTokenIDParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.Unlink(c.Context, txHParam.Load(c), []blobcache.LinkTokenID{linkTokenIDParam.Load(c)}); err != nil {
			return err
		}
		printOK(c, "UNLINK")
		return nil
	},
}

var txVisitLinksCmd = star.Command{
	Metadata: star.Metadata{
		Short: "mark a link as visited in a GC transaction",
	},
	Pos: []star.Positional{txHParam, linkTokenIDParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.VisitLinks(c.Context, txHParam.Load(c), []blobcache.LinkTokenID{linkTokenIDParam.Load(c)}); err != nil {
			return err
		}
		printOK(c, "VISIT-LINKS")
		return nil
	},
}

func printOK(c star.Context, method string) {
	c.Printf("%s %s OK\n", checkmark, method)
}

var volHParam = &star.Required[blobcache.Handle]{
	PosName:  "volh",
	ShortDoc: "a volume handle",
	Parse:    blobcache.ParseHandle,
}

var txHParam = &star.Required[blobcache.Handle]{
	PosName:  "txh",
	ShortDoc: "a transaction handle",
	Parse:    blobcache.ParseHandle,
}

var cidParam = &star.Required[blobcache.CID]{
	PosName:  "cid",
	ShortDoc: "a content identifier",
	Parse:    blobcache.ParseCID,
}

var cidsParam = &star.Repeated[blobcache.CID]{
	PosName:  "cids",
	ShortDoc: "a list of content identifiers",
	Parse:    blobcache.ParseCID,
}

var subvolHParam = &star.Required[blobcache.Handle]{
	PosName:  "subvolh",
	ShortDoc: "a handle to a volume",
	Parse:    blobcache.ParseHandle,
}

var oidsParam = &star.Repeated[blobcache.OID]{
	ShortDoc: "an object identifier (OID)",
	Parse:    blobcache.ParseOID,
}

var linkTokenIDParam = &star.Required[blobcache.LinkTokenID]{
	PosName:  "link-token-id",
	ShortDoc: "a link token id (base64 CID format)",
	Parse: func(s string) (blobcache.LinkTokenID, error) {
		cid, err := blobcache.ParseCID(s)
		if err != nil {
			return blobcache.LinkTokenID{}, err
		}
		return blobcache.LinkTokenID(cid), nil
	},
}

const checkmark = "✓"
