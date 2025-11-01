package blobcachecmd

import (
	"go.brendoncarroll.net/star"
	"go.inet256.org/inet256/src/inet256"

	"blobcache.io/blobcache/src/blobcache"
)

var dropCmd = star.Command{
	Metadata: star.Metadata{
		Short: "drops a handle",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		if err := svc.Drop(c.Context, volHParam.Load(c)); err != nil {
			return err
		}
		printOK(c, "DROP")
		return nil
	},
}

var shareCmd = star.Command{
	Metadata: star.Metadata{
		Short: "shares a handle",
	},
	Pos: []star.Positional{volHParam, peerParam},
	Flags: map[string]star.Flag{
		"mask": maskParam,
	},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		mask, maskOK := maskParam.LoadOpt(c)
		if !maskOK {
			mask = blobcache.Action_ALL
		}
		h, err := svc.Share(c.Context, volHParam.Load(c), peerParam.Load(c), mask)
		if err != nil {
			return err
		}
		printOK(c, "SHARE")
		c.Printf("HANDLE: %s\n", h.String())
		return nil
	},
}

var peerParam = star.Required[blobcache.PeerID]{
	ID:       "peer",
	ShortDoc: "a PeerID",
	Parse: func(s string) (blobcache.PeerID, error) {
		return inet256.ParseAddrBase64([]byte(s))
	},
}
