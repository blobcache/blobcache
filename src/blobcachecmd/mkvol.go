package blobcachecmd

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/simplens"
	"go.brendoncarroll.net/star"
)

var mkVolLocalCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new local volume",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{nameParam},
	F: func(c star.Context) error {
		s, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		nsc := simplens.Client{Service: s}
		volh, err := nsc.CreateAt(c, blobcache.Handle{}, nameParam.Load(c), blobcache.DefaultLocalSpec())
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *volh)
		return nil
	},
}

var mkVolRemoteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new remote volume",
	},
	Flags: []star.AnyParam{},
	Pos:   []star.AnyParam{nameParam},
	F: func(c star.Context) error {
		return fmt.Errorf("not yet implemented")
	},
}

var mkVolVaultCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new vault volume",
	},
	Flags: []star.AnyParam{},
	Pos:   []star.AnyParam{nameParam},
	F: func(c star.Context) error {
		return fmt.Errorf("not yet implemented")
	},
}
