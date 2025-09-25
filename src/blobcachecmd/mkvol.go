package blobcachecmd

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/star"
)

var mkVolLocalCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new local volume",
	},
	Flags: []star.Flag{stateDirParam},
	Pos:   []star.Positional{},
	F: func(c star.Context) error {
		s, err := openLocal(c)
		if err != nil {
			return err
		}
		defer s.Close()
		h, err := s.CreateVolume(c.Context, nil, blobcache.DefaultLocalSpec())
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *h)
		return nil
	},
}

var mkVolRemoteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new remote volume",
	},
	Flags: []star.Flag{},
	Pos:   []star.Positional{},
	F: func(c star.Context) error {
		return fmt.Errorf("not yet implemented")
	},
}

var mkVolVaultCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new vault volume",
	},
	Flags: []star.Flag{},
	Pos:   []star.Positional{},
	F: func(c star.Context) error {
		return fmt.Errorf("not yet implemented")
	},
}
