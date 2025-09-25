package blobcachecmd

import (
	"log"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/basicns"
	"go.brendoncarroll.net/star"
)

var basicnsCmd = star.NewDir(
	star.Metadata{
		Short: "basicns is a simple namespace implementation",
	},
	map[star.Symbol]star.Command{
		"createat": basicnsCreateAtCmd,
		"ls":       basicnsLsCmd,
	},
)

var basicnsCreateAtCmd = star.Command{
	Pos:   []star.Positional{volNameParam},
	Flags: []star.Flag{stateDirParam},
	F: func(c star.Context) error {
		log.Println(c.Extra)
		s, err := openLocal(c)
		if err != nil {
			return err
		}
		defer s.Close()
		nsc := basicns.Client{Service: s}
		volh, err := nsc.CreateAt(c, blobcache.Handle{}, volNameParam.Load(c), blobcache.DefaultLocalSpec())
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v", *volh)
		return nil
	},
}

var basicnsLsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists volumes",
	},
	Flags: []star.Flag{stateDirParam},
	F: func(c star.Context) error {
		s, err := openLocal(c)
		if err != nil {
			return err
		}
		defer s.Close()
		nsc := basicns.Client{Service: s}
		names, err := nsc.ListNames(c, blobcache.Handle{})
		if err != nil {
			return err
		}
		for _, name := range names {
			c.Printf("%v\n", name)
		}
		return nil
	},
}

var volNameParam = star.Required[string]{
	Name:  "name",
	Parse: star.ParseString,
}
