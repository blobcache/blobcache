package blobcachecmd

import (
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/basicns"
	"go.brendoncarroll.net/star"
)

var basicnsCmd = star.NewDir(
	star.Metadata{
		Short: "basicns is a simple namespace implementation",
	},
	map[string]star.Command{
		"init": bnsInit,
	},
)

var bnsInit = star.Command{
	Metadata: star.Metadata{
		Short: "initializes a basic namespace",
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		s, err := openService(c)
		if err != nil {
			return err
		}
		nsc := schema.NSClient{
			Service:  s,
			Protocol: basicns.Schema{},
		}
		if err := nsc.Init(c, blobcache.Handle{}); err != nil {
			return err
		}
		c.Printf("Namespace successfully initialized.\n\n")
		return nil
	},
}

// var basicnsCreateAtCmd = star.Command{
// 	Pos:   []star.Positional{volNameParam},
// 	Flags: map[string]star.Flag{},
// 	F: func(c star.Context) error {
// 		s, err := openService(c)
// 		if err != nil {
// 			return err
// 		}
// 		nsc := basicns.Client{Service: s}
// 		name := volNameParam.Load(c)
// 		volh, err := nsc.CreateAt(c, blobcache.Handle{}, name, blobcache.DefaultLocalSpec())
// 		if err != nil {
// 			return err
// 		}
// 		c.Printf("Volume successfully created.\n\n")
// 		c.Printf("HANDLE: %v\n", *volh)
// 		c.Printf("NAME: %v\n", name)
// 		return nil
// 	},
// }

// var basicnsLsCmd = star.Command{
// 	Metadata: star.Metadata{
// 		Short: "lists volumes",
// 	},
// 	Flags: map[string]star.Flag{},
// 	F: func(c star.Context) error {
// 		s, err := openService(c)
// 		if err != nil {
// 			return err
// 		}
// 		nsc := basicns.Client{Service: s}
// 		ents, err := nsc.ListEntries(c, blobcache.Handle{})
// 		if err != nil {
// 			return err
// 		}
// 		c.Printf("%-16s\t%-32s\t%s\n", "RIGHTS", "OID", "NAME")
// 		for _, ent := range ents {
// 			c.Printf("%-16v\t%-32s\t%s\n", ent.Rights, ent.Target, ent.Name)
// 		}
// 		return nil
// 	},
// }
