package blobcachecmd

import (
	"path/filepath"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"go.brendoncarroll.net/star"
)

func Main() {
	star.Main(rootCmd)
}

var rootCmd = star.NewDir(
	star.Metadata{
		Short: "blobcache is content-addressable storage",
	}, map[star.Symbol]star.Command{
		"daemon":           daemonCmd,
		"daemon-ephemeral": daemonEphemeralCmd,
		"mkvol":            mkVolCmd,
	},
)

var mkVolCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new volume",
	},
	Flags: []star.IParam{stateDirParam},
	Pos:   []star.IParam{nameParam},
	F: func(c star.Context) error {
		db, err := dbutil.OpenDB(filepath.Join(stateDirParam.Load(c), "blobcache.db"))
		if err != nil {
			return err
		}
		if err := bclocal.SetupDB(c, db); err != nil {
			return err
		}
		s := bclocal.New(bclocal.Env{DB: db})
		volh, err := s.CreateVolume(c, blobcache.DefaultLocalSpec())
		if err != nil {
			return err
		}
		if err := s.PutEntry(c, *volh, nameParam.Load(c), *volh); err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", volh)
		return nil
	},
}

var nameParam = star.Param[string]{
	Name:  "name",
	Parse: star.ParseString,
}
