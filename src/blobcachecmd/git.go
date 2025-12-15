package blobcachecmd

import (
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcgit"
	"go.brendoncarroll.net/star"
)

var gitCmd = star.NewDir(star.Metadata{
	Short: "Work with git formatted volumes.  Unreleated to the git volume backend",
}, map[string]star.Command{
	"mkremote": gitMkRemote,
	"url-for":  gitURLFor,
})

var gitMkRemote = star.Command{
	Metadata: star.Metadata{
		Short: "Create a new volume and format and format it as a git remote",
	},
	Flags: map[string]star.Flag{
		"host": hostParam,
	},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		host, _ := hostParam.LoadOpt(c)
		volh, err := svc.CreateVolume(c, host, bcgit.DefaultVolumeSpec())
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *volh)
		return nil
	},
}

var gitURLFor = star.Command{
	Metadata: star.Metadata{
		Short: "Create a new volume and format and format it as a git remote",
	},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		volh, err := nsc.OpenAt(c, *nsh, volNameParam.Load(c), blobcache.Action_VOLUME_INSPECT)
		if err != nil {
			return err
		}
		u, err := bcsdk.URLFor(c, svc, *volh)
		if err != nil {
			return err
		}
		c.Printf("%s\n", bcgit.FmtURL(*u))
		return nil
	},
}
