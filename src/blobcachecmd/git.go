package blobcachecmd

import (
	"blobcache.io/blobcache/src/schema/bcgit"
	"go.brendoncarroll.net/star"
)

var gitCmd = star.NewDir(star.Metadata{
	Short: "Work with git formatted volumes.  Unreleated to the git volume backend",
}, map[string]star.Command{
	"mkremote": gitMkRemote,
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
