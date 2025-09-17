package blobcachecmd

import (
	"blobcache.io/blobcache/src/bcfuse"
	"blobcache.io/blobcache/src/bcfuse/scheme_glfs"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"blobcache.io/blobcache/src/schema/basicns"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.brendoncarroll.net/star"
)

var fuseMountCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Mount a blobcache volume as a FUSE filesystem",
	},
	Flags: []star.AnyParam{},
	Pos: []star.AnyParam{
		volumeNameParam,
		mountpointParam,
	},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		volName := volumeNameParam.Load(c)
		nsc := basicns.Client{Service: svc}
		volh, err := nsc.OpenAt(c.Context, blobcache.Handle{}, volName, blobcache.Action_ALL)
		if err != nil {
			return err
		}
		db := sqlutil.OpenMemory()
		fsx := bcfuse.New(db, svc, *volh, scheme_glfs.NewScheme())
		fuseSrv, err := fs.Mount(mountpointParam.Load(c), fsx.FUSERoot(), &fs.Options{
			MountOptions: fuse.MountOptions{
				Debug: true,
			},
		})
		if err != nil {
			return err
		}
		fuseSrv.Serve()
		return nil
	},
}

var mountpointParam = star.Param[string]{
	Name:  "mp",
	Parse: star.ParseString,
}
