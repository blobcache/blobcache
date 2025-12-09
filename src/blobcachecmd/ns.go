package blobcachecmd

import (
	"context"
	"errors"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

const EnvVar_NSRoot = "BLOBCACHE_NSROOT"

var nsCmd = star.NewDir(star.Metadata{
	Short: "perform common operations on namespace volumes",
}, map[string]star.Command{
	"ls":  nsListCmd,
	"get": nsGetCmd,
	"del": nsDeleteCmd,
	"put": nsPutCmd,
})

var nsListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "List blobs in the namespace",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		return doNSOp(c, func() error {
			return nil
		})
	},
}

var nsGetCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Get a blob from the namespace",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		return doNSOp(c, func() error {
			return nil
		})
	},
}

var nsDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Delete a blob from the namespace",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		return nil
	},
}

var nsPutCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Put a blob into the namespace",
	},
	Pos: []star.Positional{volNameParam, volHParam},
	F: func(c star.Context) error {
		return nil
	},
}

var nsRootH = star.Optional[blobcache.Handle]{
	ID: "nsrootH",
	Parse: func(x string) (blobcache.Handle, error) {
		return blobcache.ParseHandle(x)
	},
	ShortDoc: "a handle to a namespace volume",
}

var nsRoot = star.Optional[blobcache.OID]{
	ID: "nsroot",
	Parse: func(x string) (blobcache.OID, error) {
		return blobcache.ParseOID(x)
	},
	ShortDoc: "the OID of a volume containing a namespace",
}

var volNameParam = star.Required[string]{
	ID:    "name",
	Parse: star.ParseString,
}

func doNSOp(c star.Context, fn func() error) error {
	return nil
}

// getNSRoot returns a handle to the volume containing the root namespace
func getNSRoot(ctx context.Context, c star.Context) blobcache.Handle {
	if h, ok := nsRootH.LoadOpt(c); ok {
		return h
	}
	if oid, ok := nsRoot.LoadOpt(c); ok {
		return blobcache.Handle{OID: oid}
	}
	if rootStr, ok := c.Env[EnvVar_NSRoot]; ok {
		var errs error
		if h, err := blobcache.ParseHandle(rootStr); err == nil {
			return h
		} else {
			errs = errors.Join(errs, err)
		}
		if h, err := blobcache.ParseHandle(rootStr); err == nil {
			return h
		} else {
			errs = errors.Join(errs, err)
		}
		logctx.Error(ctx, "could not parse "+EnvVar_NSRoot, zap.Error(errs))
	}
	// Just return the root
	return blobcache.Handle{}
}
