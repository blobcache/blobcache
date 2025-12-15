package blobcachecmd

import (
	"context"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema/bcns"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// EnvVar_NSRoot is the key for the environment variable that holds the root namespace
const EnvVar_NSRoot = "BLOBCACHE_NS_ROOT"

var nsCmd = star.NewDir(star.Metadata{
	Short: "perform common operations on namespace volumes",
}, map[string]star.Command{
	"init":   nsInitCmd,
	"ls":     nsListCmd,
	"get":    nsGetCmd,
	"del":    nsDeleteCmd,
	"put":    nsPutCmd,
	"create": nsCreateCmd,
	"open":   nsOpenCmd,
})

var nsInitCmd = star.Command{
	F: func(c star.Context) error {
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			if err := nsc.Init(c, nsh); err != nil {
				return err
			}
			c.Printf("Namespace successfully initialized.\n\n")
			return nil
		})
	},
}

var nsListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "List blobs in the namespace",
	},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
	},
	F: func(c star.Context) error {
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			ents, err := nsc.List(c, nsh)
			if err != nil {
				return err
			}
			c.Printf("%-32s\t%-8s\t%s\n", "OID", "RIGHTS", "NAME")
			for _, ent := range ents {
				c.Printf("%-32s\t%-8s\t%s\n", ent.Target, ent.Rights, ent.Name)
			}
			return nil
		})
	},
}

var nsGetCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Get a blob from the namespace",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			var ent bcns.Entry
			found, err := nsc.Get(c, nsh, name, &ent)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("namespace does not have entry %s", name)
			}
			c.Printf("%-32s\t%-8s\t%s\n", ent.Target, ent.Rights, ent.Name)
			return nil
		})
	},
}

var nsDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Delete a blob from the namespace",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			return nsc.Delete(c, nsh, name)
		})
	},
}

var nsPutCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Put a blob into the namespace",
	},
	Pos: []star.Positional{volNameParam, volHParam},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
		"mask": maskParam,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		subvolh := volHParam.Load(c)
		mask, ok := maskParam.LoadOpt(c)
		if !ok {
			mask = blobcache.Action_ALL
		}
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			return nsc.Put(c, nsh, name, subvolh, mask)
		})
	},
}

var nsCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Create a blob at a specific location in the namespace",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			_, err := nsc.CreateAt(c, nsh, name, blobcache.DefaultLocalSpec())
			return err
		})
	},
}

var nsOpenCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Open a blob at a specific location in the namespace",
	},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"nsrh": nsRootH,
	},
	Pos: []star.Positional{volNameParam, maskParam},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		mask, ok := maskParam.LoadOpt(c)
		if !ok {
			mask = blobcache.Action_ALL
		}
		return doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
			volh, err := nsc.OpenAt(c, nsh, name, mask)
			if err != nil {
				return err
			}
			c.Printf("Volume successfully created.\n\n")
			c.Printf("HANDLE: %v\n", *volh)
			c.Printf("NAME: %v\n", name)
			return nil
		})
	},
}

func openAt(c star.Context) (*blobcache.Handle, error) {
	name := volNameParam.Load(c)
	mask := blobcache.Action_ALL
	var ret blobcache.Handle
	if err := doNSOp(c, func(nsc bcns.Client, nsh blobcache.Handle) error {
		volh, err := nsc.OpenAt(c, nsh, name, mask)
		if err != nil {
			return err
		}
		ret = *volh
		return nil
	}); err != nil {
		return nil, err
	}
	return &ret, nil
}

var nsRootH = star.Optional[blobcache.Handle]{
	ID: "nsrootH",
	Parse: func(x string) (blobcache.Handle, error) {
		return blobcache.ParseHandle(x)
	},
	ShortDoc: "a handle to a volume containing a namespace",
}

var nsRoot = star.Optional[blobcache.OID]{
	ID: "nsroot",
	Parse: func(x string) (blobcache.OID, error) {
		return blobcache.ParseOID(x)
	},
	ShortDoc: "the OID of a volume containing a namespace",
}

var volNameParam = star.Required[string]{
	ID:    "volname",
	Parse: star.ParseString,
}

func getNS(c star.Context) (*bcns.Client, *blobcache.Handle, error) {
	rooth := getNSRoot(c)
	bc, err := openService(c)
	if err != nil {
		return nil, nil, err
	}
	if rooth.Secret == ([16]byte{}) {
		h, err := bc.OpenFiat(c, rooth.OID, blobcache.Action_ALL)
		if err != nil {
			return nil, nil, err
		}
		rooth = *h
	}
	nsc, err := NSClientForVolume(c, bc, rooth)
	if err != nil {
		return nil, nil, err
	}
	return &nsc, &rooth, nil
}

// TODO: remove this and use getNS instead
func doNSOp(c star.Context, fn func(nsc bcns.Client, volh blobcache.Handle) error) error {
	nsc, rooth, err := getNS(c)
	if err != nil {
		return err
	}
	return fn(*nsc, *rooth)
}

// getNSRoot returns a handle to the volume containing the root namespace
func getNSRoot(c star.Context) blobcache.Handle {
	if h, ok := nsRootH.LoadOpt(c); ok {
		return h
	}
	if oid, ok := nsRoot.LoadOpt(c); ok {
		// when the secret is empty, the Client will call OpenFiat
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
		logctx.Error(c, "could not parse "+EnvVar_NSRoot, zap.Error(errs))
	}
	// Just return the root
	return blobcache.Handle{}
}

// NSClientForVolume returns a NSClient configured with the Schema used by the given volume.
func NSClientForVolume(ctx context.Context, svc blobcache.Service, nsvolh blobcache.Handle) (bcns.Client, error) {
	vinfo, err := svc.InspectVolume(ctx, nsvolh)
	if err != nil {
		return bcns.Client{}, err
	}
	sch, err := schemareg.Factory(vinfo.Schema)
	if err != nil {
		return bcns.Client{}, err
	}
	nssch, ok := sch.(bcns.Namespace)
	if !ok {
		return bcns.Client{}, fmt.Errorf("volume has a non-namespace Schema %v", vinfo.Schema.Name)
	}
	return bcns.Client{
		Service: svc,
		Schema:  nssch,
	}, nil
}
