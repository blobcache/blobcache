package blobcachecmd

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
	"go.brendoncarroll.net/star"
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
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		if err := nsc.Init(c, *nsh); err != nil {
			return err
		}
		c.Printf("Namespace successfully initialized.\n\n")
		return nil
	},
}

var nsListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "List entries in the namespace",
	},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		ents, err := nsc.List(c, *nsh)
		if err != nil {
			return err
		}
		c.Printf("%-32s\t%-8s\t%s\n", "OID", "RIGHTS", "NAME")
		for _, ent := range ents {
			c.Printf("%-32s\t%-8s\t%s\n", ent.Target, ent.Rights, ent.Name)
		}
		return nil
	},
}

var nsGetCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Get an entry from the namespace",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		var ent bcns.Entry
		found, err := nsc.Get(c, *nsh, name, &ent)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("namespace does not have entry %s", name)
		}
		c.Printf("%-32s\t%-8s\t%s\n", ent.Target, ent.Rights, ent.Name)
		return nil
	},
}

var nsDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Delete an entry from the namespace",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		return nsc.Delete(c, *nsh, name)
	},
}

var nsPutCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Put an entry into the namespace",
	},
	Pos: []star.Positional{volNameParam, volHParam},
	Flags: map[string]star.Flag{
		"nsr":  nsRoot,
		"mask": maskParam,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		subvolh := volHParam.Load(c)
		mask, ok := maskParam.LoadOpt(c)
		if !ok {
			mask = blobcache.Action_ALL
		}
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		return nsc.Put(c, *nsh, name, subvolh, mask)
	},
}

var nsCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Create a volume and insert it at a specific name in the namespace",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		_, err = nsc.CreateAt(c, *nsh, name, blobcache.DefaultLocalSpec())
		return err
	},
}

var nsOpenCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Open a Volume at a specific location in the namespace",
	},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	Pos: []star.Positional{volNameParam, maskParam},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		mask, ok := maskParam.LoadOpt(c)
		if !ok {
			mask = blobcache.Action_ALL
		}
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}
		volh, err := nsc.OpenAt(c, *nsh, name, mask)
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *volh)
		c.Printf("NAME: %v\n", name)
		return nil
	},
}

var nsRoot = star.Optional[bcns.Objectish]{
	ID:       "nsroot",
	Parse:    bcns.ParseObjectish,
	ShortDoc: "a handle or object id for the root",
}

func openAt(c star.Context) (*blobcache.Handle, error) {
	name := volNameParam.Load(c)
	mask := blobcache.Action_ALL
	nsc, nsh, err := getNS(c)
	if err != nil {
		return nil, err
	}
	return nsc.OpenAt(c, *nsh, name, mask)
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
	nsvolh, err := rooth.Open(c, bc)
	if err != nil {
		return nil, nil, err
	}
	nsc, err := bcns.ClientForVolume(c, bc, rooth)
	if err != nil {
		return nil, nil, err
	}
	return nsc, nsvolh, nil
}

// getNSRoot returns a handle to the volume containing the root namespace
func getNSRoot(c star.Context) bcns.Objectish {
	if objish, ok := nsRoot.LoadOpt(c); ok {
		return objish
	}
	if rootStr, ok := c.Env[EnvVar_NSRoot]; ok {
		objish, err := bcns.ParseObjectish(rootStr)
		if err == nil {
			return objish
		}
	}
	// Just return the root
	return bcns.Objectish{}
}
