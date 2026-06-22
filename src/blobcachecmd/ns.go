package blobcachecmd

import (
	"encoding/json"
	"fmt"
	"io"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
)

// EnvVar_NSRoot is the key for the environment variable that holds the root namespace
const EnvVar_NSRoot = bcclient.EnvBlobcacheNSRoot

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
	"lookup": nsResolveCmd,
	"mv":     nsMvCmd,
})

var nsInitCmd = star.Command{
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		if err := nsc.Init(c); err != nil {
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
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		ents, err := nsc.List(c, "")
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
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		var ent bcns.Entry
		found, err := nsc.Get(c, name, &ent)
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
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		return nsc.Delete(c, name)
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
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		return nsc.Put(c, name, subvolh, mask)
	},
}

var nsCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Create a volume and insert it at a specific name in the namespace. Reads VolumeSpec JSON from stdin.",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		logctx.Infof(c.Context, "reading VolumeSpec JSON from stdin")
		data, err := io.ReadAll(c.StdIn)
		if err != nil {
			return err
		}
		var spec blobcache.VolumeSpec
		if err := json.Unmarshal(data, &spec); err != nil {
			return fmt.Errorf("parsing VolumeSpec JSON from stdin: %w", err)
		}
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		_, err = nsc.CreateVolume(c, name, spec)
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
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		volh, err := nsc.Open(c, name, mask)
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", volh)
		c.Printf("NAME: %v\n", name)
		return nil
	},
}

var nsResolveCmd = star.Command{
	Metadata: star.Metadata{
		Short: "resolve a name across namespace volumes",
	},
	Pos: []star.Positional{volNameParam},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		name := volNameParam.Load(c)
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		fqp, err := nsc.Resolve(c, name)
		if err != nil {
			return err
		}
		c.Printf("%v\n", fqp)
		return nil
	},
}

var newNameParam = &star.Required[string]{
	PosName: "new-name",
	Parse:   star.ParseString,
}

var nsMvCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Rename an entry in the namespace",
	},
	Pos: []star.Positional{volNameParam, newNameParam},
	Flags: map[string]star.Flag{
		"nsr": nsRoot,
	},
	F: func(c star.Context) error {
		oldPath := volNameParam.Load(c)
		newPath := newNameParam.Load(c)
		nsc, err := getNS(c)
		if err != nil {
			return err
		}
		if err := nsc.Move(c, oldPath, newPath); err != nil {
			return err
		}
		c.Printf("✓ %s -> %s\n", oldPath, newPath)
		return nil
	},
}

var nsRoot = &star.Optional[blobcache.OID]{
	PosName:  "nsr",
	Parse:    blobcache.ParseOID,
	ShortDoc: "an object id for the root",
}

func openByName(c star.Context) (blobcache.Handle, error) {
	name := volNameParam.Load(c)
	mask := blobcache.Action_ALL
	nsc, err := getNS(c)
	if err != nil {
		return blobcache.Handle{}, err
	}
	return nsc.Open(c, name, mask)
}

var volNameParam = &star.Required[string]{
	PosName: "volume-name",
	Parse:   star.ParseString,
}

func getNS(c star.Context) (bcns.Client, error) {
	rootOID, err := getNSRoot(c)
	if err != nil {
		return bcns.Client{}, err
	}
	bc, err := openService(c)
	if err != nil {
		return bcns.Client{}, err
	}
	return bcns.NewClient(bc, rootOID), nil
}

// getNSRoot returns a handle to the volume containing the root namespace
func getNSRoot(c star.Context) (blobcache.OID, error) {
	if nsr, ok := nsRoot.LoadOpt(c); ok {
		return nsr, nil
	}
	return bcclient.EnvNSRoot()
}
