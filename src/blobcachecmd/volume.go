package blobcachecmd

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
)

var mkVolCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new volume from a VolumeSpec JSON on stdin",
	},
	Flags: map[string]star.Flag{
		"host": hostParam,
	},
	F: func(c star.Context) error {
		s, err := openService(c)
		if err != nil {
			return err
		}
		host, _ := hostParam.LoadOpt(c)
		logctx.Infof(c.Context, "reading VolumeSpec JSON from stdin")
		data, err := io.ReadAll(c.StdIn)
		if err != nil {
			return err
		}
		var spec blobcache.VolumeSpec
		if err := json.Unmarshal(data, &spec); err != nil {
			return err
		}
		h, err := s.CreateVolume(c.Context, host, spec)
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *h)
		return nil
	},
}

var mkVolLocalCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new local volume",
	},
	Flags: map[string]star.Flag{
		"host":     hostParam,
		"hash":     hashAlgoParam,
		"max-size": maxSizeParam,
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		s, err := openService(c)
		if err != nil {
			return err
		}
		host, _ := hostParam.LoadOpt(c)
		spec := blobcache.DefaultLocalSpec()
		if ha, ok := hashAlgoParam.LoadOpt(c); ok {
			spec.Local.HashAlgo = ha
		}
		if maxSize, ok := maxSizeParam.LoadOpt(c); ok {
			spec.Local.MaxSize = maxSize
		}
		h, err := s.CreateVolume(c.Context, host, spec)
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *h)
		return nil
	},
}

var mkVolRemoteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new remote volume",
	},
	Pos: []star.Positional{endpointParam, volOIDParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		h, err := svc.CreateVolume(c.Context, nil, blobcache.VolumeSpec{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: endpointParam.Load(c),
				Volume:   volOIDParam.Load(c),
			},
		})
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *h)
		return nil
	},
}

var mkVolVaultCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new vault volume",
	},
	Flags: map[string]star.Flag{
		"hash":   hashAlgoParam,
		"secret": secretParam,
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}

		hashAlgo := blobcache.HashAlgo_BLAKE3_256
		if ha, ok := hashAlgoParam.LoadOpt(c); ok {
			hashAlgo = ha
		}

		secret := blobcache.Secret{}
		if s, ok := secretParam.LoadOpt(c); ok {
			secret = s
		}

		h, err := svc.CreateVolume(c.Context, nil, blobcache.VolumeSpec{
			Vault: &blobcache.VolumeBackend_Vault[blobcache.Handle]{
				X:        volHParam.Load(c),
				HashAlgo: hashAlgo,
				Secret:   secret,
			},
		})
		if err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", *h)
		return nil
	},
}

var mkVolGitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new git volume",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		return fmt.Errorf("not yet implemented")
	},
}

var awaitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "await for a volume to change",
	},
	Pos: []star.Positional{volHParam},
	F: func(c star.Context) error {
		_, err := openService(c)
		if err != nil {
			return err
		}
		return fmt.Errorf("not yet implemented")
	},
}

var ivolCmd = star.Command{
	Metadata: star.Metadata{Short: "inspect a volume and print JSON"},
	Pos:      []star.Positional{volHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		vi, err := svc.InspectVolume(c.Context, volHParam.Load(c))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(c.StdOut)
		enc.SetIndent("", "  ")
		return enc.Encode(vi)
	},
}

var cloneVolCmd = star.Command{
	Metadata: star.Metadata{Short: "clone a volume and print handle"},
	Pos:      []star.Positional{volHParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		h, err := svc.CloneVolume(c.Context, nil, volHParam.Load(c))
		if err != nil {
			return err
		}
		c.Printf("%s\n", h.String())
		return nil
	},
}

var openFiatCmd = star.Command{
	Metadata: star.Metadata{
		Short: "opens a handle to an object by OID",
	},
	Pos: []star.Positional{oidParam, maskParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		mask, maskOK := maskParam.LoadOpt(c)
		if !maskOK {
			mask = blobcache.Action_ALL
		}
		h, err := svc.OpenFiat(c.Context, oidParam.Load(c), mask)
		if err != nil {
			return err
		}
		printOK(c, "OPEN FIAT")
		fmt.Fprintf(c.StdOut, "HANDLE: %s\n", h.String())
		return nil
	},
}

var openFromCmd = star.Command{
	Metadata: star.Metadata{
		Short: "opens a handle to an object from a base volume",
	},
	Pos: []star.Positional{volHParam, linkTokenParam, maskParam},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		mask, maskOK := maskParam.LoadOpt(c)
		if !maskOK {
			mask = blobcache.Action_ALL
		}
		h, err := svc.OpenFrom(c.Context, volHParam.Load(c), linkTokenParam.Load(c), mask)
		if err != nil {
			return err
		}
		printOK(c, "OPEN FROM")
		fmt.Fprintf(c.StdOut, "HANDLE: %s\n", h.String())
		return nil
	},
}

var oidParam = star.Required[blobcache.OID]{
	ID:       "oid",
	ShortDoc: "an object identifier (OID)",
	Parse:    blobcache.ParseOID,
}

// hostParam is for rerouting CreateVolume calls to a remote Node.
// It is a pointer so that the zero value works out to nil.
var hostParam = star.Optional[*blobcache.Endpoint]{
	ID:       "host",
	ShortDoc: "the endpoint of the node to perform the operation on",
	Parse: func(s string) (*blobcache.Endpoint, error) {
		ep, err := blobcache.ParseEndpoint(s)
		return &ep, err
	},
}

var endpointParam = star.Required[blobcache.Endpoint]{
	ID:       "endpoint",
	ShortDoc: "an endpoint for connecting to another node",
	Parse:    blobcache.ParseEndpoint,
}

var volOIDParam = star.Required[blobcache.OID]{
	ID:       "volid",
	ShortDoc: "the OID of a volume",
	Parse:    blobcache.ParseOID,
}

// maskParam is an ActionSet encoded as a hex string.
var maskParam = star.Optional[blobcache.ActionSet]{
	ID:       "mask",
	ShortDoc: "a bitwise mask to AND with an ActionSet",
	Parse: func(s string) (blobcache.ActionSet, error) {
		n, err := strconv.ParseUint(s, 16, 64)
		if err != nil {
			return 0, err
		}
		return blobcache.ActionSet(n), err
	},
}

var hashAlgoParam = star.Optional[blobcache.HashAlgo]{
	ID:       "hash",
	ShortDoc: "the hash algorithm to use",
	Parse: func(s string) (blobcache.HashAlgo, error) {
		ha := blobcache.HashAlgo(s)
		if err := ha.Validate(); err != nil {
			return "", err
		}
		return ha, nil
	},
}

var maxSizeParam = star.Optional[int64]{
	ID:       "max-size",
	ShortDoc: "the maximum size of blobs in the volume",
	Parse: func(s string) (int64, error) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, err
		}
		return n, nil
	},
}

var secretParam = star.Optional[blobcache.Secret]{
	ID:       "secret",
	ShortDoc: "the secret for vault encryption (hex string)",
	Parse: func(s string) (blobcache.Secret, error) {
		var secret blobcache.Secret
		err := secret.UnmarshalText([]byte(s))
		return secret, err
	},
}

var linkTokenParam = star.Required[blobcache.LinkToken]{
	ID:       "link-token",
	ShortDoc: "a token that proves access from one volume to another",
	Parse: func(s string) (blobcache.LinkToken, error) {
		data, err := hex.DecodeString(s)
		if err != nil {
			return blobcache.LinkToken{}, err
		}
		var ltok blobcache.LinkToken
		if err := ltok.Unmarshal(data); err != nil {
			return blobcache.LinkToken{}, err
		}
		return ltok, nil
	},
}
