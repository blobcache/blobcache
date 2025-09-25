package blobcachecmd

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
)

func Main() {
	star.Main(rootCmd)
}

func Root() star.Command {
	return rootCmd
}

var rootCmd = star.NewDir(
	star.Metadata{
		Short: "blobcache is content-addressable storage",
	}, map[star.Symbol]star.Command{
		"endpoint":         endpointCmd,
		"daemon":           daemonCmd,
		"daemon-ephemeral": daemonEphemeralCmd,

		// volumes
		"mkvol.local":  mkVolLocalCmd,
		"mkvol.remote": mkVolRemoteCmd,
		"mkvol.vault":  mkVolVaultCmd,
		"mkvol.git":    mkVolGitCmd,
		"ivol":         ivolCmd,
		"clone":        cloneVolCmd,
		"open-fiat":    openFiatCmd,
		"open-from":    openFromCmd,
		"await":        awaitCmd,
		"drop":         dropCmd,

		// transactions
		"begin": beginTxCmd,
		"tx":    txCmd,

		// applications
		"glfs":       glfsCmd,
		"basicns":    basicnsCmd,
		"fuse-mount": fuseMountCmd,
	},
)

var endpointCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the endpoint",
	},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		ep, err := svc.Endpoint(c.Context)
		if err != nil {
			return err
		}
		c.Printf("%s\n", ep.String())
		return nil
	},
}

// openService opens a service
func openService(c star.Context) (blobcache.Service, error) {
	apiUrl := c.Env[bcclient.EnvBlobcacheAPI]
	if apiUrl == "" {
		logctx.Warnf(c.Context, "%s not set, using default=%s", bcclient.EnvBlobcacheAPI, bcclient.DefaultEndpoint)
		apiUrl = bcclient.DefaultEndpoint
	}
	return bcclient.NewClient(apiUrl), nil
}

func RunTest(t testing.TB, env map[string]string, calledAs string, args []string, stdin *bufio.Reader, stdout *bufio.Writer, stderr *bufio.Writer) {
	if stdin == nil {
		stdin = bufio.NewReader(bytes.NewReader([]byte{}))
	}
	if stdout == nil {
		stdout = bufio.NewWriter(io.Discard)
	}
	if stderr == nil {
		stderr = bufio.NewWriter(io.Discard)
	}

	ctx := testutil.Context(t)
	t.Log(calledAs, args)
	err := star.Run(ctx, Root(), env, calledAs, args, stdin, stdout, stderr)
	require.NoError(t, err)
}
