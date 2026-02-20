package blobcachecmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/blobcached"
	"github.com/stretchr/testify/require"

	_ "blobcache.io/blobcache/src/schema/jsonns"
)

func TestOwn(t *testing.T) {
	d, apiUrl := blobcached.BGTestDaemon(t)
	env := map[string]string{
		bcclient.EnvBlobcacheAPI: apiUrl,
	}

	// Get the node's peer ID to verify the output
	nodePeerID, err := d.GetPeerID()
	require.NoError(t, err)

	// Use a made-up peer ID for the owner
	ownerPeerID := nodePeerID // reuse node's own ID as the owner for simplicity

	// Run the own command
	out := runCmdGetOut(t, env, []string{"--state", d.StateDir.Name(), "own", ownerPeerID.String()})
	outStr := string(out)
	t.Log(outStr)

	// Verify the output contains expected strings
	require.Contains(t, outStr, "OWN")
	require.Contains(t, outStr, "full access")
	require.Contains(t, outStr, "ns create")
	require.Contains(t, outStr, nodePeerID.String()[:8])

	// Verify the peer was added to the admin group
	pol, err := d.GetPolicy()
	require.NoError(t, err)
	rights := pol.OpenFiat(ownerPeerID, blobcache.OID{})
	require.NotZero(t, rights, "owner should have rights on the root volume")
}

func TestGLFS(t *testing.T) {
	stateDir := t.TempDir()
	_, apiUrl := blobcached.BGTestDaemon(t)
	env := map[string]string{
		bcclient.EnvBlobcacheAPI: apiUrl,
	}

	specJSON, err := json.Marshal(blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	runCmdWithStdin(t, env, []string{"--state", stateDir, "ns", "create", "vol1"}, specJSON)
	runCmd(t, env, []string{"--state", stateDir, "ns", "ls"})

	runCmd(t, env, []string{"glfs", "init", "vol1"})
	runCmd(t, env, []string{"glfs", "look", "vol1", "/"})

	inputDir := t.TempDir()
	// write file1
	data1 := []byte("hello")
	require.NoError(t, os.WriteFile(filepath.Join(inputDir, "file1"), data1, 0644))
	// put file1
	runCmd(t, env, []string{"glfs", "import", "vol1", "/file1", filepath.Join(inputDir, "file1")})
	// get file1
	data2 := runCmdGetOut(t, env, []string{"glfs", "read", "vol1", "/file1"})
	// check file1
	require.Equal(t, data1, data2)
}

func runCmd(t testing.TB, env map[string]string, args []string) {
	RunTest(t, env, "blobcache", args, nil, nil, nil)
}

func runCmdWithStdin(t testing.TB, env map[string]string, args []string, stdinData []byte) {
	stdin := bufio.NewReader(bytes.NewReader(stdinData))
	RunTest(t, env, "blobcache", args, stdin, nil, nil)
}

func runCmdGetOut(t testing.TB, env map[string]string, args []string) []byte {
	stdoutBuf := bytes.Buffer{}
	bufw := bufio.NewWriter(&stdoutBuf)
	RunTest(t, env, "blobcache", args, nil, bufw, nil)
	require.NoError(t, bufw.Flush())
	return stdoutBuf.Bytes()
}
