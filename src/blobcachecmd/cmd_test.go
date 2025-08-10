package blobcachecmd

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGLFS(t *testing.T) {
	stateDir := t.TempDir()
	runCmd(t, []string{"--state", stateDir, "mkvol", "vol1"})
	runCmd(t, []string{"--state", stateDir, "ls"})
	runCmd(t, []string{"--state", stateDir, "glfs", "init", "vol1"})
	runCmd(t, []string{"--state", stateDir, "glfs", "look", "vol1", "/"})

	inputDir := t.TempDir()
	// write file1
	data1 := []byte("hello")
	require.NoError(t, os.WriteFile(filepath.Join(inputDir, "file1"), data1, 0644))
	// put file1
	runCmd(t, []string{"--state", stateDir, "glfs", "import", "vol1", "/file1", filepath.Join(inputDir, "file1")})
	// get file1
	data2 := runCmdGetOut(t, []string{"--state", stateDir, "glfs", "read", "vol1", "/file1"})
	// check file1
	require.Equal(t, data1, data2)
}

func runCmd(t testing.TB, args []string) {
	RunTest(t, nil, "blobcache", args, nil, nil, nil)
}

func runCmdGetOut(t testing.TB, args []string) []byte {
	stdoutBuf := bytes.Buffer{}
	bufw := bufio.NewWriter(&stdoutBuf)
	RunTest(t, nil, "blobcache", args, nil, bufw, nil)
	require.NoError(t, bufw.Flush())
	return stdoutBuf.Bytes()
}
