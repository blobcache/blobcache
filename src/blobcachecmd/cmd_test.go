package blobcachecmd

import (
	"testing"
)

func TestGLFSInit(t *testing.T) {
	stateDir := t.TempDir()
	runCmd(t, []string{"--state", stateDir, "mkvol", "vol1"})
	runCmd(t, []string{"--state", stateDir, "ls"})
	runCmd(t, []string{"--state", stateDir, "glfs", "init", "vol1"})
	runCmd(t, []string{"--state", stateDir, "glfs", "look", "vol1"})
}

func runCmd(t testing.TB, args []string) {
	RunTest(t, nil, "blobcache", args, nil, nil, nil)
}
