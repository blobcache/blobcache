package bcgit

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/testutil"
	_ "blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
)

var enableGitOut = true

func TestLsRemote(t *testing.T) {
	ctx := testutil.Context(t)
	te := setup(t)
	bc := te.Service

	// prepare volume
	rem := NewRemote(bc, te.Volume)
	refs := []GitRef{
		{Name: "refs/heads/999"},
		{Name: "refs/heads/888"},
		{Name: "refs/heads/777"},
		{Name: "refs/heads/666"},
	}
	require.NoError(t, rem.putRefs(ctx, refs))
	// prepare url
	ep, err := bc.Endpoint(ctx)
	require.NoError(t, err)
	u := blobcache.URL{
		Node: ep.Peer,
		Base: te.Volume.OID,
	}

	gitInit(t, te)
	cmd(t, te, "git", "remote", "add", "test1", FmtURL(u))
	refsActual := gitLsRemote(t, te, "test1")
	t.Log(refsActual)
	require.Equal(t, len(refs), len(refsActual))
}

func TestPushRefs(t *testing.T) {
	ctx := testutil.Context(t)
	te := setup(t)
	bc := te.Service

	gitInit(t, te)
	putFile(t, te, "file1.txt", []byte("hello world"))
	gitCommit(t, te, "file1.txt")
	cmd(t, te, "git", "remote", "add", "test1", FmtURL(te.URL))
	for i := 0; i < 5; i++ {
		gitTag(t, te, fmt.Sprintf("tag-%d", i))
	}
	gitPushAll(t, te, "test1")

	rem := NewRemote(bc, te.Volume)
	it, err := rem.Iterate(ctx)
	require.NoError(t, err)
	require.NoError(t, streams.ForEach(ctx, it, func(gr GitRef) error {
		return nil
	}))
}

func setup(t testing.TB) testEnv {
	_, apiStr := blobcached.BGTestDaemon(t)
	bc := bchttp.NewClient(nil, apiStr)
	wd := t.TempDir()
	testutil.BuildGoExec(t, filepath.Join(wd, "git-remote-bc"), "../../../cmd/git-remote-bc")

	ctx := testutil.Context(t)
	gitVol, err := bc.CreateVolume(ctx, nil, DefaultVolumeSpec())
	require.NoError(t, err)
	ep, err := bc.Endpoint(ctx)
	require.NoError(t, err)
	return testEnv{
		Dir:     wd,
		APIStr:  apiStr,
		Service: bc,
		Volume:  *gitVol,
		URL: blobcache.URL{
			Node: ep.Peer,
			Base: gitVol.OID,
		},
	}
}

type testEnv struct {
	Dir     string
	APIStr  string
	Service *bchttp.Client
	Volume  blobcache.Handle
	URL     blobcache.URL
}

func putFile(t testing.TB, te testEnv, p string, data []byte) {
	p = filepath.Join(te.Dir, p)
	require.NoError(t, os.WriteFile(p, data, 0644))
}

func gitInit(t testing.TB, te testEnv) {
	cmd(t, te, "git", "init", "--object-format=sha256")
}

func gitCommit(t testing.TB, te testEnv, files ...string) {
	cmd(t, te, "git", append([]string{"add"}, files...)...)
	cmd(t, te, "git", "commit", "-m", "commit")
}

func gitPushAll(t testing.TB, te testEnv, remoteName string) {
	// to anyone reading this, don't do this on a real project, always push specific tags
	cmd(t, te, "git", "push", "--tags", remoteName)
}

func gitTag(t testing.TB, te testEnv, tagName string) {
	cmd(t, te, "git", "tag", tagName)
}

func gitLsRemote(t testing.TB, te testEnv, remoteName string) []string {
	lines := strings.Split(string(cmd(t, te, "git", "ls-remote", remoteName)), "\n")
	lines = slices.DeleteFunc(lines, func(x string) bool { return x == "" })
	return lines
}

func cmd(t testing.TB, te testEnv, name string, args ...string) []byte {
	t.Helper()
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	cmd := exec.Command(name, args...)
	cmd.Dir = te.Dir
	cmd.Env = []string{
		"HOME=" + home,
		"PATH=/usr/bin:",
		// TODO: don't use the system blobcache
		"BLOBCACHE_API=" + te.APIStr,
	}
	var stdout bytes.Buffer
	if enableGitOut {
		cmd.Stdout = io.MultiWriter(&stdout, os.Stdout)
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = &stdout
	}

	require.NoError(t, cmd.Run())
	return stdout.Bytes()
}
