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

func TestPushTags(t *testing.T) {
	ctx := testutil.Context(t)
	te := setup(t)
	bc := te.Service

	rem := NewRemote(bc, te.Volume)
	gitInit(t, te)
	cmd(t, te, "git", "remote", "add", "test1", FmtURL(te.URL))

	putFile(t, te, "file1.txt", fmt.Appendf(nil, "hello world"))
	gitCommit(t, te, "file1.txt")
	for i := 0; i < 5; i++ {
		gitTag(t, te, fmt.Sprintf("tag-%d", i))
	}
	gitPushAllTags(t, te, "test1")

	it, err := rem.OpenIterator(ctx)
	require.NoError(t, err)
	actualRefs, err := streams.Collect(ctx, it, 1<<20)
	require.NoError(t, err)
	require.Len(t, actualRefs, 5)
	require.NoError(t, it.Close())
}

func TestPushBranch(t *testing.T) {
	ctx := testutil.Context(t)
	te := setup(t)
	bc := te.Service

	rem := NewRemote(bc, te.Volume)
	gitInit(t, te)
	cmd(t, te, "git", "remote", "add", "test1", FmtURL(te.URL))
	for i := range 10 {
		putFile(t, te, "file1.txt", fmt.Appendf(nil, "hello world %d", i))
		gitCommit(t, te, "file1.txt")
		gitPush(t, te, "test1", "master")
	}

	// check what's there
	it, err := rem.OpenIterator(ctx)
	require.NoError(t, err)
	actualRefs, err := streams.Collect(ctx, it, 1<<20)
	require.NoError(t, err)
	require.Len(t, actualRefs, 1)
	require.NoError(t, it.Close())
}

func TestPushPull(t *testing.T) {
	// ctx := testutil.Context(t)
	te1 := setup(t)
	te2 := te1.fork(t)

	// git init in both repos
	for _, te := range []testEnv{te1, te2} {
		gitInit(t, te)
		cmd(t, te, "git", "remote", "add", "test1", FmtURL(te.URL))
	}
	var content []byte
	for i := range 3 {
		content = fmt.Appendf(nil, "hello world %d", i)
		putFile(t, te1, "file1.txt", content)
		gitCommit(t, te1, "file1.txt")
		gitPush(t, te1, "test1", "master")
	}

	gitPull(t, te2, "test1", "master")
	data, err := os.ReadFile(filepath.Join(te2.Dir, "file1.txt"))
	require.NoError(t, err)
	require.Equal(t, string(content), string(data))
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

func (te testEnv) fork(t testing.TB) testEnv {
	te2 := te

	te2.Dir = t.TempDir()
	copyFile(t,
		filepath.Join(te.Dir, "git-remote-bc"),
		filepath.Join(te2.Dir, "git-remote-bc"),
	)
	return te2
}

func copyFile(t testing.TB, src, dst string) {
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, data, 0o755))
}

func putFile(t testing.TB, te testEnv, p string, data []byte) {
	p = filepath.Join(te.Dir, p)
	require.NoError(t, os.WriteFile(p, data, 0644))
}

func gitInit(t testing.TB, te testEnv) {
	cmd(t, te, "git", "init",
		"--object-format=sha256",
		"-b", "master",
	)
	out := cmd(t, te, "git", "config", "extensions.objectFormat")
	require.Equal(t, string(bytes.TrimSpace(out)), "sha256")
}

func gitCommit(t testing.TB, te testEnv, files ...string) {
	cmd(t, te, "git", append([]string{"add"}, files...)...)
	cmd(t, te, "git", "commit", "-m", "commit")
}

func gitPush(t testing.TB, te testEnv, remoteName, branch string) {
	cmd(t, te, "git", "push", "-u", remoteName, branch)
}

func gitPushAllTags(t testing.TB, te testEnv, remoteName string) {
	// to anyone reading this, don't do this on a real project, always push specific tags
	cmd(t, te, "git", "push", "--tags", "-f", remoteName)
}

func gitTag(t testing.TB, te testEnv, tagName string) {
	cmd(t, te, "git", "tag", tagName)
}

func gitLsRemote(t testing.TB, te testEnv, remoteName string) []string {
	lines := strings.Split(string(cmd(t, te, "git", "ls-remote", remoteName)), "\n")
	lines = slices.DeleteFunc(lines, func(x string) bool { return x == "" })
	return lines
}

func gitPull(t testing.TB, te testEnv, remote, branch string) {
	cmd(t, te, "git", "pull", remote, branch)
}

func cmd(t testing.TB, te testEnv, name string, args ...string) []byte {
	t.Helper()
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	cmd := exec.Command(name, args...)
	cmd.Dir = te.Dir
	cmd.Env = []string{
		"HOME=" + home,   // TODO: this is to make git shut up about name + email.
		"PATH=/usr/bin:", // the trailing colon is very important.
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
