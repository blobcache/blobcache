// Package gitvol implements a Volume in terms of Git
package gitvol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cells"
	"lukechampine.com/blake3"
)

const (
	BlobPrefix = "b"
	RootPath   = "root"
)

type Volume struct {
	storer storage.Storer
	remote *git.Remote
	branch string

	// blobOps tracks the hashes of blobs that have been queued for addition or deletion.
	// The existence of an entry in the map indicates that the blob state in the remote may be stale.
	// If the hash is plumbing.ZeroHash, then the blob is queued for deletion.
	// If it is non-zero, then the blob is queued for addition.
	blobOps map[blobcache.CID]plumbing.Hash

	cells.BytesCellBase
}

func New(storer storage.Storer, remoteURL string, branch string) *Volume {
	remote := git.NewRemote(storer, &config.RemoteConfig{
		Name: "origin",
		URLs: []string{remoteURL},
	})
	return &Volume{
		storer: storer,
		remote: remote,
		branch: branch,

		blobOps: make(map[blobcache.CID]plumbing.Hash),
	}
}

// Reset resets the volume to an empty state, creating a the branch and commit if necessary.
func (v *Volume) Reset(ctx context.Context) error {
	// Create an empty blob for the root file
	blobHash, err := createGitBlob(v.storer, []byte{})
	if err != nil {
		return fmt.Errorf("failed to create empty blob: %w", err)
	}
	// Create a tree with the root file
	entries := sliceIter(object.TreeEntry{
		Name: RootPath,
		Mode: 0644,
		Hash: blobHash,
	})
	treeHash, err := createGitTree(v.storer, entries)
	if err != nil {
		return fmt.Errorf("failed to create tree: %w", err)
	}
	commitHash, err := createGitCommit(v.storer, treeHash, "Initial commit")
	if err != nil {
		return fmt.Errorf("failed to create commit: %w", err)
	}
	if err := v.setRef(ctx, commitHash, nil); err != nil {
		return err
	}
	clear(v.blobOps)
	return nil
}

func (v *Volume) CAS(ctx context.Context, actual *[]byte, prev, next []byte) (bool, error) {
	// check if the next is too large
	if len(next) > 1<<16 {
		return false, cells.ErrTooLarge{}
	}
	// determin the actual value
	prevCommit, err := v.loadCommit(ctx)
	if err != nil {
		return false, err
	}
	var prevCommitHash *plumbing.Hash
	var prevTree *object.Tree
	if prevCommit != nil {
		prevTree, err = prevCommit.Tree()
		if err != nil {
			return false, err
		}
		if err := loadRoot(prevTree, actual); err != nil {
			return false, err
		}

		prevCommitHash = &prevCommit.Hash
	} else {
		*actual = (*actual)[:0]
	}

	if !bytes.Equal(*actual, prev) {
		return false, nil
	}

	cellHash, err := createGitBlob(v.storer, next)
	if err != nil {
		return false, err
	}
	// If the previous tree is not nil, then we need to copy over the blob directory.
	var ents []object.TreeEntry
	if prevTree != nil {
		if blobTree, err := prevTree.Tree(BlobPrefix); err == nil {
			nextBlobTree, err := v.prepareBlobTree(ctx, blobTree)
			if err != nil {
				return false, err
			}
			ents = append(ents, object.TreeEntry{
				Name: BlobPrefix,
				Mode: 0755,
				Hash: nextBlobTree,
			})
		}
	}
	ents = append(ents, object.TreeEntry{
		Name: RootPath,
		Mode: 0644,
		Hash: cellHash,
	})
	treeHash, err := createGitTree(v.storer, sliceIter(ents...))
	if err != nil {
		return false, err
	}

	commitHash, err := createGitCommit(v.storer, treeHash, "_")
	if err != nil {
		return false, err
	}
	if err := v.setRef(ctx, commitHash, prevCommitHash); err != nil {
		if isErrRefRequirement(err) {
			// call Load to set the actual value
			if err := v.load(ctx, actual); err != nil {
				return false, err
			}
			return false, nil
		}
		return false, err
	}
	cells.CopyBytes(actual, next)
	return true, nil
}

// setRef sets the branch reference to the given commit hash and pushes the branch to the remote.
// If prevHash is not nil, it is used as a required remote ref.
func (v *Volume) setRef(ctx context.Context, commitHash plumbing.Hash, prevHash *plumbing.Hash) error {
	// Create/update the branch reference
	ref := plumbing.NewHashReference(
		plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", v.branch)),
		commitHash,
	)
	if err := v.storer.SetReference(ref); err != nil {
		return fmt.Errorf("failed to set branch reference: %w", err)
	}
	var requiredRemoteRefs []config.RefSpec
	if prevHash != nil && false {
		requiredRemoteRefs = []config.RefSpec{config.RefSpec(fmt.Sprintf("%s:%s", prevHash.String(), v.branch))}
	}
	err := v.remote.PushContext(ctx, &git.PushOptions{
		RefSpecs: []config.RefSpec{
			config.RefSpec(fmt.Sprintf("refs/heads/%s:refs/heads/%s", v.branch, v.branch)),
		},
		Atomic:            true,
		Force:             true,
		RequireRemoteRefs: requiredRemoteRefs,
	})
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		err = nil
	}
	return err
}

func (v *Volume) Load(ctx context.Context, dst *[]byte) error {
	if err := v.load(ctx, dst); err != nil {
		return err
	}
	clear(v.blobOps)
	return nil
}

func (v *Volume) load(ctx context.Context, dst *[]byte) error {
	tree, err := v.loadTree(ctx)
	if err != nil {
		return err
	}
	return loadRoot(tree, dst)
}

func (v *Volume) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	if len(data) > v.MaxSize() {
		return blobcache.CID{}, blobcache.ErrTooLarge{BlobSize: len(data), MaxSize: v.MaxSize()}
	}
	id := v.Hash(data)
	if exists, err := v.Exists(ctx, id); err != nil {
		return id, err
	} else if exists {
		return id, nil
	}
	h, err := createGitBlob(v.storer, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	v.blobOps[id] = h
	return id, nil
}

func (v *Volume) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	var blob *object.Blob
	if h, ok := v.blobOps[id]; ok {
		eo, err := v.storer.EncodedObject(plumbing.BlobObject, h)
		if err != nil {
			return 0, err
		}
		blob, err = object.DecodeBlob(eo)
		if err != nil {
			return 0, err
		}
	} else {
		tree, err := v.loadTree(ctx)
		if err != nil {
			return 0, err
		}
		if tree == nil {
			return 0, blobcache.ErrNotFound{Key: id}
		}
		f, err := tree.File(blobPath(id))
		if err != nil {
			if errors.Is(err, object.ErrFileNotFound) {
				return 0, blobcache.ErrNotFound{Key: id}
			}
			return 0, err
		}
		blob = &f.Blob
	}
	rc, err := blob.Reader()
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	return readInto(rc, buf)
}

func (v *Volume) Exists(ctx context.Context, id blobcache.CID) (bool, error) {
	if h, ok := v.blobOps[id]; ok {
		return h != plumbing.ZeroHash, nil
	}
	tree, err := v.loadTree(ctx)
	if err != nil {
		return false, err
	}
	if tree == nil {
		return false, nil
	}
	file, err := tree.File(blobPath(id))
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return false, nil
		}
		return false, err
	}
	v.blobOps[id] = file.Hash
	return true, nil
}

func (v *Volume) List(ctx context.Context, span cadata.Span, buf []blobcache.CID) (int, error) {
	tree, err := v.loadTree(ctx)
	if err != nil {
		return 0, err
	}
	var blobTree *object.Tree
	if tree != nil {
		blobTree, err = tree.Tree(BlobPrefix)
		if err != nil && !errors.Is(err, object.ErrDirectoryNotFound) {
			return 0, err
		}
	}
	treeHash, err := v.prepareBlobTree(ctx, blobTree)
	if err != nil {
		return 0, err
	}
	blobTree, err = object.GetTree(v.storer, treeHash)
	if err != nil {
		return 0, err
	}
	var n int
	for _, ent := range blobTree.Entries {
		if n >= len(buf) {
			break
		}
		var id blobcache.CID
		if err := id.UnmarshalBase64([]byte(ent.Name)); err != nil {
			return 0, err
		}
		if span.Contains(id, cadata.ID.Compare) {
			buf[n] = id
			n++
		}
	}
	return n, nil
}

func (v *Volume) Delete(ctx context.Context, cid blobcache.CID) error {
	v.blobOps[cid] = plumbing.ZeroHash
	return nil
}

func (v *Volume) Hash(data []byte) blobcache.CID {
	return blake3.Sum256(data)
}

func (v *Volume) MaxSize() int {
	return 1 << 21
}

// prepareBlobs takes an existing blob tree (which may be nil) and returns a new blob tree with any queued blobs added.
func (v *Volume) prepareBlobTree(ctx context.Context, prevBlobTree *object.Tree) (plumbing.Hash, error) {
	var entries []object.TreeEntry
	for id, h := range v.blobOps {
		if h == plumbing.ZeroHash {
			continue
		}
		entries = append(entries, object.TreeEntry{
			Name: id.String(),
			Mode: 0644,
			Hash: h,
		})
	}
	slices.SortStableFunc(entries, func(a, b object.TreeEntry) int {
		return strings.Compare(a.Name, b.Name)
	})
	slices.CompactFunc(entries, func(a, b object.TreeEntry) bool {
		return a.Name == b.Name
	})
	return createGitTree(v.storer, sliceIter(entries...))
}

// loadCommit fetches the latest commit from the remote.
func (v *Volume) loadCommit(ctx context.Context) (*object.Commit, error) {
	if err := v.remote.FetchContext(ctx, &git.FetchOptions{
		Force: true,
		RefSpecs: []config.RefSpec{
			config.RefSpec(fmt.Sprintf("refs/heads/%s:refs/heads/%s", v.branch, v.branch)),
		},
		Depth: 1,
	}); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) && !isErrSomeRefsNotUpdated(err) {
		if isErrRemoteEmpty(err) {
			return nil, nil
		}
		return nil, err
	}
	ref, err := v.storer.Reference(plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", v.branch)))
	if err != nil {
		return nil, err
	}
	return object.GetCommit(v.storer, ref.Hash())
}

func (v *Volume) loadTree(ctx context.Context) (*object.Tree, error) {
	commit, err := v.loadCommit(ctx)
	if err != nil {
		return nil, err
	}
	if commit == nil {
		return nil, nil
	}
	return commit.Tree()
}

// loadRoot loads the root data from a  git tree.
// The tree may be nil, in which case the dst is set to an empty slice.
// But if the tree is not nil, it must contain a file at RootPath.
func loadRoot(tree *object.Tree, dst *[]byte) error {
	if tree == nil {
		*dst = (*dst)[:0]
		return nil
	}
	rootFile, err := tree.File(RootPath)
	if err != nil {
		return err
	}
	rc, err := rootFile.Reader()
	if err != nil {
		return err
	}
	defer rc.Close()
	sw := &sliceWriter{Out: *dst, AllowAppend: true}
	if _, err := io.Copy(sw, rc); err != nil {
		return err
	}
	*dst = sw.Bytes()
	return nil
}

func blobPath(id blobcache.CID) string {
	return path.Join(BlobPrefix, id.String())
}

func isErrRemoteEmpty(err error) bool {
	return strings.Contains(err.Error(), "remote repository is empty")
}

func isErrSomeRefsNotUpdated(err error) bool {
	return strings.Contains(err.Error(), "some refs were not updated")
}

func isErrRefRequirement(err error) bool {
	return refRequirementRe.MatchString(err.Error())
}

var refRequirementRe = regexp.MustCompile(`remote ref .+ required to be .+ but is absent`)

func createGitCommit(storer storage.Storer, treeHash plumbing.Hash, message string) (plumbing.Hash, error) {
	// Create the initial commit
	commitObj := storer.NewEncodedObject()
	commitObj.SetType(plumbing.CommitObject)

	w, err := commitObj.Writer()
	if err != nil {
		return plumbing.Hash{}, fmt.Errorf("failed to get commit writer: %w", err)
	}

	now := time.Now()
	commitContent := fmt.Sprintf("tree %s\nauthor gitvol <gitvol@example.com> %d +0000\ncommitter gitvol <gitvol@example.com> %d +0000\n\nInitial commit\n",
		treeHash.String(), now.Unix(), now.Unix())

	if _, err := w.Write([]byte(commitContent)); err != nil {
		w.Close()
		return plumbing.Hash{}, fmt.Errorf("failed to write commit: %w", err)
	}
	if err := w.Close(); err != nil {
		return plumbing.Hash{}, fmt.Errorf("failed to close commit writer: %w", err)
	}

	commitHash, err := storer.SetEncodedObject(commitObj)
	if err != nil {
		return plumbing.Hash{}, fmt.Errorf("failed to create commit: %w", err)
	}
	return commitHash, nil
}

func createGitTree(storer storage.Storer, entries iter.Seq[object.TreeEntry]) (plumbing.Hash, error) {
	treeObj := storer.NewEncodedObject()
	treeObj.SetType(plumbing.TreeObject)

	w, err := treeObj.Writer()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to get tree writer: %w", err)
	}
	defer w.Close()

	// Write tree entries in the correct format
	for entry := range entries {
		// Tree entry format: mode name\0hash
		entryLine := fmt.Sprintf("%o %s\x00", entry.Mode, entry.Name)
		if _, err := w.Write([]byte(entryLine)); err != nil {
			return plumbing.ZeroHash, fmt.Errorf("failed to write tree entry: %w", err)
		}
		if _, err := w.Write(entry.Hash[:]); err != nil {
			return plumbing.ZeroHash, fmt.Errorf("failed to write tree hash: %w", err)
		}
	}

	hash, err := storer.SetEncodedObject(treeObj)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to store tree: %w", err)
	}

	return hash, nil
}

func createGitBlob(storer storage.Storer, data []byte) (plumbing.Hash, error) {
	blobObj := storer.NewEncodedObject()
	blobObj.SetType(plumbing.BlobObject)
	blobObj.SetSize(int64(len(data)))

	if len(data) > 0 {
		w, err := blobObj.Writer()
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("failed to get blob writer: %w", err)
		}
		defer w.Close()
		if _, err := w.Write(data); err != nil {
			return plumbing.ZeroHash, fmt.Errorf("failed to write blob data: %w", err)
		}
	}

	hash, err := storer.SetEncodedObject(blobObj)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to store blob: %w", err)
	}

	return hash, nil
}

type sliceWriter struct {
	Out []byte
	N   int

	AllowAppend bool
}

func (w *sliceWriter) Write(b []byte) (int, error) {
	if w.N+len(b) > len(w.Out) && !w.AllowAppend {
		return 0, fmt.Errorf("sliceWriter: write out of bounds")
	} else if w.AllowAppend {
		w.Out = append(w.Out[:w.N], b...)
		w.N = len(w.Out)
		return len(b), nil
	} else {
		w.N += copy(w.Out[w.N:], b)
		return len(b), nil
	}
}

func (w *sliceWriter) Bytes() []byte {
	return w.Out[:w.N]
}

func readInto(rc io.Reader, buf []byte) (int, error) {
	sw := &sliceWriter{Out: buf}
	if _, err := io.Copy(sw, rc); err != nil {
		return 0, err
	}
	return sw.N, nil
}

func sliceIter[T any](s ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}
