package glfsport

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"blobcache.io/glfs"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Importer struct {
	Store  cadata.PostExister
	Dir    string
	Filter func(p string) bool
}

func (im *Importer) Import(ctx context.Context, p string) (*glfs.Ref, error) {
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	finfo, err := im.stat(p)
	if err != nil {
		return nil, err
	}
	return im.importPath(ctx, sem, p, finfo)
}

func (im *Importer) importPath(ctx context.Context, sem *semaphore.Weighted, p string, finfo fs.FileInfo) (*glfs.Ref, error) {
	mode := finfo.Mode()
	switch mode.Type() {
	case fs.ModeDir:
		return im.importDir(ctx, sem, p)
	case fs.ModeSymlink:
		return im.importSymlink(ctx, p)
	default:
		if mode.IsRegular() {
			return im.importFile(ctx, p)
		}
		return nil, fmt.Errorf("cannot import irregular file %q", p)
	}
}

func (im *Importer) importDir(ctx context.Context, sem *semaphore.Weighted, p string) (*glfs.Ref, error) {
	dirEnts, err := im.readDir(p)
	if err != nil {
		return nil, err
	}
	// TODO: find out why this context was being cancelled early
	eg, _ := errgroup.WithContext(ctx)
	ents := make([]glfs.TreeEntry, len(dirEnts))
	for i, dirEnt := range dirEnts {
		i := i
		dirEnt := dirEnt
		fn := func() error {
			p2 := path.Join(p, dirEnt.Name())
			finfo, err := dirEnt.Info()
			if err != nil {
				return err
			}
			ref, err := im.importPath(ctx, sem, p2, finfo)
			if err != nil {
				return err
			}
			ents[i] = glfs.TreeEntry{
				FileMode: finfo.Mode(),
				Name:     dirEnt.Name(),
				Ref:      *ref,
			}
			return nil
		}
		if sem.TryAcquire(1) {
			eg.Go(fn)
		} else {
			if err := fn(); err != nil {
				return nil, err
			}
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return glfs.PostTreeSlice(ctx, im.Store, ents)
}

func (im *Importer) importFile(ctx context.Context, p string) (*glfs.Ref, error) {
	f, err := im.readFile(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ref, err := glfs.PostBlob(ctx, im.Store, f)
	if err != nil {
		return nil, err
	}
	return ref, nil
}

func (im *Importer) importSymlink(ctx context.Context, p string) (*glfs.Ref, error) {
	target, err := im.readLink(p)
	if err != nil {
		return nil, err
	}
	return glfs.PostBlob(ctx, im.Store, strings.NewReader(target))
}

func (im *Importer) path(x string) (string, error) {
	if im.Dir == "" {
		panic("glfsport.Importer cannot have empty dir")
	}
	x = glfs.CleanPath(x)
	if im.Filter != nil && !im.Filter(x) {
		return "", fmt.Errorf("path %q is filtered", x)
	}
	return filepath.Join(im.Dir, filepath.FromSlash(x)), nil
}

func (im *Importer) stat(p string) (os.FileInfo, error) {
	p2, err := im.path(p)
	if err != nil {
		return nil, err
	}
	return os.Stat(p2)
}

func (im *Importer) readDir(p string) ([]os.DirEntry, error) {
	p2, err := im.path(p)
	if err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(p2)
	if err != nil {
		return nil, err
	}
	ents2 := ents[:0]
	for _, ent := range ents {
		p2 := path.Join(p, ent.Name())
		if im.Filter != nil && !im.Filter(p2) {
			continue
		}
		ents2 = append(ents2, ent)
	}
	return ents2, nil
}

func (im *Importer) readFile(p string) (*os.File, error) {
	if im.Filter != nil && !im.Filter(p) {
		return nil, fmt.Errorf("path %q is filtered", p)
	}
	p2, err := im.path(p)
	if err != nil {
		return nil, err
	}
	return os.Open(p2)
}

func (im *Importer) readLink(p string) (string, error) {
	p2, err := im.path(p)
	if err != nil {
		return "", err
	}
	return os.Readlink(p2)
}
