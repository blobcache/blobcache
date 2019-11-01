package fsbridge

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	cbor "github.com/brianolson/cbor_go"
	"golang.org/x/crypto/sha3"
)

type PutGet interface {
	Put(ctx context.Context, k, v []byte) error
	Get(ctx context.Context, k []byte) ([]byte, error)
}

type Spec struct {
	Path       string
	Transforms []string
}

type Entry struct {
	Path      string `cbor:"p"`
	Offset    int64  `cbor:"o"`
	Length    int    `cbor:"l"`
	Transform string `cbor:"t"`
}

type Bridge struct {
	spec  Spec
	store PutGet
}

func New(spec Spec, store PutGet) *Bridge {
	return &Bridge{
		store: store,
		spec:  spec,
	}
}

func (b *Bridge) Run(ctx context.Context) error {
	const period = 5 * time.Minute
	ticker := time.NewTicker(period)

	if err := b.Index(ctx, b.spec.Path); err != nil {
		log.Println(err)
	}
	for {
		select {
		case <-ticker.C:
			if err := b.Index(ctx, b.spec.Path); err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *Bridge) Index(ctx context.Context, p string) (err error) {
	log.Println("fs_bridge: start indexing", p)
	if b.indexPath(ctx, p); err != nil {
		return err
	}
	log.Println("fs_bridge: done indexing", p)
	return nil
}

func (b *Bridge) indexPath(ctx context.Context, p string) error {
	finfo, err := os.Stat(p)
	if err != nil {
		return err
	}
	if finfo.IsDir() {
		return b.indexDir(ctx, p)
	}

	t, err := b.whenImported(ctx, p)
	if err != nil {
		return err
	}
	if t != nil && !finfo.ModTime().After(*t) {
		return nil
	}
	return b.indexFile(ctx, p)
}

func (b *Bridge) indexDir(ctx context.Context, p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	finfos, err := f.Readdir(-1)
	if err != nil {
		return err
	}
	for _, finfo := range finfos {
		p2 := filepath.Join(p, finfo.Name())
		if err := b.indexPath(ctx, p2); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bridge) indexFile(ctx context.Context, p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	log.Println("fs bridge: indexing file", p)
	chunker := &FixedSizeChunker{r: f, size: blobs.MaxSize}
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return err
		}

		for _, tranName := range b.spec.Transforms {
			data := chunk.Data
			data1 := transforms[tranName](data)

			id := sha3.Sum256(data1)
			e := Entry{
				Offset:    chunk.Offset,
				Path:      p,
				Transform: tranName,
				Length:    len(data1),
			}
			if err := b.putEntry(ctx, id[:], e); err != nil {
				return err
			}
		}

		if err == io.EOF {
			break
		}
	}

	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	modTime := finfo.ModTime()
	if err := b.markImported(ctx, p, modTime); err != nil {
		return err
	}
	return nil
}

func (b *Bridge) markImported(ctx context.Context, p string, t time.Time) error {
	v, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return b.store.Put(ctx, []byte(p), v)
}

func (b *Bridge) whenImported(ctx context.Context, p string) (*time.Time, error) {
	v, err := b.store.Get(ctx, []byte(p))
	if err != nil {
		return nil, err
	}
	if len(v) < 1 {
		return nil, nil
	}
	t := &time.Time{}
	if err := json.Unmarshal(v, t); err != nil {
		return nil, err
	}
	return t, nil
}

func (b *Bridge) putEntry(ctx context.Context, key []byte, e Entry) error {
	value, err := cbor.Dumps(e)
	if err != nil {
		return err
	}
	return b.store.Put(ctx, key, value)
}

func (b *Bridge) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	value, err := b.store.Get(ctx, id[:])
	ent := Entry{}
	if err := cbor.Loads(value, &ent); err != nil {
		return nil, err
	}
	f, err := os.Open(ent.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(ent.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	r := io.LimitReader(f, int64(ent.Length))
	return ioutil.ReadAll(r)
}

func (b *Bridge) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	x, err := b.store.Get(ctx, id[:])
	if err != nil {
		return false, err
	}
	return len(x) > 0, nil
}
