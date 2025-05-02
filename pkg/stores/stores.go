package stores

import (
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cadata/fsstore"
	"go.brendoncarroll.net/state/posixfs"
	"lukechampine.com/blake3"
)

func Hash(x []byte) cadata.ID {
	return blake3.Sum256(x)
}

const MaxSize = 1 << 21

type Store interface {
	cadata.Store
}

func NewFSStore(fs posixfs.FS) Store {
	return fsstore.New(fs, Hash, MaxSize)
}

func NewMem() Store {
	return cadata.NewMem(Hash, MaxSize)
}
