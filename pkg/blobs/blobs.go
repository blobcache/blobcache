package blobs

import (
	"bytes"

	"encoding/base64"

	"golang.org/x/crypto/sha3"
)

const IDSize = 32
const MaxSize = 1 << 16

type ID [IDSize]byte

func (id ID) String() string {
	return base64.URLEncoding.EncodeToString(id[:])
}

func (a ID) Equals(b ID) bool {
	return a.Cmp(b) == 0
}

func (a ID) Cmp(b ID) int {
	return bytes.Compare(a[:], b[:])
}

var NullID = ID{}

type Blob = []byte

func Hash(data []byte) ID {
	return ID(sha3.Sum256(data))
}

// func MakeStore(kv KV) Store {
// 	return &kvWrapper{kv}
// }

// type kvWrapper struct {
// 	KV
// }

// func (w *kvWrapper) Post(ctx context.Context, b Blob) (ID, error) {
// 	id := Hash(b)
// 	err := w.KV.Put(ctx, id[:], b)
// 	return id, err
// }

// func (w *kvWrapper) Get(ctx context.Context, id ID) (Blob, error) {
// 	return w.KV.Get(ctx, id[:])
// }

// func (w *kvWrapper) Delete(ctx context.Context, id ID) error {
// 	return w.KV.Delete(ctx, id[:])
// }
