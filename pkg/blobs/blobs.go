package blobs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/zeebo/blake3"
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

func ZeroID() ID { return ID{} }

func (id *ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id[:])
}

func (id *ID) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, id[:])
}

type Blob = []byte

func Hash(data []byte) ID {
	h := blake3.New()
	h.Write(data)
	id := ID{}
	idBytes := h.Sum(make([]byte, 0, 32))
	copy(id[:], idBytes)
	return id
}

func ForEach(ctx context.Context, s Lister, fn func(ID) error) error {
	return forEach(ctx, s, []byte{}, fn)
}

func forEach(ctx context.Context, s Lister, prefix []byte, fn func(ID) error) error {
	ids := make([]ID, 1<<10)
	n, err := s.List(ctx, prefix, ids)
	switch {
	case err == ErrTooMany:
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			if err := forEach(ctx, s, prefix2, fn); err != nil {
				return err
			}
		}
		return nil
	case err != nil:
		return err
	default:
		for _, id := range ids[:n] {
			if err := fn(id); err != nil {
				return err
			}
		}
		return nil
	}
}
