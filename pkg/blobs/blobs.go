package blobs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/zeebo/blake3"
)

const IDSize = 32
const MaxSize = 1 << 16

type ID [IDSize]byte

func (id ID) String() string {
	return base64.RawURLEncoding.EncodeToString(id[:])
}

func (id *ID) UnmarshalB64(data []byte) error {
	n, err := base64.RawURLEncoding.Decode(id[:], data)
	if err != nil {
		return err
	}
	if n != IDSize {
		return errors.New("base64 string is too short")
	}
	return nil
}

func (a ID) Equals(b ID) bool {
	return a.Cmp(b) == 0
}

func (a ID) Cmp(b ID) int {
	return bytes.Compare(a[:], b[:])
}

func ZeroID() ID { return ID{} }

func (id ID) MarshalJSON() ([]byte, error) {
	s := base64.RawURLEncoding.EncodeToString(id[:])
	return json.Marshal(s)
}

func (id *ID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	_, err := base64.RawURLEncoding.Decode(id[:], []byte(s))
	return err
}

type Blob = []byte

func Hash(data []byte) ID {
	h := blake3.New()
	h.Write(data)
	id := ID{}
	idBytes := h.Sum(nil)
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
