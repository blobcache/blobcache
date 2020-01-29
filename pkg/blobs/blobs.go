package blobs

import (
	"bytes"

	"encoding/base64"
	"encoding/json"

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

func ZeroID() ID { return ID{} }

func (id *ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id[:])
}

func (id *ID) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, id[:])
}

type Blob = []byte

func Hash(data []byte) ID {
	return ID(sha3.Sum256(data))
}
