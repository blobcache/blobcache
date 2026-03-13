package blobcache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"database/sql/driver"
)

var _ driver.Value = CID{}

// CID is a content identifier.
// It is produced by hashing data.
// CIDs can be used as salts.
// CIDs are cannonically printed in an order-preserving base64 encoding, which distinguishes
// them from OIDs which are printed as hex.
//type CID [CIDSize]byte

// CIDSize is the number of bytes in a CID.
const CIDSize = 32

func ParseCID(s string) (CID, error) {
	var ret CID
	if err := ret.UnmarshalBase64([]byte(s)); err != nil {
		return CID{}, err
	}
	return ret, nil
}

const (
	IDSize = 32
	// Base64Alphabet is used when encoding CIDs as base64 strings.
	// It is a URL and filepath safe encoding, which maintains ordering.
	Base64Alphabet = "-0123456789" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "_" + "abcdefghijklmnopqrstuvwxyz"
)

// CID identifies a particular piece of data
type CID [IDSize]byte

var enc = base64.NewEncoding(Base64Alphabet).WithPadding(base64.NoPadding)

func (id CID) String() string {
	return enc.EncodeToString(id[:])
}

// MarshalBase64 encodes ID using Base64Alphabet
func (id CID) MarshalBase64() ([]byte, error) {
	buf := make([]byte, enc.EncodedLen(len(id)))
	enc.Encode(buf, id[:])
	return buf, nil
}

// UnmarshalBase64 decodes data into the ID using Base64Alphabet
func (id *CID) UnmarshalBase64(data []byte) error {
	n, err := enc.Decode(id[:], data)
	if err != nil {
		return err
	}
	if n != IDSize {
		return errors.New("base64 string is too short")
	}
	return nil
}

func (a CID) Equals(b CID) bool {
	return a.Compare(b) == 0
}

func (a CID) Compare(b CID) int {
	return bytes.Compare(a[:], b[:])
}

func (id CID) IsZero() bool {
	return id == (CID{})
}

func (id CID) MarshalJSON() ([]byte, error) {
	s := enc.EncodeToString(id[:])
	return json.Marshal(s)
}

func (id *CID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	_, err := enc.Decode(id[:], []byte(s))
	return err
}

func (id *CID) Scan(x interface{}) error {
	switch x := x.(type) {
	case []byte:
		if len(x) != 32 {
			return fmt.Errorf("wrong length for blobcache.CID HAVE: %d WANT: %d", len(x), IDSize)
		}
		copy(id[:], x)
		return nil
	default:
		return fmt.Errorf("cannot scan type %T", x)
	}
}

func (id CID) Value() (driver.Value, error) {
	return id[:], nil
}
