package blobcache

import (
	"crypto/sha256"
	"crypto/sha3"
	"fmt"

	"golang.org/x/crypto/blake2b"
	"lukechampine.com/blake3"
)

// HashFunc is a cryptographic hash function.
type HashFunc func(salt *CID, data []byte) CID

// HashAlgo is a cryptographic hash algorithm.
type HashAlgo string

const (
	HashAlgo_BLAKE3_256  HashAlgo = "blake3-256"
	HashAlgo_BLAKE2b_256 HashAlgo = "blake2b-256"
	HashAlgo_SHA2_256    HashAlgo = "sha2-256"
	HashAlgo_SHA3_256    HashAlgo = "sha3-256"
	HashAlgo_CSHAKE256   HashAlgo = "cshake256"
)

// IsKeyed returns true if the hash algorithm supports a key/salt.
func (h HashAlgo) IsKeyed() bool {
	switch h {
	case HashAlgo_BLAKE3_256, HashAlgo_BLAKE2b_256:
		return true
	case HashAlgo_SHA3_256, HashAlgo_CSHAKE256:
		return true
	case HashAlgo_SHA2_256:
		return false
	default:
		return false
	}
}

func (h HashAlgo) Validate() error {
	switch h {
	case HashAlgo_BLAKE3_256, HashAlgo_BLAKE2b_256:
		fallthrough
	case HashAlgo_SHA2_256:
		fallthrough
	case HashAlgo_SHA3_256, HashAlgo_CSHAKE256:
		return nil
	}
	return fmt.Errorf("unknown hash algo: %q", h)
}

func (h HashAlgo) HashFunc() HashFunc {
	switch h {
	case HashAlgo_SHA2_256:
		return func(salt *CID, x []byte) CID {
			if salt != nil {
				panic("salt not supported for sha2-256")
			}
			return sha256.Sum256(x)
		}
	case HashAlgo_SHA3_256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {
				return sha3.Sum256(x)
			}
			h := sha3.NewCSHAKE256(nil, salt[:])
			h.Write(x)
			var ret CID
			if _, err := h.Read(ret[:]); err != nil {
				panic(err)
			}
			return ret
		}
	case HashAlgo_CSHAKE256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {
				return CID(sha3.SumSHAKE256(x, 32))
			} else {
				h := sha3.NewCSHAKE256(nil, salt[:])
				h.Write(x)
				var ret CID
				if _, err := h.Read(ret[:]); err != nil {
					panic(err)
				}
				return ret
			}
		}
	case HashAlgo_BLAKE2b_256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {
				return blake2b.Sum256(x)
			}
			h, err := blake2b.New(32, salt[:])
			if err != nil {
				panic(err)
			}
			h.Write(x)
			var ret CID
			copy(ret[:], h.Sum(nil))
			return ret
		}
	case HashAlgo_BLAKE3_256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {
				return blake3.Sum256(x)
			}
			h := blake3.New(32, salt[:])
			h.Write(x)
			var ret CID
			copy(ret[:], h.Sum(nil))
			return ret
		}
	default:
		panic(h)
	}
}

// AEADAlgo is an algorithm for Authenticated Encryption with Associated Data (AEAD).
type AEADAlgo string

const (
	AEAD_CHACHA20POLY1305 AEADAlgo = "chacha20poly1305"
)

func (a AEADAlgo) Validate() error {
	switch a {
	case AEAD_CHACHA20POLY1305:
	default:
		return fmt.Errorf("unknown aead algo: %s", a)
	}
	return nil
}
