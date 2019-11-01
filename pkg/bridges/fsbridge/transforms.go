package fsbridge

import (
	"github.com/Yawning/chacha20"
	"golang.org/x/crypto/sha3"
)

type Transform func([]byte) []byte

var transforms = map[string]Transform{
	"webfs": WebFSTransform,
}

// WebFSTransform encrypts blobs using the default
// convergent encryption strategy used by WebFS
func WebFSTransform(x []byte) []byte {
	// derive key
	id := sha3.Sum256(x)
	dek := sha3.Sum256(id[:])

	nonce := [chacha20.NonceSize]byte{} // 0s
	ciph, err := chacha20.NewCipher(dek[:], nonce[:])
	if err != nil {
		panic(err)
	}
	ctext := make([]byte, len(x))
	ciph.XORKeyStream(ctext, x)

	return ctext
}
