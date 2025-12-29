package bclocal

import (
	"crypto/rand"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"github.com/cockroachdb/pebble"
)

func doSnapshot(db *pebble.DB, fn func(*pebble.Snapshot) error) error {
	sn := db.NewSnapshot()
	defer sn.Close()
	return fn(sn)
}

// doRWBatch creates an indexed batch and calls fn with it.
// if fn returns nil, the batch is committed.
func doRWBatch(db *pebble.DB, fn func(*pebble.Batch) error) error {
	ba := db.NewIndexedBatch()
	//defer ba.Close()
	if err := fn(ba); err != nil {
		return err
	}
	return ba.Commit(nil)
}

// keyLocalOIDSalt is the key in the MISC table where the salt for OIDs is stored.
// DO NOT change this value
var keyLocalOIDSalt = pdb.TKey{
	TableID: dbtab.TID_MISC,
	Key:     []byte("LOCAL_OID_SALT"),
}

func getOIDSalt(db *pebble.DB) (*[16]byte, error) {
	val, closer, err := db.Get(keyLocalOIDSalt.Marshal(nil))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	if len(val) != 16 {
		return nil, fmt.Errorf("invalid OID_SALT %q", val)
	}
	ret := [16]byte{}
	copy(ret[:], val)
	return &ret, nil
}

func ensureOIDSalt(db *pebble.DB) (*[16]byte, error) {
	salt, err := getOIDSalt(db)
	if errors.Is(err, pebble.ErrNotFound) {
		// This value does not need to be secret.
		salt := &[16]byte{}
		if _, err := rand.Read(salt[:]); err != nil {
			return nil, err
		}
		if err := db.Set(keyLocalOIDSalt.Marshal(nil), salt[:], nil); err != nil {
			return nil, err
		}
		return getOIDSalt(db)
	} else if err != nil {
		return nil, err
	}
	return salt, nil
}
