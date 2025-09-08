package bclocal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sbe"
	"github.com/cockroachdb/pebble"
)

// putVolume writes to the VOLUMES table.
// It does not commit the batch.
func putVolume(db dbWriting, oid blobcache.OID, info blobcache.VolumeInfo) error {
	backendJSON, err := json.Marshal(info.Backend)
	if err != nil {
		return err
	}
	ve := volumeEntry{
		OID: oid,

		Schema:   string(info.Schema),
		HashAlgo: string(info.HashAlgo),
		MaxSize:  info.MaxSize,
		Backend:  backendJSON,
		Schema:   string(info.Schema),
		Salted:   info.Salted,
	}
	return ba.Set(ve.Key(nil), ve.Value(nil), nil)
}

// getVolume reads a volume entry from the VOLUMES table.
// getVolume returns nil if the volume does not exist.
func getVolume(sn dbReading, oid blobcache.OID) (*volumeEntry, error) {
	k := tableKey(nil, tid_VOLUMES, oid[:])
	v, closer, err := sn.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	return parseVolumeEntry(k, v)
}

// ensureRootVolume creates the root volume if it does not exist.
// The batch must be indexed.
func ensureRootVolume(ba *pebble.Batch, spec blobcache.VolumeSpec) (*blobcache.VolumeInfo, error) {
	rootOID := blobcache.OID{}
	ve, err := getVolume(ba, rootOID)
	if err != nil {
		return nil, err
	}
	if ve != nil {
		return inspectVolume(ba, rootOID)
	}
	info := blobcache.VolumeInfo{
		ID:           rootOID,
		VolumeParams: spec.Params(),
		Backend:      blobcache.VolumeBackendToOID(spec),
	}
	if err := putVolume(ba, rootOID, info); err != nil {
		return nil, err
	}
	return inspectVolume(ba, rootOID)
}

// volumeEntry is an entry in the volumes table.
type volumeEntry struct {
	OID blobcache.OID `db:"id"`

	MaxSize int64  `db:"max_size"`
	Schema  string `db:"sch"`
	// TODO: use the HashAlgo type, make sure it serializes correctly for the database.
	HashAlgo string          `db:"hash_algo"`
	Salted   bool            `db:"salted"`
	Backend  json.RawMessage `db:"backend"`
}

// Key appends the key for the volume entry.
func (ve *volumeEntry) Key(out []byte) []byte {
	return tableKey(out, tid_VOLUMES, ve.OID[:])
}

// Value appends the value for the volume entry.
func (ve *volumeEntry) Value(out []byte) []byte {
	out = binary.LittleEndian.AppendUint32(out, uint32(ve.MaxSize))
	out = append(out, boolToUint8(ve.Salted))
	out = sbe.AppendLP(out, []byte(ve.HashAlgo))
	out = sbe.AppendLP(out, []byte(ve.Schema))
	out = sbe.AppendLP(out, ve.Backend)
	return out
}

func parseVolumeEntry(k, v []byte) (*volumeEntry, error) {
	if len(k) < blobcache.OIDSize {
		return nil, fmt.Errorf("volumeRow: key too short: %d", len(k))
	}
	oid := blobcache.OID(k[:blobcache.OIDSize])
	maxSize, rest, err := sbe.ReadUint32(v)
	if err != nil {
		return nil, err
	}
	salted := uint8ToBool(rest[0])
	hashAlgo, rest, err := sbe.ReadLP(rest[1:])
	if err != nil {
		return nil, err
	}
	schema, rest, err := sbe.ReadLP(rest)
	if err != nil {
		return nil, err
	}
	backend, rest, err := sbe.ReadLP(rest)
	if err != nil {
		return nil, err
	}

	return &volumeEntry{
		OID: oid,

		MaxSize:  int64(maxSize),
		Schema:   string(schema),
		HashAlgo: string(hashAlgo),
		Salted:   salted,
		Backend:  backend,
	}, nil
}

// inspectVolume is like getVolume, but it returns a VolumeInfo.
func inspectVolume(sn dbReading, volID blobcache.OID) (*blobcache.VolumeInfo, error) {
	ve, err := getVolume(sn, volID)
	if err != nil {
		return nil, err
	}
	var backend blobcache.VolumeBackend[blobcache.OID]
	if err := json.Unmarshal(ve.Backend, &backend); err != nil {
		return nil, err
	}
	volInfo := blobcache.VolumeInfo{
		ID: volID,
		VolumeParams: blobcache.VolumeParams{
			Schema:   blobcache.Schema(ve.Schema),
			HashAlgo: blobcache.HashAlgo(ve.HashAlgo),
			MaxSize:  ve.MaxSize,
			Salted:   ve.Salted,
		},
		Backend: backend,
	}
	return &volInfo, nil
}

// dropVolume drops a volume from the VOLUMES table.
// It does not delete local volume state
func dropVolume(ba *pebble.Batch, volID blobcache.OID) error {
	k := tableKey(nil, tid_VOLUMES, volID[:])
	if err := ba.Delete(k, nil); err != nil {
		return err
	}
	return nil
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func uint8ToBool(b uint8) bool {
	return b != 0
}
