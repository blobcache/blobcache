package bclocal

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcsys"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/exp/sbe"
)

var _ bcsys.MetadataStore = (*mdStore)(nil)

// mdStore implements bcsys.MetadataStore
type mdStore struct {
	db *pebble.DB
}

// Delete implements bcsys.MetadataStore.
func (s *mdStore) Delete(ctx context.Context, oid blobcache.OID) error {
	ve := volumeEntry{
		OID: oid,
	}
	return s.db.Delete(ve.Key(nil), nil)
}

// Get implements bcsys.MetadataStore.
func (s *mdStore) Get(ctx context.Context, oid blobcache.OID, dst *bcsys.VolumeEntry) (bool, error) {
	snp := s.db.NewSnapshot()
	defer snp.Close()
	ve, err := getVolume(snp, oid)
	if err != nil {
		return false, err
	}
	if ve == nil {
		return false, nil
	}
	if err := json.Unmarshal(ve.Backend, &dst.Backend); err != nil {
		return false, err
	}
	dst.OID = oid
	dst.Schema = ve.Schema
	dst.HashAlgo = blobcache.HashAlgo(ve.HashAlgo)
	dst.MaxSize = ve.MaxSize
	dst.Salted = ve.Salted
	return true, nil
}

// Put implements bcsys.MetadataStore.
func (s *mdStore) Put(ctx context.Context, oid blobcache.OID, ve bcsys.VolumeEntry) error {
	w := s.db.NewBatch()
	defer w.Close()

	backendJSON, err := json.Marshal(ve.Backend)
	if err != nil {
		return err
	}
	ve2 := volumeEntry{
		OID: oid,

		Schema:   ve.Schema,
		HashAlgo: string(ve.HashAlgo),
		MaxSize:  ve.MaxSize,
		Backend:  backendJSON,
		Salted:   ve.Salted,
	}
	if err := w.Set(ve2.Key(nil), ve2.Value(nil), nil); err != nil {
		return err
	}
	for range ve.Deps {
		// TODO: set Volume deps in the database
	}
	return w.Commit(nil)
}

// putVolume writes to the VOLUMES table.
// It does not commit the batch.
func putVolume(w pdb.WO, info blobcache.VolumeInfo) error {
	backendJSON, err := json.Marshal(info.Backend)
	if err != nil {
		return err
	}
	ve := volumeEntry{
		OID: info.ID,

		Schema:   info.Schema,
		HashAlgo: string(info.HashAlgo),
		MaxSize:  info.MaxSize,
		Backend:  backendJSON,
		Salted:   info.Salted,
	}
	return w.Set(ve.Key(nil), ve.Value(nil), nil)
}

// getVolume reads a volume entry from the VOLUMES table.
// getVolume returns nil if the volume does not exist.
func getVolume(sn pdb.RO, oid blobcache.OID) (*volumeEntry, error) {
	k := pdb.TKey{TableID: dbtab.TID_VOLUMES, Key: oid[:]}.Marshal(nil)
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
		VolumeConfig: spec.Config(),
		Backend:      blobcache.VolumeBackendToOID(spec),
	}
	if err := putVolume(ba, info); err != nil {
		return nil, err
	}
	return inspectVolume(ba, rootOID)
}

// volumeEntry is an entry in the volumes table.
type volumeEntry struct {
	OID blobcache.OID `db:"id"`

	MaxSize int64                `db:"max_size"`
	Schema  blobcache.SchemaSpec `db:"sch"`
	// TODO: use the HashAlgo type, make sure it serializes correctly for the database.
	HashAlgo string          `db:"hash_algo"`
	Salted   bool            `db:"salted"`
	Backend  json.RawMessage `db:"backend"`
}

// Key appends the key for the volume entry.
func (ve *volumeEntry) Key(out []byte) []byte {
	return pdb.TKey{TableID: dbtab.TID_VOLUMES, Key: ve.OID[:]}.Marshal(out)
}

// Value appends the value for the volume entry.
func (ve *volumeEntry) Value(out []byte) []byte {
	out = binary.LittleEndian.AppendUint32(out, uint32(ve.MaxSize))
	out = append(out, boolToUint8(ve.Salted))
	out = sbe.AppendLP(out, []byte(ve.HashAlgo))
	out = sbe.AppendLP(out, []byte(ve.Schema.Marshal(nil)))
	out = sbe.AppendLP(out, ve.Backend)
	return out
}

func (ve *volumeEntry) Params() blobcache.VolumeConfig {
	return blobcache.VolumeConfig{
		Schema:   ve.Schema,
		HashAlgo: blobcache.HashAlgo(ve.HashAlgo),
		MaxSize:  ve.MaxSize,
		Salted:   ve.Salted,
	}
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
	schemaData, rest, err := sbe.ReadLP(rest)
	if err != nil {
		return nil, err
	}
	var schema blobcache.SchemaSpec
	if err := schema.Unmarshal(schemaData); err != nil {
		return nil, err
	}
	backend, rest, err := sbe.ReadLP(rest)
	if err != nil {
		return nil, err
	}

	return &volumeEntry{
		OID: oid,

		MaxSize:  int64(maxSize),
		Schema:   schema,
		HashAlgo: string(hashAlgo),
		Salted:   salted,
		Backend:  backend,
	}, nil
}

// inspectVolume is like getVolume, but it returns a VolumeInfo.
func inspectVolume(sn pdb.RO, volID blobcache.OID) (*blobcache.VolumeInfo, error) {
	ve, err := getVolume(sn, volID)
	if err != nil {
		return nil, err
	}
	if ve == nil {
		return nil, nil
	}
	var backend blobcache.VolumeBackend[blobcache.OID]
	if err := json.Unmarshal(ve.Backend, &backend); err != nil {
		return nil, err
	}
	volInfo := blobcache.VolumeInfo{
		ID: volID,
		VolumeConfig: blobcache.VolumeConfig{
			Schema:   ve.Schema,
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
func dropVolume(ba pdb.WO, volID blobcache.OID) error {
	k := pdb.TKey{TableID: dbtab.TID_VOLUMES, Key: volID[:]}.Marshal(nil)
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

// volumeDep is a dependency on a volume.
// unlike volumeLink, it does not store the rights
type volumeDep struct {
	From blobcache.OID
	To   blobcache.OID
}

// putVolumeDep puts a row in the VOLUME_DEPS table, and the inverse row in the VOLUME_DEPS_INV table.
func putVolumeDep(ba pdb.WO, fromVolID blobcache.OID, toVolID blobcache.OID) error {
	// forwards
	k := pdb.TKey{TableID: dbtab.TID_VOLUME_DEPS, Key: slices.Concat(fromVolID[:], toVolID[:])}
	v := []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	// inverse
	k = pdb.TKey{TableID: dbtab.TID_VOLUME_DEPS_INV, Key: slices.Concat(toVolID[:], fromVolID[:])}
	v = []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	return nil
}

// deleteVolumeDep deletes a row in the VOLUME_DEPS table, and the inverse row in the VOLUME_DEPS_INV table.
func deleteVolumeDep(ba pdb.WO, fromVolID blobcache.OID, toVolID blobcache.OID) error {
	k := pdb.TKey{TableID: dbtab.TID_VOLUME_DEPS, Key: slices.Concat(fromVolID[:], toVolID[:])}
	if err := ba.Delete(k.Marshal(nil), nil); err != nil {
		return err
	}
	k = pdb.TKey{TableID: dbtab.TID_VOLUME_DEPS_INV, Key: slices.Concat(toVolID[:], fromVolID[:])}
	if err := ba.Delete(k.Marshal(nil), nil); err != nil {
		return err
	}
	return nil
}

// readVolumeDepsTo reads the volume deps to toVolID into dst.
func readVolumeDepsTo(sp pdb.RO, toVolID blobcache.OID, dst map[blobcache.OID]struct{}) error {
	gteq := pdb.TKey{
		TableID: dbtab.TID_VOLUME_DEPS_INV,
		Key:     toVolID[:],
	}
	lt := pdb.TKey{
		TableID: dbtab.TID_VOLUME_DEPS_INV,
		Key:     slices.Concat(toVolID[:], allOnesOID[:]),
	}
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: gteq.Marshal(nil),
		UpperBound: lt.Marshal(nil),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < 2*blobcache.OIDSize {
			return fmt.Errorf("volume dep inverse key too short: %d", len(iter.Key()))
		}
		fromID := blobcache.OID(iter.Key()[:blobcache.OIDSize])
		dst[fromID] = struct{}{}
	}
	return nil
}

var allOnesOID = blobcache.OID(bytes.Repeat([]byte{0xff}, blobcache.OIDSize))
