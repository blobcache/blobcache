package bclocal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sbe"
	"github.com/cockroachdb/pebble"
)

// putVolume writes to the VOLUMES table.
// It does not commit the batch.
func putVolume(w pdb.WO, info blobcache.VolumeInfo) error {
	backendJSON, err := json.Marshal(info.Backend)
	if err != nil {
		return err
	}
	ve := volumeEntry{
		OID: info.ID,

		Schema:   string(info.Schema),
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
	k := pdb.TKey{TableID: tid_VOLUMES, Key: oid[:]}.Marshal(nil)
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
	if err := putVolume(ba, info); err != nil {
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
	return pdb.TKey{TableID: tid_VOLUMES, Key: ve.OID[:]}.Marshal(out)
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
func dropVolume(ba pdb.WO, volID blobcache.OID) error {
	k := pdb.TKey{TableID: tid_VOLUMES, Key: volID[:]}.Marshal(nil)
	if err := ba.Delete(k, nil); err != nil {
		return err
	}
	return nil
}

func cleanupVolumes(db *pebble.DB, keep func(blobcache.OID) bool) error {
	ba := db.NewIndexedBatch()
	defer ba.Close()
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: pdb.TableLowerBound(tid_VOLUMES),
		UpperBound: pdb.TableUpperBound(tid_VOLUMES),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	volumeDeps := make(map[blobcache.OID]struct{})
	volumeLinks := make(map[blobcache.OID]struct{})
	for iter.Next(); iter.Valid(); iter.Next() {
		k, err := pdb.ParseTKey(iter.Key())
		if err != nil {
			return err
		}
		if len(k.Key) < blobcache.OIDSize {
			return fmt.Errorf("volume key too short: %d", len(k.Key))
		}
		if keep(blobcache.OID(k.Key)) {
			continue
		}

		clear(volumeDeps)
		if err := readVolumeDepsTo(ba, blobcache.OID(k.Key), volumeDeps); err != nil {
			return err
		}
		if len(volumeLinks) > 0 {
			continue
		}
		clear(volumeLinks)
		if err := readVolumeLinksTo(ba, blobcache.OID(k.Key), volumeLinks); err != nil {
			return err
		}
		if len(volumeLinks) > 0 {
			continue
		}
		if err := ba.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}
	return ba.Commit(nil)
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

type volumeLink struct {
	From, To blobcache.OID
	Rights   blobcache.ActionSet
}

// parseVolumeLink parses an entry from the VOLUME_LINKS table.
func parseVolumeLink(k, v []byte) (volumeLink, error) {
	if len(k) < 4 {
		return volumeLink{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	k = k[4:]
	if len(k) < 2*blobcache.OIDSize {
		return volumeLink{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	fromID := blobcache.OID(k[:blobcache.OIDSize])
	toID := blobcache.OID(k[blobcache.OIDSize:])
	return volumeLink{
		From:   fromID,
		To:     toID,
		Rights: blobcache.ActionSet(binary.LittleEndian.Uint64(v)),
	}, nil
}

func putVolumeLink(ba *pebble.Batch, fromVolID blobcache.OID, toID blobcache.OID, rights blobcache.ActionSet) error {
	// forwards
	k := pdb.TKey{TableID: tid_VOLUME_LINKS, Key: slices.Concat(fromVolID[:], toID[:])}
	v := binary.LittleEndian.AppendUint64(nil, uint64(rights))
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	// inverse
	k = pdb.TKey{TableID: tid_VOLUME_LINKS_INV, Key: slices.Concat(toID[:], fromVolID[:])}
	v = []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	return nil
}

// putVolumeLinks puts the volume links from fromVolID into links into the database.
// It overwrites the previous links for the volume.
// putVolumeLinks requires that the batch is indexed.
func putVolumeLinks(ba *pebble.Batch, fromVolID blobcache.OID, links linkSet) error {
	oldLinks := make(map[blobcache.OID]blobcache.ActionSet)
	if err := readVolumeLinks(ba, fromVolID, oldLinks); err != nil {
		return err
	}
	// delete forwards
	gteq := pdb.TKey{
		TableID: tid_VOLUME_LINKS,
		Key:     fromVolID[:],
	}
	lt := pdb.TKey{
		TableID: tid_VOLUME_LINKS,
		Key:     slices.Concat(fromVolID[:], allOnesOID[:]),
	}
	if err := ba.DeleteRange(gteq.Marshal(nil), lt.Marshal(nil), nil); err != nil {
		return err
	}
	// delete inverse
	for toID := range oldLinks {
		invKey := pdb.TKey{TableID: tid_VOLUME_LINKS_INV, Key: slices.Concat(toID[:], fromVolID[:])}
		if err := ba.Delete(invKey.Marshal(nil), nil); err != nil {
			return err
		}
	}
	// add the new links
	for toID, rights := range links {
		if err := putVolumeLink(ba, fromVolID, toID, rights); err != nil {
			return err
		}
	}
	return nil
}

// readVolumeLinks reads the volume links from volID into dst.
func readVolumeLinks(sp pdb.RO, fromVolID blobcache.OID, dst linkSet) error {
	gteq := pdb.TKey{
		TableID: tid_VOLUME_LINKS,
		Key:     fromVolID[:],
	}
	lt := pdb.TKey{
		TableID: tid_VOLUME_LINKS,
		Key:     slices.Concat(fromVolID[:], allOnesOID[:]),
	}
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: gteq.Marshal(nil),
		UpperBound: lt.Marshal(nil),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Last(); iter.Valid(); iter.Next() {
		volLink, err := parseVolumeLink(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		dst[volLink.To] = volLink.Rights
	}
	return nil
}

// readVolumeLinksTo reads the volume links to toVolID into dst.
// It only reads the inverse links.
// To get the rights, read from the VOLUME_LINKS table.
func readVolumeLinksTo(sp pdb.RO, toVolID blobcache.OID, dst map[blobcache.OID]struct{}) error {
	gteq := pdb.TKey{
		TableID: tid_VOLUME_LINKS_INV,
		Key:     toVolID[:],
	}
	lt := pdb.TKey{
		TableID: tid_VOLUME_LINKS_INV,
		Key:     pdb.PrefixUpperBound(toVolID[:]),
	}
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: gteq.Marshal(nil),
		UpperBound: lt.Marshal(nil),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Last(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < 2*blobcache.OIDSize {
			return fmt.Errorf("volume link inverse key too short: %d", len(iter.Key()))
		}
		fromID := blobcache.OID(iter.Key()[:blobcache.OIDSize])
		dst[fromID] = struct{}{}
	}
	return nil
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
	k := pdb.TKey{TableID: tid_VOLUME_DEPS, Key: slices.Concat(fromVolID[:], toVolID[:])}
	v := []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	// inverse
	k = pdb.TKey{TableID: tid_VOLUME_DEPS_INV, Key: slices.Concat(toVolID[:], fromVolID[:])}
	v = []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	return nil
}

// deleteVolumeDep deletes a row in the VOLUME_DEPS table, and the inverse row in the VOLUME_DEPS_INV table.
func deleteVolumeDep(ba pdb.WO, fromVolID blobcache.OID, toVolID blobcache.OID) error {
	k := pdb.TKey{TableID: tid_VOLUME_DEPS, Key: slices.Concat(fromVolID[:], toVolID[:])}
	if err := ba.Delete(k.Marshal(nil), nil); err != nil {
		return err
	}
	k = pdb.TKey{TableID: tid_VOLUME_DEPS_INV, Key: slices.Concat(toVolID[:], fromVolID[:])}
	if err := ba.Delete(k.Marshal(nil), nil); err != nil {
		return err
	}
	return nil
}

// readVolumeDepsTo reads the volume deps to toVolID into dst.
func readVolumeDepsTo(sp pdb.RO, toVolID blobcache.OID, dst map[blobcache.OID]struct{}) error {
	gteq := pdb.TKey{
		TableID: tid_VOLUME_DEPS_INV,
		Key:     toVolID[:],
	}
	lt := pdb.TKey{
		TableID: tid_VOLUME_DEPS_INV,
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

	for iter.Last(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < 2*blobcache.OIDSize {
			return fmt.Errorf("volume dep inverse key too short: %d", len(iter.Key()))
		}
		fromID := blobcache.OID(iter.Key()[:blobcache.OIDSize])
		dst[fromID] = struct{}{}
	}
	return nil
}

type linkSet = map[blobcache.OID]blobcache.ActionSet
