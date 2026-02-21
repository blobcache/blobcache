package localvol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
)

type VolumeLink struct {
	From ID
	To   blobcache.OID
	Hash [32]byte
}

// ParseVolumeLink parses an entry from the LOCAL_VOLUME_LINKS table.
func ParseVolumeLink(k, v []byte) (VolumeLink, error) {
	if len(k) < 4 {
		return VolumeLink{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	k = k[4:]
	if len(k) < 2*blobcache.OIDSize {
		return VolumeLink{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	fromID := binary.BigEndian.Uint64(k[:8])
	toID := blobcache.OID(k[8 : 8+blobcache.OIDSize])
	var h [32]byte
	copy(h[:], k[8+blobcache.OIDSize:])
	return VolumeLink{
		From: ID(fromID),
		To:   toID,
		Hash: h,
	}, nil
}

func (ls *System) putVolumeLink(ba *pebble.Batch, mvid pdb.MVTag, vl VolumeLink) error {
	// forwards
	k := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
		Key:     slices.Concat(vl.From.Marshal(nil), vl.To[:], vl.Hash[:]),
		Version: mvid,
	}
	v := vl.Hash[:]
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	// inverse
	k = pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS_INV,
		Key:     slices.Concat(vl.To[:], vl.From.Marshal(nil)),
	}
	v = []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	return nil
}

// ReadVolumeLinks reads the volume links from volID into dst.
func (ls *System) readVolumeLinks(sp pdb.RO, mvid pdb.MVTag, fromVolID ID, dst backend.LinkSet) error {
	exclude := func(x pdb.MVTag) bool {
		if x == mvid {
			return false
		} else {
			ok, err := ls.txSys.IsActive(sp, x)
			if err != nil {
				return false
			}
			return ok
		}
	}
	gteq := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
		Key:     fromVolID.Marshal(nil),
	}
	lt := pdb.TKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
		Key:     slices.Concat(fromVolID.Marshal(nil), allOnesOID[:]),
	}
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: gteq.Marshal(nil),
		UpperBound: lt.Marshal(nil),
		SkipPoint: func(k []byte) bool {
			mvk, err := pdb.ParseMVKey(k)
			if err != nil {
				return false
			}
			return exclude(mvk.Version)
		},
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		volLink, err := ParseVolumeLink(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		dst[volLink.Hash] = volLink.To
	}
	return nil
}

// readVolumeLinksTo reads the volume links to toVolID into dst.
// It only reads the inverse links.
// To get the rights, read from the VOLUME_LINKS table.
func (ls *System) readVolumeLinksTo(sp pdb.RO, toVolID blobcache.OID, dst map[blobcache.OID]struct{}) error {
	gteq := pdb.TKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS_INV,
		Key:     slices.Concat(toVolID[:], allOnesOID[:]),
	}
	lt := pdb.TKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS_INV,
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

	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) < 2*blobcache.OIDSize {
			return fmt.Errorf("volume link inverse key too short: %d", len(iter.Key()))
		}
		fromID := blobcache.OID(iter.Key()[:blobcache.OIDSize])
		dst[fromID] = struct{}{}
	}
	return nil
}

// putVolumeLinks puts the volume links from fromVolID into links into the database.
// It overwrites the previous links for the volume.
// putVolumeLinks requires that the batch is indexed.
func (sys *System) putVolumeLinks(ba *pebble.Batch, mvid pdb.MVTag, fromVolID ID, links backend.LinkSet) error {
	oldLinks := make(backend.LinkSet)
	if err := sys.readVolumeLinks(ba, mvid, fromVolID, oldLinks); err != nil {
		return err
	}
	// delete forwards
	gteq := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
		Key:     fromVolID.Marshal(nil),
		Version: mvid,
	}
	lt := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
		Key:     slices.Concat(fromVolID.Marshal(nil), allOnesOID[:]),
		Version: mvid,
	}
	if err := ba.DeleteRange(gteq.Marshal(nil), lt.Marshal(nil), nil); err != nil {
		return err
	}
	// delete inverse
	for h, toID := range oldLinks {
		invKey := pdb.MVKey{
			TableID: dbtab.TID_LOCAL_VOLUME_LINKS_INV,
			Key:     slices.Concat(toID[:], fromVolID.Marshal(nil), h[:]),
		}
		if err := ba.Delete(invKey.Marshal(nil), nil); err != nil {
			return err
		}
	}

	// add the new links
	for h, toID := range links {
		vl := VolumeLink{From: fromVolID, To: toID, Hash: h}
		if err := sys.putVolumeLink(ba, mvid, vl); err != nil {
			return err
		}
	}
	return nil
}

var allOnesOID = blobcache.OID(bytes.Repeat([]byte{0xff}, blobcache.OIDSize))
