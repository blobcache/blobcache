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
)

type VolumeLink struct {
	From, To blobcache.OID
	Rights   blobcache.ActionSet
}

// ParseVolumeLink parses an entry from the VOLUME_LINKS table.
func ParseVolumeLink(k, v []byte) (VolumeLink, error) {
	if len(k) < 4 {
		return VolumeLink{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	k = k[4:]
	if len(k) < 2*blobcache.OIDSize {
		return VolumeLink{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	fromID := blobcache.OID(k[:blobcache.OIDSize])
	toID := blobcache.OID(k[blobcache.OIDSize:])
	return VolumeLink{
		From:   fromID,
		To:     toID,
		Rights: blobcache.ActionSet(binary.LittleEndian.Uint64(v)),
	}, nil
}

func PutVolumeLink(ba *pebble.Batch, fromVolID blobcache.OID, toID blobcache.OID, rights blobcache.ActionSet) error {
	// forwards
	k := pdb.TKey{TableID: dbtab.TID_VOLUME_LINKS, Key: slices.Concat(fromVolID[:], toID[:])}
	v := binary.LittleEndian.AppendUint64(nil, uint64(rights))
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	// inverse
	k = pdb.TKey{TableID: dbtab.TID_VOLUME_LINKS_INV, Key: slices.Concat(toID[:], fromVolID[:])}
	v = []byte{}
	if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
		return err
	}
	return nil
}

type LinkSet = map[blobcache.OID]blobcache.ActionSet

// ReadVolumeLinks reads the volume links from volID into dst.
func ReadVolumeLinks(sp pdb.RO, fromVolID blobcache.OID, dst LinkSet) error {
	gteq := pdb.TKey{
		TableID: dbtab.TID_VOLUME_LINKS,
		Key:     fromVolID[:],
	}
	lt := pdb.TKey{
		TableID: dbtab.TID_VOLUME_LINKS,
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

	for iter.First(); iter.Valid(); iter.Next() {
		volLink, err := ParseVolumeLink(iter.Key(), iter.Value())
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
func ReadVolumeLinksTo(sp pdb.RO, toVolID blobcache.OID, dst map[blobcache.OID]struct{}) error {
	gteq := pdb.TKey{
		TableID: dbtab.TID_VOLUME_LINKS_INV,
		Key:     toVolID[:],
	}
	lt := pdb.TKey{
		TableID: dbtab.TID_VOLUME_LINKS_INV,
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
func PutVolumeLinks(ba *pebble.Batch, fromVolID blobcache.OID, links LinkSet) error {
	oldLinks := make(map[blobcache.OID]blobcache.ActionSet)
	if err := ReadVolumeLinks(ba, fromVolID, oldLinks); err != nil {
		return err
	}
	// delete forwards
	gteq := pdb.TKey{
		TableID: dbtab.TID_VOLUME_LINKS,
		Key:     fromVolID[:],
	}
	lt := pdb.TKey{
		TableID: dbtab.TID_VOLUME_LINKS,
		Key:     slices.Concat(fromVolID[:], allOnesOID[:]),
	}
	if err := ba.DeleteRange(gteq.Marshal(nil), lt.Marshal(nil), nil); err != nil {
		return err
	}
	// delete inverse
	for toID := range oldLinks {
		invKey := pdb.TKey{TableID: dbtab.TID_VOLUME_LINKS_INV, Key: slices.Concat(toID[:], fromVolID[:])}
		if err := ba.Delete(invKey.Marshal(nil), nil); err != nil {
			return err
		}
	}

	// add the new links
	for toID, rights := range links {
		if err := PutVolumeLink(ba, fromVolID, toID, rights); err != nil {
			return err
		}
	}
	return nil
}

var allOnesOID = blobcache.OID(bytes.Repeat([]byte{0xff}, blobcache.OIDSize))
