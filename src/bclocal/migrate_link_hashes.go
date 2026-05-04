package bclocal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

type linkHashRemap struct {
	Old    blobcache.LinkTokenID
	New    blobcache.LinkTokenID
	Target blobcache.OID
}

type linkHashMigrateStats struct {
	Affected  int
	Unchanged int
}

// MigrateLinkTokenHashes rewrites legacy sha3-based link-token hashes to per-volume hashes
// for local volumes whose schema has a matching bcns.Namespace implementation.
func MigrateLinkTokenHashes(ctx context.Context, sys *Service, nss map[string]bcns.Namespace) error {
	oidSalt, err := getOIDSalt(sys.db)
	if err != nil {
		return err
	}
	rootCfg := sys.env.Root.Config()
	if ns, ok := nss[string(rootCfg.Schema.Name)]; ok {
		stats, err := migrateOneNamespaceVolume(ctx, sys, 0, rootCfg, ns)
		if err != nil {
			return fmt.Errorf("volume %s: %w", blobcache.OID{}, err)
		}
		logctx.Info(ctx, "migrated link token hashes",
			zap.Stringer("volume", blobcache.OID{}),
			zap.Int("affected", stats.Affected),
			zap.Int("unchanged", stats.Unchanged),
		)
	}

	sn := sys.db.NewSnapshot()
	defer sn.Close()

	iter, err := sn.NewIter(&pebble.IterOptions{
		LowerBound: pdb.TableLowerBound(dbtab.TID_VOLUMES),
		UpperBound: pdb.TableUpperBound(dbtab.TID_VOLUMES),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		tk, err := pdb.ParseTKey(iter.Key())
		if err != nil {
			return err
		}
		if len(tk.Key) != blobcache.OIDSize {
			return fmt.Errorf("invalid volume key length: %d", len(tk.Key))
		}
		oid := blobcache.OID(tk.Key)
		if oid == (blobcache.OID{}) {
			continue
		}

		ve, err := parseVolumeEntry(tk.Key, iter.Value())
		if err != nil {
			return err
		}
		var volBackend blobcache.VolumeBackend[blobcache.OID]
		if err := json.Unmarshal(ve.Backend, &volBackend); err != nil {
			return err
		}
		if volBackend.Local == nil {
			continue
		}
		ns, ok := nss[string(volBackend.Local.Schema.Name)]
		if !ok {
			continue
		}

		lvid, err := localvol.LocalIDFromOID(oidSalt, oid)
		if err != nil {
			return fmt.Errorf("volume %s: %w", oid, err)
		}

		cfg := blobcache.VolumeConfig(*volBackend.Local)
		stats, err := migrateOneNamespaceVolume(ctx, sys, lvid, cfg, ns)
		if err != nil {
			return fmt.Errorf("volume %s: %w", oid, err)
		}
		logctx.Info(ctx, "migrated link token hashes",
			zap.Stringer("volume", oid),
			zap.Int("affected", stats.Affected),
			zap.Int("unchanged", stats.Unchanged),
		)
	}
	return nil
}

func migrateOneNamespaceVolume(ctx context.Context, sys *Service, lvid localvol.ID, cfg blobcache.VolumeConfig, ns bcns.Namespace) (linkHashMigrateStats, error) {
	stats := linkHashMigrateStats{}
	vol := sys.lvs.UpNoErr(localvol.Params{Key: lvid, Params: cfg})

	entries, err := readNamespaceEntries(ctx, vol, ns)
	if err != nil {
		return stats, err
	}
	if len(entries) == 0 {
		return stats, nil
	}

	links := make(backend.LinkSet)
	if err := vol.ReadLinks(ctx, links); err != nil {
		return stats, err
	}

	remaps := make(map[blobcache.LinkTokenID]linkHashRemap)
	for _, ent := range entries {
		lt := ent.LinkToken()
		oldHash := lt.GetID(blobcache.HashAlgo_SHA3_256)
		newHash := lt.GetID(cfg.HashAlgo)
		if oldHash == newHash {
			stats.Unchanged++
			continue
		}
		if to, exists := links[newHash]; exists && to != lt.Target {
			return stats, fmt.Errorf("hash collision for target %s", lt.Target)
		}
		if prev, exists := remaps[oldHash]; exists {
			if prev.New != newHash || prev.Target != lt.Target {
				return stats, fmt.Errorf("conflicting remap for old hash %x", oldHash)
			}
			continue
		}
		remaps[oldHash] = linkHashRemap{Old: oldHash, New: newHash, Target: lt.Target}
	}
	if len(remaps) == 0 {
		return stats, nil
	}

	keysToDelete, err := findLegacyLinkRows(sys.db, lvid, remaps)
	if err != nil {
		return stats, err
	}
	stats.Affected += len(keysToDelete)
	stats.Unchanged += len(remaps) - len(keysToDelete)

	mvid, err := sys.txSys.AllocateTxID()
	if err != nil {
		return stats, err
	}

	ba := sys.db.NewBatch()
	defer ba.Close()

	for _, k := range keysToDelete {
		if err := ba.Delete(k, nil); err != nil {
			return stats, err
		}
	}

	for _, remap := range remaps {
		k := pdb.MVKey{
			TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
			Key:     slices.Concat(lvid.Marshal(nil), remap.Target[:], remap.New[:]),
			Version: mvid,
		}
		v := remap.New[:]
		if err := ba.Set(k.Marshal(nil), v, nil); err != nil {
			return stats, err
		}
	}

	return stats, ba.Commit(nil)
}

func findLegacyLinkRows(db *pebble.DB, lvid localvol.ID, remaps map[blobcache.LinkTokenID]linkHashRemap) ([][]byte, error) {
	sn := db.NewSnapshot()
	defer sn.Close()

	iter, err := sn.NewIter(&pebble.IterOptions{
		LowerBound: pdb.TKey{TableID: dbtab.TID_LOCAL_VOLUME_LINKS, Key: lvid.Marshal(nil)}.Marshal(nil),
		UpperBound: pdb.TKey{TableID: dbtab.TID_LOCAL_VOLUME_LINKS, Key: pdb.PrefixUpperBound(lvid.Marshal(nil))}.Marshal(nil),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		vl, err := localvol.ParseVolumeLink(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		remap, exists := remaps[vl.Hash]
		if !exists || remap.Target != vl.To {
			continue
		}
		keys = append(keys, bytes.Clone(iter.Key()))
	}
	return keys, nil
}

func readNamespaceEntries(ctx context.Context, vol *localvol.Volume, ns bcns.Namespace) ([]bcns.Entry, error) {
	tx, err := vol.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)

	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return nil, err
	}
	if len(root) == 0 {
		return nil, nil
	}
	return ns.NSList(ctx, nsStore{tx: tx}, root)
}

var _ bcsdk.RO = nsStore{}

type nsStore struct {
	tx backend.Tx
}

func (s nsStore) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return s.tx.Get(ctx, cid, buf, blobcache.GetOpts{})
}

func (s nsStore) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return s.tx.Exists(ctx, cids, dst)
}

func (s nsStore) Hash(data []byte) blobcache.CID {
	return s.tx.HashAlgo().Hash(data)
}

func (s nsStore) MaxSize() int {
	return s.tx.MaxSize()
}
