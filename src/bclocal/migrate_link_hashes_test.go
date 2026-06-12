package bclocal

import (
	"context"
	"slices"
	"testing"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/jsonns"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestMigrateLinkTokenHashes(t *testing.T) {
	ctx := context.Background()
	svc := NewTestService(t)

	target, err := svc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	nsc := bcns.Client{Service: svc, Schema: jsonns.Schema{}}
	require.NoError(t, nsc.Put(ctx, blobcache.Handle{}, "legacy-link", *target, blobcache.Action_ALL))
	root, err := svc.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
	require.NoError(t, err)
	rootInfo, err := svc.InspectVolume(ctx, *root)
	require.NoError(t, err)
	require.Equal(t, jsonns.SchemaName, rootInfo.Schema.Name)

	var ent bcns.Entry
	found, err := nsc.Get(ctx, blobcache.Handle{}, "legacy-link", &ent)
	require.NoError(t, err)
	require.True(t, found)

	lt := ent.LinkToken()
	oldHash := lt.GetID(blobcache.HashAlgo_SHA3_256)
	newHash := lt.GetID(blobcache.DefaultVolumeParams().HashAlgo)
	require.NotEqual(t, oldHash, newHash)

	require.NoError(t, rewriteLinkHashForVolume(t, svc, 0, lt.Target, newHash, oldHash))
	oldRows, err := collectLinkKeys(svc.db, 0, lt.Target, oldHash)
	require.NoError(t, err)
	require.NotEmpty(t, oldRows)
	newRows, err := collectLinkKeys(svc.db, 0, lt.Target, newHash)
	require.NoError(t, err)
	require.Empty(t, newRows)

	_, err = svc.OpenFrom(ctx, *root, lt, blobcache.Action_ALL)
	require.Error(t, err)

	require.NoError(t, MigrateLinkTokenHashes(ctx, svc, map[string]bcns.Namespace{
		string(jsonns.SchemaName): jsonns.Schema{},
	}))

	oldRows, err = collectLinkKeys(svc.db, 0, lt.Target, oldHash)
	require.NoError(t, err)
	require.Empty(t, oldRows)
	newRows, err = collectLinkKeys(svc.db, 0, lt.Target, newHash)
	require.NoError(t, err)
	require.NotEmpty(t, newRows)

	_, err = svc.OpenFrom(ctx, *root, lt, blobcache.Action_ALL)
	require.NoError(t, err)
}

func rewriteLinkHashForVolume(t testing.TB, svc *Service, lvid localvol.ID, target blobcache.OID, fromHash, toHash blobcache.LinkID) error {
	t.Helper()

	deleteKeys, err := collectLinkKeys(svc.db, lvid, target, fromHash)
	if err != nil {
		return err
	}
	require.NotEmpty(t, deleteKeys)

	mvid, err := svc.txSys.AllocateTxID()
	if err != nil {
		return err
	}

	ba := svc.db.NewBatch()
	defer ba.Close()

	for _, k := range deleteKeys {
		if err := ba.Delete(k, nil); err != nil {
			return err
		}
	}

	k := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_LINKS,
		Key:     slices.Concat(lvid.Marshal(nil), target[:], toHash[:]),
		Version: mvid,
	}
	if err := ba.Set(k.Marshal(nil), toHash[:], nil); err != nil {
		return err
	}

	return ba.Commit(nil)
}

func collectLinkKeys(db *pebble.DB, lvid localvol.ID, target blobcache.OID, h blobcache.LinkID) ([][]byte, error) {
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
		if vl.To == target && vl.Hash == h {
			keys = append(keys, slices.Clone(iter.Key()))
		}
	}
	return keys, nil
}
