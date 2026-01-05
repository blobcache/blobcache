package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func defaultLocalSpec() blobcache.VolumeSpec {
	return blobcache.DefaultLocalSpec()
}

func CreateVolume(t testing.TB, s blobcache.Service, host *blobcache.Endpoint, spec blobcache.VolumeSpec) blobcache.Handle {
	ctx := testutil.Context(t)
	volh, err := s.CreateVolume(ctx, host, spec)
	require.NoError(t, err)
	require.NotNil(t, volh)
	return *volh
}

// CreateOnSameHost creates a new subvolume on the same host as the base volume.
func CreateOnSameHost(t testing.TB, s blobcache.Service, base blobcache.Handle, spec blobcache.VolumeSpec) (blobcache.Handle, blobcache.FQOID) {
	ctx := testutil.Context(t)
	info, err := s.InspectVolume(ctx, base)
	require.NoError(t, err)
	var host *blobcache.Endpoint
	if info.Backend.Remote != nil {
		host = &info.Backend.Remote.Endpoint
	}
	svolh, err := s.CreateVolume(ctx, host, spec)
	require.NoError(t, err)
	if host != nil {
		svinfo, err := s.InspectVolume(ctx, *svolh)
		require.NoError(t, err)
		return *svolh, svinfo.GetRemoteFQOID()
	} else {
		ep, err := s.Endpoint(ctx)
		require.NoError(t, err)
		return *svolh, blobcache.FQOID{
			Peer: ep.Peer,
			OID:  svolh.OID,
		}
	}
}

func BeginTx(t testing.TB, s blobcache.Service, volh blobcache.Handle, params blobcache.TxParams) blobcache.Handle {
	ctx := testutil.Context(t)
	txh, err := s.BeginTx(ctx, volh, params)
	require.NoError(t, err)
	require.NotNil(t, txh)
	return *txh
}

func Post(t testing.TB, s blobcache.Service, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) blobcache.CID {
	ctx := testutil.Context(t)
	cid, err := s.Post(ctx, txh, data, opts)
	require.NoError(t, err)
	return cid
}

func Get(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) int {
	ctx := testutil.Context(t)
	n, err := s.Get(ctx, txh, cid, buf, opts)
	require.NoError(t, err)
	return n
}

func GetBytes(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID, opts blobcache.GetOpts, maxSize int) []byte {
	ctx := testutil.Context(t)
	buf := make([]byte, maxSize)
	n, err := s.Get(ctx, txh, cid, buf, opts)
	require.NoError(t, err)
	return buf[:n]
}

func Exists(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID) bool {
	ctx := testutil.Context(t)
	yes, err := bcsdk.ExistsSingle(ctx, s, txh, cid)
	require.NoError(t, err)
	return yes
}

func Delete(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID) {
	t.Helper()
	ctx := testutil.Context(t)
	require.NoError(t, s.Delete(ctx, txh, []blobcache.CID{cid}))
}

func Link(t testing.TB, s blobcache.Service, txh blobcache.Handle, volh blobcache.Handle, mask blobcache.ActionSet) blobcache.LinkToken {
	ctx := testutil.Context(t)
	lt, err := s.Link(ctx, txh, volh, mask)
	require.NoError(t, err)
	return *lt
}

func Unlink(t testing.TB, s blobcache.Service, txh blobcache.Handle, ltok blobcache.LinkToken) {
	ctx := testutil.Context(t)
	require.NoError(t, s.Unlink(ctx, txh, []blobcache.LinkToken{ltok}))
}

func Modify(t testing.TB, s blobcache.Service, volh blobcache.Handle, f func(tx *bcsdk.Tx) ([]byte, error)) {
	ctx := testutil.Context(t)
	tx, err := bcsdk.BeginTx(ctx, s, volh, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	data, err := f(tx)
	require.NoError(t, err)
	require.NoError(t, tx.Save(ctx, data))
	require.NoError(t, tx.Commit(ctx))
}

func Load(t testing.TB, s blobcache.Service, txh blobcache.Handle) []byte {
	ctx := testutil.Context(t)
	buf := []byte{}
	require.NoError(t, s.Load(ctx, txh, &buf))
	if buf == nil {
		buf = []byte{}
	}
	return buf
}

func Save(t testing.TB, s blobcache.Service, txh blobcache.Handle, data []byte) {
	ctx := testutil.Context(t)
	require.NoError(t, s.Save(ctx, txh, data))
}

func Commit(t testing.TB, s blobcache.Service, txh blobcache.Handle) {
	ctx := testutil.Context(t)
	require.NoError(t, s.Commit(ctx, txh))
}

// SaveCommit is a convenience function that saves the given data and then commits the transaction.
func SaveCommit(t testing.TB, s blobcache.Service, txh blobcache.Handle, data []byte) {
	ctx := testutil.Context(t)
	require.NoError(t, s.Save(ctx, txh, data))
	require.NoError(t, s.Commit(ctx, txh))
}

func Abort(t testing.TB, s blobcache.Service, txh blobcache.Handle) {
	ctx := testutil.Context(t)
	require.NoError(t, s.Abort(ctx, txh))
}

func Endpoint(t testing.TB, s blobcache.Service) blobcache.Endpoint {
	ctx := testutil.Context(t)
	ep, err := s.Endpoint(ctx)
	require.NoError(t, err)
	return ep
}

func IsVisited(t testing.TB, s blobcache.Service, txh blobcache.Handle, cids []blobcache.CID) []bool {
	ctx := testutil.Context(t)
	visited := make([]bool, len(cids))
	require.NoError(t, s.IsVisited(ctx, txh, cids, visited))
	return visited
}

func Visit(t testing.TB, s blobcache.Service, txh blobcache.Handle, cids []blobcache.CID) {
	ctx := testutil.Context(t)
	require.NoError(t, s.Visit(ctx, txh, cids))
}
