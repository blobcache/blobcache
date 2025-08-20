package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func defaultLocalSpec() blobcache.VolumeSpec {
	return blobcache.DefaultLocalSpec()
}

func CreateVolume(t testing.TB, s blobcache.Service, caller *blobcache.PeerID, spec blobcache.VolumeSpec) blobcache.Handle {
	ctx := testutil.Context(t)
	volh, err := s.CreateVolume(ctx, caller, spec)
	require.NoError(t, err)
	require.NotNil(t, volh)
	return *volh
}

func BeginTx(t testing.TB, s blobcache.Service, volh blobcache.Handle, params blobcache.TxParams) blobcache.Handle {
	ctx := testutil.Context(t)
	txh, err := s.BeginTx(ctx, volh, params)
	require.NoError(t, err)
	require.NotNil(t, txh)
	return *txh
}

func Post(t testing.TB, s blobcache.Service, txh blobcache.Handle, salt *blobcache.CID, data []byte) blobcache.CID {
	ctx := testutil.Context(t)
	cid, err := s.Post(ctx, txh, salt, data)
	require.NoError(t, err)
	return cid
}

func Get(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID, salt *blobcache.CID, maxLen int) []byte {
	ctx := testutil.Context(t)
	buf := make([]byte, maxLen)
	n, err := s.Get(ctx, txh, cid, salt, buf)
	require.NoError(t, err)
	return buf[:n]
}

func Exists(t testing.TB, s blobcache.Service, txh blobcache.Handle, cid blobcache.CID) bool {
	ctx := testutil.Context(t)
	yes, err := s.Exists(ctx, txh, cid)
	require.NoError(t, err)
	return yes
}

func Modify(t testing.TB, s blobcache.Service, volh blobcache.Handle, f func(tx *blobcache.Tx) ([]byte, error)) {
	ctx := testutil.Context(t)
	tx, err := blobcache.BeginTx(ctx, s, volh, blobcache.TxParams{Mutate: true})
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
