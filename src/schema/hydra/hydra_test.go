package hydra

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/internal/testutil"
)

func TestTx(t *testing.T) {
	ctx := testutil.Context(t)
	h, svc := setup(t)
	txh := blobcachetests.BeginTx(t, svc, h, blobcache.TxParams{Modify: true})
	defer svc.Abort(ctx, txh)
	cid1 := blobcachetests.Post(t, svc, txh, []byte("hello world1"), blobcache.PostOpts{})
	cid2 := blobcachetests.Post(t, svc, txh, []byte("hello world2"), blobcache.PostOpts{})
	r := Root{cid1, cid2}
	blobcachetests.Save(t, svc, txh, r.Marshal(nil))
	blobcachetests.Commit(t, svc, txh)
}

func setup(t testing.TB) (blobcache.Handle, blobcache.Service) {
	env := bclocal.NewTestEnv(t)
	env.MkSchema = schemareg.Factory
	svc := bclocal.NewTestServiceFromEnv(t, env)
	spec := blobcache.DefaultLocalSpec()
	spec.Local.Schema = blobcache.SchemaSpec{Name: SchemaName}
	h := blobcachetests.CreateVolume(t, svc, nil, spec)
	return h, svc
}
