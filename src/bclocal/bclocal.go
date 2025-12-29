// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcsys"
	"blobcache.io/blobcache/src/internal/svcgroup"
	"blobcache.io/blobcache/src/schema"
)

const (
	// MaxMaxBlobSize is the maximum value that a Volume's max size can be set to.
	MaxMaxBlobSize = 1 << 24
)

type (
	Policy = bcsys.Policy
)

var _ blobcache.Service = &Service{}

type Env struct {
	Background context.Context
	// StateDir is the directory where all the state is stored.
	StateDir string
	// PrivateKey determines the node's identity.
	// It must be provided if PacketConn is set.
	PrivateKey ed25519.PrivateKey
	// MkSchema is the factory function to create a schema.
	MkSchema schema.Factory
	// Root is the spec to use for the root volume.
	Root blobcache.VolumeSpec
	// Policy control network access to the service.
	Policy Policy
}

// Config contains configuration for the service.
// Config{} works fine.
// You don't need to worry about anything in here.
type Config struct {
	// When set to true, the service will not sync the database and blob directory.
	NoSync bool
}

// Service implements a blobcache.Service.
type Service struct {
	env Env
	cfg Config

	db      *pebble.DB
	blobDir *os.Root

	sys   *bcsys.Service[localvol.ID, *localvol.Volume]
	svcs  svcgroup.Group
	txSys pdb.TxSys
	lvs   localvol.System
}

func New(env Env, cfg Config) (*Service, error) {
	if env.Background == nil {
		return nil, fmt.Errorf("bclocal.New: Background cannot be nil")
	}
	if env.StateDir == "" {
		return nil, fmt.Errorf("bclocal.New: StateDir cannot be empty")
	}
	if env.PrivateKey == nil {
		return nil, fmt.Errorf("bclocal.New: PrivateKey cannot be nil")
	}
	if env.MkSchema == nil {
		env.MkSchema = func(spec blobcache.SchemaSpec) (schema.Schema, error) {
			if spec.Name != "" {
				return nil, fmt.Errorf("unknown schema name: %s", spec.Name)
			}
			return schema.None{}, nil
		}
	}

	dbPath := filepath.Join(env.StateDir, "pebble")
	blobDirPath := filepath.Join(env.StateDir, "blob")
	for _, dir := range []string{dbPath, blobDirPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := pebble.Open(dbPath, &pebble.Options{
		Logger: noOpLogger{},
	})
	if err != nil {
		return nil, err
	}
	blobDir, err := os.OpenRoot(blobDirPath)
	if err != nil {
		return nil, err
	}

	s := &Service{
		env: env,
		cfg: cfg,

		db:      db,
		blobDir: blobDir,
		svcs:    svcgroup.New(env.Background),
		txSys:   pdb.NewTxSys(db, dbtab.TID_SYS_TXNS),
	}

	s.lvs = localvol.New(localvol.Config{
		NoSync: s.cfg.NoSync,
	}, localvol.Env{
		DB:       db,
		BlobDir:  blobDir,
		TxSys:    &s.txSys,
		MkSchema: env.MkSchema,
	})
	rootVol := s.lvs.UpNoErr(localvol.Params{
		Key:    0,
		Params: s.env.Root.Config(),
	})
	loidSalt, err := ensureOIDSalt(db)
	if err != nil {
		panic(err)
	}

	s.sys = bcsys.New(bcsys.Env[localvol.ID, *localvol.Volume]{
		Background: env.Background,
		PrivateKey: env.PrivateKey,
		Root:       rootVol,
		MDS:        &mdStore{db: db},
		Policy:     env.Policy,
		MkSchema:   env.MkSchema,

		Local:      &s.lvs,
		GenerateLK: s.lvs.GenerateLocalID,
		OIDToLK: func(x blobcache.OID) (localvol.ID, error) {
			return localvol.LocalIDFromOID(loidSalt, x)
		},
		LKToOID: func(x localvol.ID) blobcache.OID {
			return localvol.OIDFromLocalID(loidSalt, x)
		},
	}, bcsys.Config{
		MaxMaxBlobSize: MaxMaxBlobSize,
	})
	s.svcs.Always(func(ctx context.Context) error {
		s.cleanupLoop(ctx)
		return nil
	})

	return s, nil
}

func (s *Service) Close() error {
	ctx := context.TODO()
	// stop background services.
	s.svcs.Shutdown()
	return errors.Join(
		// abort all transactions.
		s.AbortAll(ctx),
		// flush the blobs to disk.
		func() error {
			if !s.cfg.NoSync {
				return s.lvs.Flush()
			}
			return nil
		}(),
		// close the blob directory.
		s.blobDir.Close(),
		// close the database.
		s.db.Close(),
	)
}

// Serve handles requests from the network.
// Serve blocks untilt the context is cancelled, or Close is called.
// Cancelling the context will cause Run to return without an error.
// If Serve is *not* running, then remote volumes will not work, hosted on this Node or other Nodes.
func (s *Service) Serve(ctx context.Context, pc net.PacketConn) error {
	return s.sys.Serve(ctx, pc)
}

func (s *Service) Ping(ctx context.Context, ep blobcache.Endpoint) error {
	return s.sys.Ping(ctx, ep)
}

func (s *Service) LocalID() blobcache.PeerID {
	return s.sys.LocalID()
}

// AbortAll aborts all transactions.
func (s *Service) AbortAll(ctx context.Context) error {
	return s.sys.AbortAll(ctx)
}

// Cleanup runs the full cleanup process.
// This method is called periodically by Run, but it can also be called manually.
func (s *Service) Cleanup(ctx context.Context) error {
	// drop expired handles, and reclaim memory resources for transactions and volumes.
	_, err := s.sys.Cleanup(ctx)
	if err != nil {
		return err
	}
	// TODO: cleanup database.
	// We need to walk the local volumes table, and check if any of the volumes
	// are still active according to the system.
	return nil
}

func (s *Service) cleanupLoop(ctx context.Context) {
	tick := time.NewTicker(300 * time.Second)
	defer tick.Stop()
	for {
		if err := s.Cleanup(ctx); err != nil {
			logctx.Error(ctx, "during cleanup", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// Endpoint blocks waiting for a node to be created (happens when Serve is running).
// And then returns the Endpoint for that Node.
func (s *Service) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return s.sys.Endpoint(ctx)
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	return s.sys.Drop(ctx, h)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return s.sys.KeepAlive(ctx, hs)
}

func (s *Service) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return s.sys.Share(ctx, h, to, mask)
}

func (s *Service) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return s.sys.InspectHandle(ctx, h)
}

func (s *Service) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return s.sys.OpenFiat(ctx, x, mask)
}

func (s *Service) OpenFrom(ctx context.Context, base blobcache.Handle, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return s.sys.OpenFrom(ctx, base, x, mask)
}

func (s *Service) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	return s.sys.CreateVolume(ctx, host, vspec)
}

func (s *Service) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	return s.sys.CloneVolume(ctx, caller, volh)
}

func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return s.sys.InspectVolume(ctx, h)
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	return s.sys.BeginTx(ctx, volh, txspec)
}

func (s *Service) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	return s.sys.InspectTx(ctx, txh)
}

func (s *Service) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
	return s.sys.Save(ctx, txh, root)
}

func (s *Service) Commit(ctx context.Context, txh blobcache.Handle) error {
	return s.sys.Commit(ctx, txh)
}

func (s *Service) Abort(ctx context.Context, txh blobcache.Handle) error {
	return s.sys.Abort(ctx, txh)
}

func (s *Service) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	return s.sys.Load(ctx, txh, dst)
}

func (s *Service) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return s.sys.Post(ctx, txh, data, opts)
}

func (s *Service) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return s.sys.Exists(ctx, txh, cids, dst)
}

func (s *Service) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return s.sys.Get(ctx, txh, cid, buf, opts)
}

func (s *Service) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	return s.sys.Delete(ctx, txh, cids)
}

func (s *Service) Copy(ctx context.Context, txh blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, out []bool) error {
	return s.sys.Copy(ctx, txh, srcTxns, cids, out)
}

func (s *Service) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	return s.sys.Visit(ctx, txh, cids)
}

func (s *Service) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return s.sys.IsVisited(ctx, txh, cids, dst)
}

func (s *Service) Link(ctx context.Context, txh blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) error {
	return s.sys.Link(ctx, txh, target, mask)
}

func (s *Service) Unlink(ctx context.Context, txh blobcache.Handle, targets []blobcache.OID) error {
	return s.sys.Unlink(ctx, txh, targets)
}

func (s *Service) VisitLinks(ctx context.Context, txh blobcache.Handle, targets []blobcache.OID) error {
	return s.sys.VisitLinks(ctx, txh, targets)
}

func (s *Service) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	return s.sys.CreateQueue(ctx, host, qspec)
}

func (s *Service) Next(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.NextOpts) (int, error) {
	return s.sys.Next(ctx, qh, buf, opts)
}

func (s *Service) Insert(ctx context.Context, from *blobcache.Endpoint, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return s.sys.Insert(ctx, from, qh, msgs)
}

func (s *Service) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle) error {
	return s.sys.SubToVolume(ctx, qh, volh)
}

func (s *Service) cleanupVolumes(ctx context.Context, db *pebble.DB, keep func(blobcache.OID) bool) error {
	ba := db.NewIndexedBatch()
	defer ba.Close()
	iter, err := db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: pdb.TableLowerBound(dbtab.TID_VOLUMES),
		UpperBound: pdb.TableUpperBound(dbtab.TID_VOLUMES),
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
		// TODO: read links to the volume.
	}
	return ba.Commit(nil)
}

// AllOrNothingPolicy is a policy that allows or disallows all actions for all peers.
type AllOrNothingPolicy struct {
	Allow []blobcache.PeerID
}

func (p *AllOrNothingPolicy) OpenFiat(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	if !slices.Contains(p.Allow, peer) {
		return 0
	}
	return blobcache.Action_ALL
}

func (p *AllOrNothingPolicy) CanConnect(peer blobcache.PeerID) bool {
	return slices.Contains(p.Allow, peer)
}

func (p *AllOrNothingPolicy) CanCreate(peer blobcache.PeerID) bool {
	return slices.Contains(p.Allow, peer)
}
