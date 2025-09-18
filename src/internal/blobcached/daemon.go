package blobcached

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var pki = inet256.PKI{
	Default: inet256.SignAlgo_Ed25519,
	Schemes: map[string]sign.Scheme{
		inet256.SignAlgo_Ed25519: inet256.SignScheme_Ed25519(),
	},
}

// Run runs the blobcache daemon, until the context is cancelled.
// If the context is cancelled, Run returns nil.  Run returns an error if it returns for any other reason.
func Run(ctx context.Context, stateDir string, pc net.PacketConn, serveAPI net.Listener) error {
	d := Daemon{StateDir: stateDir}
	db, err := pebble.Open(filepath.Join(stateDir, "pebble"), &pebble.Options{})
	if err != nil {
		return err
	}
	defer db.Close()
	blobDirPath := filepath.Join(stateDir, "blob")
	if err := os.MkdirAll(blobDirPath, 0o755); err != nil {
		return err
	}
	blobDir, err := os.OpenRoot(blobDirPath)
	if err != nil {
		return err
	}
	defer blobDir.Close()

	var privateKey ed25519.PrivateKey
	if pc != nil {
		privKey, err := d.EnsurePrivateKey()
		if err != nil {
			return err
		}
		privateKey = privKey.(ed25519.PrivateKey)
	}
	svc := bclocal.New(bclocal.Env{
		PacketConn: pc,
		DB:         db,
		BlobDir:    blobDir,
		PrivateKey: privateKey,
		Schemas:    bclocal.DefaultSchemas(),
		Root:       bclocal.DefaultRoot(),
	})

	eg, ctx := errgroup.WithContext(ctx)
	// if we have been given a listener for the API, serve it
	if serveAPI != nil {
		eg.Go(func() error {
			err := http.Serve(serveAPI, &bchttp.Server{
				Service: svc,
			})
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		})
		eg.Go(func() error {
			<-ctx.Done()
			return serveAPI.Close()
		})
	}
	// always run the local service in the background
	eg.Go(func() error {
		return svc.Run(ctx)
	})
	if err := eg.Wait(); errors.Is(err, context.Canceled) {
		return nil
	} else {
		return err
	}
}

type Daemon struct {
	StateDir string
}

// GetDB opens the database file, runs any migrations, and returns the database.
func (d *Daemon) GetDB() (*pebble.DB, error) {
	dbPath := filepath.Join(d.StateDir, "pebble")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// EnsurePrivateKey generates a private key if it doesn't exist, and returns it.
func (d *Daemon) EnsurePrivateKey() (inet256.PrivateKey, error) {
	p := filepath.Join(d.StateDir, "private_key.inet256")
	privKey, err := LoadPrivateKey(p)
	if !os.IsNotExist(err) {
		return privKey, nil
	}
	_, privKey, err = pki.GenerateKey()
	if err != nil {
		return nil, err
	}
	if err := SavePrivateKey(p, privKey); err != nil {
		return nil, err
	}
	return privKey, nil
}

func (d *Daemon) GetPolicy() (*Policy, error) {
	return LoadPolicy(d.StateDir)
}

func LoadPrivateKey(p string) (inet256.PrivateKey, error) {
	buf, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	return pki.ParsePrivateKey(buf)
}

func SavePrivateKey(p string, privKey inet256.PrivateKey) error {
	buf, err := pki.MarshalPrivateKey(nil, privKey)
	if err != nil {
		return err
	}
	return os.WriteFile(p, buf, 0600)
}

func AwaitHealthy(ctx context.Context, svc blobcache.Service) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if func() bool {
			ctx, cf := context.WithTimeout(ctx, 3*time.Second)
			defer cf()
			_, err := svc.Endpoint(ctx)
			if err == nil {
				logctx.Info(ctx, "service is healthy")
				return true
			}
			logctx.Info(ctx, "waiting for service to come up", zap.Error(err))
			return false
		}() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
