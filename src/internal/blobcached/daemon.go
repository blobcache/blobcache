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
	"blobcache.io/blobcache/src/internal/dbutil"
	"github.com/cloudflare/circl/sign/ed25519"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Run runs the blobcache daemon, until the context is cancelled.
// If the context is cancelled, Run returns nil.  Run returns an error if it returns for any other reason.
func Run(ctx context.Context, stateDir string, pc net.PacketConn, serveAPI net.Listener) error {
	dbPath := filepath.Join(stateDir, "blobcache.db")
	db, err := dbutil.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := bclocal.SetupDB(ctx, db); err != nil {
		return err
	}
	// if we have been given a PacketConn, then we also need a private key.
	var privateKey ed25519.PrivateKey
	if pc != nil {
		privateKeyPath := filepath.Join(stateDir, "private_key.pem")
		privKey, err := LoadPrivateKey(privateKeyPath)
		if err != nil {
			return err
		}
		privateKey = privKey.(ed25519.PrivateKey)
	}
	svc := bclocal.New(bclocal.Env{
		PacketConn: pc,
		DB:         db,
		PrivateKey: privateKey,
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

func LoadPrivateKey(p string) (inet256.PrivateKey, error) {
	buf, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	return inet256.ParsePrivateKey(buf)
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
