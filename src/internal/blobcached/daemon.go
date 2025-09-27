package blobcached

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/stretchr/testify/require"
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
func (d *Daemon) Run(ctx context.Context, pc net.PacketConn, serveAPI net.Listener) error {
	if err := d.EnsurePolicyFiles(); err != nil {
		return err
	}
	pol, err := d.GetPolicy()
	if err != nil {
		return err
	}
	var privateKey ed25519.PrivateKey
	if pc != nil {
		privKey, err := d.EnsurePrivateKey()
		if err != nil {
			return err
		}
		privateKey = privKey.(ed25519.PrivateKey)
	}
	svc, err := bclocal.New(bclocal.Env{
		Background: ctx,
		PacketConn: pc,
		StateDir:   d.StateDir,
		PrivateKey: privateKey,
		Policy:     pol,
		Schemas:    bclocal.DefaultSchemas(),
		Root:       bclocal.DefaultRoot(),
	}, bclocal.Config{})
	if err != nil {
		return err
	}

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
	if pc != nil {
		// if a PacketConn is provided, then run the Serve loop.
		eg.Go(func() error {
			return svc.Serve(ctx)
		})
	}

	if err := eg.Wait(); errors.Is(err, context.Canceled) {
		err = nil
	} else if err != nil {
		return err
	}
	if err := svc.Close(); err != nil {
		return err
	}
	return nil
}

// Daemon manages the state and configuration for running a Blobache node.
type Daemon struct {
	StateDir string
}

// EnsurePolicyFiles ensures that the policy files exist.
// Creating default files if they don't exist.
func (d *Daemon) EnsurePolicyFiles() error {
	if d.StateDir == "" {
		return fmt.Errorf("StateDir is required")
	}
	files := map[string]string{
		filepath.Join(d.StateDir, IdentitiesFilename): DefaultIdentitiesFile(),
		filepath.Join(d.StateDir, ActionsFilename):    DefaultActionsFile(),
		filepath.Join(d.StateDir, ObjectsFilename):    DefaultObjectsFile(),
		filepath.Join(d.StateDir, GrantsFilename):     DefaultGrantsFile(),
	}
	for p, content := range files {
		if err := func() error {
			f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
			if err != nil {
				if os.IsExist(err) {
					return nil
				}
				return err
			}
			defer f.Close()
			if _, err := f.Write([]byte(content)); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
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

// RunTestDaemon launches a test daemon and returns it and the API address.
// This function will block until the daemon is healthy.
// The daemon will be stopped and cleaned up at the end of the test.
// The test will fail during cleanup if the daemon fails to stop, and
// The test will not complete until the daemon is successfully torn down.
func RunTestDaemon(t testing.TB) (*Daemon, string) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)
	dir := t.TempDir()
	d := Daemon{StateDir: dir}
	pc := testutil.PacketConn(t)
	lis := testutil.Listen(t)
	go func() {
		if err := d.Run(ctx, pc, lis); err != nil {
			t.Log(err)
		}
	}()
	apiURL := lis.Addr().Network() + "://" + lis.Addr().String()
	t.Cleanup(func() {
		if err := pc.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("packet conn close: %v", err)
		}
		if err := lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener close: %v", err)
		}
	})
	svc := bcclient.NewClient(apiURL)
	t.Log("awaiting healthy", apiURL)
	require.NoError(t, AwaitHealthy(ctx, svc))
	t.Log("service is up")
	return &d, apiURL
}
