package blobcachecmd

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/schemareg"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var daemonCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon",
	},
	Flags: map[string]star.Flag{
		"state":     stateDirParam,
		"serve-api": serveAPIParam,
		"net":       netParam,
	},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		serveAPI, _ := serveAPIParam.LoadOpt(c)
		pc, _ := netParam.LoadOpt(c)
		d := blobcached.Daemon{StateDir: stateDir}
		return d.Run(c, pc, serveAPI)
	},
}

var daemonEphemeralCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon without persistent state",
	},
	Flags: map[string]star.Flag{
		"serve-api": serveAPIParam,
		"net":       netParam,
	},
	F: func(ctx star.Context) error {
		stateDir, err := os.MkdirTemp("", "blobcache-ephemeral")
		if err != nil {
			return err
		}
		defer os.RemoveAll(stateDir)
		pc, _ := netParam.LoadOpt(ctx)
		svc, err := bclocal.New(bclocal.Env{
			Background: ctx,
			StateDir:   stateDir,
			MkSchema:   schemareg.Factory,
			Root:       schemareg.DefaultRoot(),
			Policy:     &bclocal.AllOrNothingPolicy{},
		}, bclocal.Config{})
		if err != nil {
			return err
		}
		go func() {
			if err := svc.Serve(ctx, pc); err != nil {
				logctx.Error(ctx, "from serve:", zap.Error(err))
			}
		}()

		apiLis, _ := serveAPIParam.LoadOpt(ctx)
		defer apiLis.Close()
		logctx.Info(ctx, "serving API", zap.String("net", apiLis.Addr().Network()), zap.String("addr", apiLis.Addr().String()))
		return http.Serve(apiLis, &bchttp.Server{
			Service: svc,
		})
	},
}

var daemonValidateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Validate the config files and exit",
	},
	Flags: map[string]star.Flag{
		"state": stateDirParam,
	},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		c.Printf("checking configuration in %s\n", stateDir.Name())
		d := blobcached.Daemon{
			StateDir: stateDir,
		}
		if _, err := d.GetPolicy(); err != nil {
			return err
		}
		c.Printf(checkmark + " configuration is valid\n")
		return nil
	},
}

var showAccessCmd = star.Command{
	Metadata: star.Metadata{
		Short: "show access rights given to a peer on a given object",
	},
	Pos: []star.Positional{
		peerParam,
		oidParam,
	},
	Flags: map[string]star.Flag{
		"state": stateDirParam,
	},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		peerID := peerParam.Load(c)
		target := oidParam.Load(c)
		d := blobcached.Daemon{
			StateDir: stateDir,
		}
		pol, err := d.GetPolicy()
		if err != nil {
			return err
		}
		rights := pol.OpenFiat(peerID, target)
		c.Printf("PEER ID: %v\n", peerID)
		c.Printf("TARGET: %v\n", target)
		c.Printf("RIGHTS: %v\n", rights)
		return nil
	},
}

var stateDirParam = star.Required[*os.Root]{
	ID:    "state",
	Parse: os.OpenRoot,
}

var serveAPIParam = star.Optional[net.Listener]{
	ID: "serve-api",
	Parse: func(s string) (net.Listener, error) {
		parts := strings.Split(s, "://")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address: %s", s)
		}
		return net.Listen(parts[0], parts[1])
	},
}

var netParam = star.Optional[net.PacketConn]{
	ID: "net",
	Parse: func(s string) (net.PacketConn, error) {
		ap, err := netip.ParseAddrPort(s)
		if err != nil {
			return nil, err
		}
		udpAddr := net.UDPAddrFromAddrPort(ap)
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, err
		}
		return conn, nil
	},
}
