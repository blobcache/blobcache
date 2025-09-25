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
	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var daemonCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon",
	},
	Flags: []star.Flag{stateDirParam, serveAPIParam, listenParam},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		serveAPI, _ := serveAPIParam.LoadOpt(c)
		lis, _ := listenParam.LoadOpt(c)
		d := blobcached.Daemon{StateDir: stateDir}
		return d.Run(c, lis.X, serveAPI)
	},
}

var daemonEphemeralCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon without persistent state",
	},
	Flags: []star.Flag{serveAPIParam, listenParam},
	F: func(ctx star.Context) error {
		stateDir, err := os.MkdirTemp("", "blobcache-ephemeral")
		if err != nil {
			return err
		}
		defer os.RemoveAll(stateDir)
		pc, _ := listenParam.LoadOpt(ctx)
		svc, err := bclocal.New(bclocal.Env{
			Background: ctx,
			StateDir:   stateDir,
			PacketConn: pc.X,
			Schemas:    bclocal.DefaultSchemas(),
			Root:       bclocal.DefaultRoot(),
		}, bclocal.Config{})
		if err != nil {
			return err
		}

		apiLis, _ := serveAPIParam.LoadOpt(ctx)
		defer apiLis.Close()
		logctx.Info(ctx, "serving API", zap.String("net", apiLis.Addr().Network()), zap.String("addr", apiLis.Addr().String()))
		return http.Serve(apiLis, &bchttp.Server{
			Service: svc,
		})
	},
}

var stateDirParam = star.Required[string]{
	Name:  "state",
	Parse: star.ParseString,
}

var serveAPIParam = star.Optional[net.Listener]{
	Name: "serve-api",
	Parse: func(s string) (net.Listener, error) {
		parts := strings.Split(s, "://")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address: %s", s)
		}
		return net.Listen(parts[0], parts[1])
	},
}

var listenParam = star.Optional[maybe.Maybe[net.PacketConn]]{
	Name: "listen",
	Parse: func(s string) (maybe.Maybe[net.PacketConn], error) {
		if s == "" {
			return maybe.Nothing[net.PacketConn](), nil
		}
		ap, err := netip.ParseAddrPort(s)
		if err != nil {
			return maybe.Nothing[net.PacketConn](), err
		}
		udpAddr := net.UDPAddrFromAddrPort(ap)
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return maybe.Nothing[net.PacketConn](), err
		}
		return maybe.Just[net.PacketConn](conn), nil
	},
}
