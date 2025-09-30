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
		pc, _ := listenParam.LoadOpt(c)
		d := blobcached.Daemon{StateDir: stateDir}
		return d.Run(c, pc, serveAPI)
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
			Schemas:    bclocal.DefaultSchemas(),
			Root:       bclocal.DefaultRoot(),
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

var listenParam = star.Optional[net.PacketConn]{
	Name: "listen",
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
