package blobcachecmd

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/dbutil"
	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var daemonCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon",
	},
	Flags: []star.AnyParam{stateDirParam, serveAPIParam, listenParam},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		serveAPI := serveAPIParam.Load(c)
		lis, _ := listenParam.LoadOpt(c)
		return blobcached.Run(c, stateDir, lis.X, serveAPI)
	},
}

var daemonEphemeralCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon without persistent state",
	},
	Flags: []star.AnyParam{serveAPIParam, listenParam},
	F: func(ctx star.Context) error {
		db := dbutil.OpenMemory()
		if err := bclocal.SetupDB(ctx, db); err != nil {
			return err
		}
		pc := listenParam.Load(ctx)
		svc := bclocal.New(bclocal.Env{
			DB:         db,
			PacketConn: pc.X,
			Schemas:    bclocal.DefaultSchemas(),
			Root:       bclocal.DefaultRoot(),
		})

		apiLis := serveAPIParam.Load(ctx)
		defer apiLis.Close()
		logctx.Info(ctx, "serving API", zap.String("net", apiLis.Addr().Network()), zap.String("addr", apiLis.Addr().String()))
		return http.Serve(apiLis, &bchttp.Server{
			Service: svc,
		})
	},
}

var stateDirParam = star.Param[string]{
	Name:  "state",
	Parse: star.ParseString,
}

var serveAPIParam = star.Param[net.Listener]{
	Name:    "serve-api",
	Default: star.Ptr(""),
	Parse: func(s string) (net.Listener, error) {
		parts := strings.Split(s, "://")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address: %s", s)
		}
		return net.Listen(parts[0], parts[1])
	},
}

var listenParam = star.Param[maybe.Maybe[net.PacketConn]]{
	Name:    "listen",
	Default: star.Ptr(""),
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
