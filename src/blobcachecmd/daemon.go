package blobcachecmd

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"path/filepath"
	"strings"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/internal/dbutil"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var daemonCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon",
	},
	Flags: []star.IParam{stateDirParam, serveAPIParam, listenParam},
	F: func(ctx star.Context) error {
		stateDir := stateDirParam.Load(ctx)
		dbPath := filepath.Join(stateDir, "blobcache.db")
		db, err := dbutil.OpenDB(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()
		if err := bclocal.SetupDB(ctx, db); err != nil {
			return err
		}
		svc := bclocal.New(bclocal.Env{
			DB: db,
		})
		return svc.Run(ctx)
	},
}

var daemonEphemeralCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon without persistent state",
	},
	Flags: []star.IParam{serveAPIParam, listenParam},
	F: func(ctx star.Context) error {
		db := dbutil.OpenMemory()
		if err := bclocal.SetupDB(ctx, db); err != nil {
			return err
		}
		pc := listenParam.Load(ctx)
		svc := bclocal.New(bclocal.Env{
			DB:         db,
			PacketConn: pc,
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
	Name: "serve-api",
	Parse: func(s string) (net.Listener, error) {
		parts := strings.Split(s, "://")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address: %s", s)
		}
		return net.Listen(parts[0], parts[1])
	},
}

var listenParam = star.Param[net.PacketConn]{
	Name:    "listen",
	Default: star.Ptr(""),
	Parse: func(s string) (net.PacketConn, error) {
		if s == "" {
			return nil, nil
		}
		ap, err := netip.ParseAddrPort(s)
		if err != nil {
			return nil, err
		}
		udpAddr := net.UDPAddrFromAddrPort(ap)
		return net.ListenUDP("udp", udpAddr)
	},
}
