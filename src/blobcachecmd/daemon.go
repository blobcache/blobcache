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
	Flags: []star.IParam{stateDirParam, addrPortParam, serveAPIParam},
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
		Short: "run a daemon without persistent state",
	},
	Flags: []star.IParam{serveAPIParam},
	F: func(ctx star.Context) error {
		db := dbutil.OpenMemory()
		if err := bclocal.SetupDB(ctx, db); err != nil {
			return err
		}
		svc := bclocal.New(bclocal.Env{
			DB: db,
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

var addrPortParam = star.Param[netip.AddrPort]{
	Name: "addr",
	Parse: func(s string) (netip.AddrPort, error) {
		addr, err := netip.ParseAddr(s)
		if err != nil {
			return netip.AddrPort{}, err
		}
		return netip.AddrPortFrom(addr, 8080), nil
	},
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
