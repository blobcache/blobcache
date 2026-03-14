package blobcachecmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"syscall"

	"blobcache.io/blobcache/src/bcipc"
	"blobcache.io/blobcache/src/internal/blobcached"
	"go.brendoncarroll.net/star"
)

var daemonCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon",
	},
	Flags: map[string]star.Flag{
		"state":      stateDirParam,
		"serve-http": serveHTTPParam,
		"serve-ipc":  serveIPCParam,
		"net":        netParam,
	},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		var lis []net.Listener
		if serveHttp, ok := serveHTTPParam.LoadOpt(c); ok {
			lis = append(lis, serveHttp)
		}
		var unixLis []*net.UnixListener
		if serveUnix, ok := serveIPCParam.LoadOpt(c); ok {
			unixLis = append(unixLis, serveUnix)
		}
		pc, _ := netParam.LoadOpt(c)
		d := blobcached.Daemon{StateDir: stateDir}
		return d.Run(c, pc, lis, unixLis)
	},
}

var daemonEphemeralCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs the blobcache daemon without persistent state",
	},
	Flags: map[string]star.Flag{
		"serve-http": serveHTTPParam,
		"serve-ipc":  serveIPCParam,
		"net":        netParam,
	},
	F: func(c star.Context) error {
		stateDirp, err := os.MkdirTemp("", "blobcache-ephemeral")
		if err != nil {
			return err
		}
		defer os.RemoveAll(stateDirp)
		stateDir, err := os.OpenRoot(stateDirp)
		if err != nil {
			return err
		}

		var lis []net.Listener
		if serveHttp, ok := serveHTTPParam.LoadOpt(c); ok {
			lis = append(lis, serveHttp)
		}
		var unixLis []*net.UnixListener
		if serveUnix, ok := serveIPCParam.LoadOpt(c); ok {
			unixLis = append(unixLis, serveUnix)
		}
		pc, _ := netParam.LoadOpt(c)
		d := blobcached.Daemon{StateDir: stateDir}

		return d.Run(c, pc, lis, unixLis)
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

var serveHTTPParam = star.Optional[net.Listener]{
	ID: "serve-http",
	Parse: func(s string) (net.Listener, error) {
		parts := strings.Split(s, "://")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address: %s", s)
		}
		return net.Listen(parts[0], parts[1])
	},
}

var serveIPCParam = star.Optional[*net.UnixListener]{
	ID:    "serve-ipc",
	Parse: bcipc.Listen,
}

var netParam = star.Optional[net.PacketConn]{
	ID: "net",
	Parse: func(s string) (net.PacketConn, error) {
		ap, err := netip.ParseAddrPort(s)
		if err != nil {
			return nil, err
		}
		udpAddr := net.UDPAddrFromAddrPort(ap)
		if runtime.GOOS == "darwin" {
			log.Println("darwin detected, adding silly SO_REUSE options")
			lc := net.ListenConfig{
				Control: func(network, address string, c syscall.RawConn) error {
					return c.Control(func(fd uintptr) {
						syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
						syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
					})
				},
			}
			ctx := context.TODO()
			return lc.ListenPacket(ctx, "udp4", udpAddr.String())
		}
		conn, err := net.ListenUDP("udp4", udpAddr)
		if err != nil {
			return nil, err
		}
		return conn, nil
	},
}
