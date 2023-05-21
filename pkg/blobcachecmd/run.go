package blobcachecmd

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/inet256/inet256/pkg/serde"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	DefaultAPIAddr = "127.0.0.1:6025"
)

var (
	privateKeyPath string
	dataDir        string
	peerSpecs      []string
	apiAddr        string
)

func init() {
	runCmd.Flags().StringVar(&dataDir, "data-dir", "", "--data-dir=path/to/data-dir")
	runCmd.Flags().StringVar(&privateKeyPath, "private-key", "", "--private-key=path/to/private-key.pem")
	runCmd.Flags().StringVar(&apiAddr, "api-addr", DefaultAPIAddr, "--api-addr=192.168.1.100:8080")
	runCmd.Flags().StringArrayVar(&peerSpecs, "peer", nil, "--peer=<peer-id>,<quota>")
}

var runCmd = &cobra.Command{
	Short: "runs the blobcache server",
	Use:   "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := cmd.ParseFlags(args); err != nil {
			return err
		}
		// private key
		if privateKeyPath == "" {
			return errors.Errorf("flag private-key is required")
		}
		privateKey, err := loadPrivateKey(privateKeyPath)
		if err != nil {
			return err
		}
		// data-dir
		if dataDir == "" {
			return errors.Errorf("flag data-dir is required")
		}
		db, store, err := setupDataDir(dataDir)
		if err != nil {
			return err
		}
		peerInfos, err := parsePeerInfos(peerSpecs)
		if err != nil {
			return err
		}
		log := logrus.StandardLogger()
		nodeParams := blobcache.Params{
			Peers:      peerInfos,
			PrivateKey: privateKey,
			DB:         db,
			Primary:    store,
			Logger:     log,
		}
		d := NewDaemon(DaemonParams{
			NodeParams: nodeParams,
			APIAddr:    apiAddr,
			Logger:     log,
		})
		return d.Run(context.Background())
	},
}

func parsePeerInfos(xs []string) (ret []blobcache.PeerInfo, _ error) {
	for _, x := range xs {
		parts := strings.SplitN(x, ",", 2)
		if len(parts) < 2 {
			return nil, errors.Errorf("must specify quota in peer spec: %q", x)
		}
		id, err := inet256.ParseAddrBase64([]byte(parts[0]))
		if err != nil {
			return nil, err
		}
		quotaCount, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		info := blobcache.PeerInfo{
			ID:         id,
			QuotaCount: quotaCount,
		}
		ret = append(ret, info)
	}
	return ret, nil
}

func loadPrivateKey(p string) (inet256.PrivateKey, error) {
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, err
	}
	return serde.ParsePrivateKeyPEM(data)
}

func setupDataDir(dataDir string) (bcdb.DB, cadata.Store, error) {
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, nil, err
	}
	dbPath := filepath.Join(dataDir, "db")
	db, err := bcdb.NewBadger(dbPath)
	if err != nil {
		return nil, nil, err
	}
	storePath := filepath.Join(dataDir, "blobs")
	if err := os.MkdirAll(storePath, 0o755); err != nil {
		return nil, nil, err
	}
	storeFS := posixfs.NewDirFS(storePath)
	store := stores.NewFSStore(storeFS)
	return db, store, nil
}
