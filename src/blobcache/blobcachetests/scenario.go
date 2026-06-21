package blobcachetests

import (
	"context"
	"testing"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/slices2"
)

// Volume describes how a Volume should be setup in a Scene
type Volume interface {
	isVolume()
}

// LocalVolume describes a Volume in a Scenario
type LocalVolume struct {
	Schema   blobcache.SchemaSpec
	Contents Contents
}

func (v *LocalVolume) isVolume() {}

func (v *LocalVolume) VolumeSpec() blobcache.VolumeSpec {
	spec := blobcache.DefaultLocalSpec()
	spec.Local.Schema = v.Schema
	return spec
}

type PeerVolume struct {
	Node   int
	Volume int
}

func (v *PeerVolume) isVolume() {}

// Contents are Volume contents
type Contents interface {
	// Fill fills a Volume
	Fill(ctx context.Context, tx *bcsdk.Tx, nodes []blobcache.NodeID, vols [][]blobcache.OID) error
}

var _ Contents = &Data{}

type Data struct {
	Cell []byte
}

func (d *Data) Fill(ctx context.Context, tx *bcsdk.Tx, nodes []blobcache.NodeID, vols [][]blobcache.OID) error {
	if d == nil {
		return nil
	}
	return tx.Save(ctx, d.Cell)
}

// Scene describes an initial configuration of Nodes and Volumes
type Scene struct {
	Nodes [][]Volume
}

func SetupScene(t testing.TB, scen Scene, nodes []blobcache.Service) {
	t.Helper()
	if len(nodes) != len(scen.Nodes) {
		t.Fatalf("cannot setup scenario, wrong number of nodes: got=%d want=%d", len(nodes), len(scen.Nodes))
	}

	nodeIDs := slices2.Map(nodes, func(x blobcache.Service) blobcache.NodeID {
		return Endpoint(t, x).Node
	})
	handles := make([][]blobcache.Handle, len(scen.Nodes))
	volOIDs := make([][]blobcache.OID, len(scen.Nodes))
	for nodeIdx := range scen.Nodes {
		handles[nodeIdx] = make([]blobcache.Handle, len(scen.Nodes[nodeIdx]))
		volOIDs[nodeIdx] = make([]blobcache.OID, len(scen.Nodes[nodeIdx]))
	}
	// Create local volumes first so peer volumes always have concrete targets.
	for nodeIdx, vols := range scen.Nodes {
		node := nodes[nodeIdx]
		for volIdx, vol := range vols {
			local, ok := vol.(*LocalVolume)
			if !ok {
				continue
			}
			spec := blobcache.DefaultLocalSpec()
			spec.Local.Schema = local.Schema
			handles[nodeIdx][volIdx] = CreateVolume(t, node, nil, spec)
			volOIDs[nodeIdx][volIdx] = handles[nodeIdx][volIdx].OID
		}
	}
	// Create peer volumes.
	for nodeIdx, vols := range scen.Nodes {
		node := nodes[nodeIdx]
		for volIdx, vol := range vols {
			pv, ok := vol.(*PeerVolume)
			if !ok {
				continue
			}
			spec := blobcache.VolumeSpec{Peer: &blobcache.VolumeBackend_Peer{
				Peer:   nodeIDs[pv.Node],
				Volume: handles[pv.Node][pv.Volume].OID,
			}}
			handles[nodeIdx][volIdx] = CreateVolume(t, node, nil, spec)
			volOIDs[nodeIdx][volIdx] = handles[nodeIdx][volIdx].OID
		}
	}

	// Add contents to Volumes
	ctx := testutil.Context(t)
	for nodeIdx, vols := range scen.Nodes {
		node := nodes[nodeIdx]
		for volIdx, vol := range vols {
			local, ok := vol.(*LocalVolume)
			if !ok {
				continue
			}
			if local.Contents == nil {
				continue
			}
			target := handles[nodeIdx][volIdx]
			vcfg := local.VolumeSpec().Config()
			func() {
				txh := BeginTx(t, node, target, blobcache.TxParams{Modify: true})
				tx := bcsdk.NewTx(node, txh, vcfg.HashAlgo, int(vcfg.MaxSize))
				defer Abort(t, node, txh)
				err := local.Contents.Fill(ctx, tx, nodeIDs, volOIDs)
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))
			}()
		}
	}
}
