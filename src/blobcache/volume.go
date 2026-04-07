package blobcache

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/netip"
	"strings"

	"go.inet256.org/inet256/src/inet256"
)

type VolumeAPI interface {
	// CreateVolume creates a new volume.
	// CreateVolume always creates a Volume on the local Node.
	// CreateVolume returns a handle to the Volume.  If no other references to the Volume
	// have been created by the time the handle expires, the Volume will be deleted.
	// Leave caller nil to skip Authorization checks.
	// Host describes where the Volume should be created.
	// If the Host is nil, the Volume will be created on the local Node.
	CreateVolume(ctx context.Context, host *Endpoint, vspec VolumeSpec) (*Handle, error)
	// InspectVolume returns info about a Volume.
	InspectVolume(ctx context.Context, h Handle) (*VolumeInfo, error)
	// OpenFiat returns a handle to an object by it's ID.
	// This is where any Authorization checks are done.
	// It's called "fiat" because it's up to the Node to say yes or no.
	// The result is implementation dependent, unlike OpenFrom, which should behave
	// the same way on any Node.
	OpenFiat(ctx context.Context, x OID, mask ActionSet) (*Handle, error)
	// OpenFrom returns a handle to an object by it's ID.
	// base is the handle of a Volume, which links to the object.
	// the base Volume's schema must be a Container.
	OpenFrom(ctx context.Context, base Handle, ltok LinkToken, mask ActionSet) (*Handle, error)

	// BeginTx begins a new transaction, on a Volume.
	BeginTx(ctx context.Context, volh Handle, txp TxParams) (*Handle, error)
	// CloneVolume clones a Volume, copying it's configuration, blobs, and cell data.
	CloneVolume(ctx context.Context, caller *PeerID, volh Handle) (*Handle, error)
}

type TxAPI interface {
	// InspectTx returns info about a transaction.
	InspectTx(ctx context.Context, tx Handle) (*TxInfo, error)
	// Commit commits a transaction.
	Commit(ctx context.Context, tx Handle) error
	// Abort aborts a transaction.
	Abort(ctx context.Context, tx Handle) error
	// Load loads the volume root into dst
	Load(ctx context.Context, tx Handle, dst *[]byte) error
	// Save writes to the volume root.
	// Like all operations in a transaction, Save will not be visible until Commit is called.
	Save(ctx context.Context, tx Handle, src []byte) error
	// Post posts data to the volume
	Post(ctx context.Context, tx Handle, data []byte, opts PostOpts) (CID, error)
	// Get returns the data for a CID.
	Get(ctx context.Context, tx Handle, cid CID, buf []byte, opts GetOpts) (int, error)
	// Exists checks if several CID exists in the volume
	// len(dst) must be equal to len(cids), or Exists will return an error.
	Exists(ctx context.Context, tx Handle, cids []CID, dst []bool) error
	// Delete deletes a CID from the volume
	Delete(ctx context.Context, tx Handle, cids []CID) error
	// Copy has the same effect as Post, but it does not require sending the data to Blobcache.
	// It returns a slice of booleans, indicating if the CID could be added.
	// srcTxns are the transactions to copy from.  They will be checked in random order.
	// If none of them have the blob to copy, then false is written to success for that blob.
	// Error is only returned if there is an internal error, otherwise the success slice is used to signal
	// whether a CID was successfully copied.
	Copy(ctx context.Context, tx Handle, srcTxns []Handle, cids []CID, success []bool) error
	// Visit is only usable in a GC transaction.
	// It marks each CID as being visited, so it will not be removed by GC.
	Visit(ctx context.Context, tx Handle, cids []CID) error
	// IsVisited is only usable in a GC transaction.
	// It checks if each CID has been visited.
	IsVisited(ctx context.Context, tx Handle, cids []CID, yesVisited []bool) error

	// Link adds a link to another volume.
	// All Link operations take effect atomically on Commit
	Link(ctx context.Context, tx Handle, target Handle, mask ActionSet) (*LinkToken, error)
	// Unlink removes a link from the transaction's volume to any and all of the OIDs
	// All Unlink operations take effect atomically on Commit.
	Unlink(ctx context.Context, tx Handle, ltoks []LinkToken) error
	// VisitLink visits a link to another volume.
	// This is only usable in a GC transaction.
	// Any unvisited links will be deleted at the end of a GC transaction.
	VisitLinks(ctx context.Context, tx Handle, targets []LinkToken) error
}

// Endpoint is somewhere that a blobcache node can be found.
// The Zero endpoint means the node is not available on the network.
type Endpoint struct {
	Peer   PeerID         `json:"peer"`
	IPPort netip.AddrPort `json:"ip_port"`
}

const EndpointSize = PeerIDSize + 16 + 2

func (e Endpoint) Marshal(out []byte) []byte {
	out = append(out, e.Peer[:]...)

	ipaddrData, err := e.IPPort.AppendBinary(nil)
	if err != nil {
		panic(err)
	}
	for len(ipaddrData) < 16+2 { // 16 bytes for the IP address, 2 bytes for the port
		ipaddrData = append(ipaddrData, 0)
	}
	out = append(out, ipaddrData...)
	return out
}

func (e *Endpoint) Unmarshal(data []byte) error {
	if len(data) < PeerIDSize+16+2 {
		return fmt.Errorf("too small to be endpoint")
	}
	e.Peer, data = PeerID(data[:PeerIDSize]), data[PeerIDSize:]
	var ipaddr netip.AddrPort
	if err := ipaddr.UnmarshalBinary(data[:16+2]); err != nil {
		return err
	}
	e.IPPort = ipaddr
	return nil
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%s", e.Peer.String(), e.IPPort.String())
}

func ParseEndpoint(s string) (Endpoint, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return Endpoint{}, fmt.Errorf("invalid endpoint: %s", s)
	}
	peer, err := inet256.ParseAddrBase64([]byte(parts[0]))
	if err != nil {
		return Endpoint{}, err
	}
	ap, err := netip.ParseAddrPort(parts[1])
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{
		Peer:   peer,
		IPPort: ap,
	}, nil
}

// VolumeSpec is a specification for a volume.
type VolumeSpec = VolumeBackend[Handle]

// VolumeInfo is a volume info.
type VolumeInfo struct {
	// ID is always the local OID for the volume.
	ID OID `json:"id"`
	VolumeConfig
	Backend VolumeBackend[OID] `json:"backend"`
}

func (vi VolumeInfo) Marshal(out []byte) []byte {
	data, err := json.Marshal(vi)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (vi *VolumeInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, vi)
}

func (vi *VolumeInfo) GetRemoteFQOID() FQOID {
	if vi.Backend.Remote == nil {
		return FQOID{}
	}
	return FQOID{
		Peer: vi.Backend.Remote.Endpoint.Peer,
		OID:  vi.Backend.Remote.Volume,
	}
}

// VolumeBackend is a specification for a volume backend.
// If it is going into the API, the it will be a VolumeBackend[Handle].
// If it is coming out of the API, the it will be a VolumeBackend[OID].
type VolumeBackend[T volSpecRef] struct {
	Local     *VolumeBackend_Local     `json:"local,omitempty"`
	Remote    *VolumeBackend_Remote    `json:"remote,omitempty"`
	Peer      *VolumeBackend_Peer      `json:"peer,omitempty"`
	Git       *VolumeBackend_Git       `json:"git,omitempty"`
	Vault     *VolumeBackend_Vault[T]  `json:"vault,omitempty"`
	Consensus *VolumeBackend_Consensus `json:"consensus,omitempty"`
}

func (v *VolumeBackend[T]) Marshal(out []byte) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (v *VolumeBackend[T]) Unmarshal(data []byte) error {
	return json.Unmarshal(data, v)
}

// Deps returns the volumes which must exist before this volume can be created.
func (v *VolumeBackend[T]) Deps() iter.Seq[T] {
	switch {
	case v.Vault != nil:
		return unitIter[T](v.Vault.X)
	default:
		return emptyIter[T]()
	}
}

func (v VolumeBackend[T]) Config() VolumeConfig {
	switch {
	case v.Local != nil:
		v := v.Local
		return VolumeConfig{
			Schema:   v.Schema,
			HashAlgo: v.HashAlgo,
			MaxSize:  v.MaxSize,
			Salted:   v.Salted,
		}
	case v.Git != nil:
		return v.Git.VolumeConfig
	default:
		panic(v)
	}
}

func (v VolumeBackend[T]) String() string {
	sb := strings.Builder{}
	sb.WriteString("VolumeBackend{")
	if v.Local != nil {
		sb.WriteString("local")
	}
	if v.Remote != nil {
		sb.WriteString("remote:")
		sb.WriteString(v.Remote.Endpoint.Peer.String())
		sb.WriteString(" ")
		sb.WriteString(v.Remote.Volume.String())
	}
	if v.Git != nil {
		sb.WriteString("git")
	}
	if v.Vault != nil {
		sb.WriteString("vault:")
		sb.WriteString(fmt.Sprintf("%v", v.Vault.X))
	}
	sb.WriteString("}")
	return sb.String()
}

func (v *VolumeBackend[T]) Validate() (err error) {
	var count int
	if v.Local != nil {
		if err := v.Local.Validate(); err != nil {
			return err
		}
		count++
	}
	if v.Remote != nil {
		count++
	}
	if v.Peer != nil {
		count++
	}
	if v.Git != nil {
		count++
	}
	if v.Vault != nil {
		if err := v.Vault.Validate(); err != nil {
			return err
		}
		count++
	}

	switch count {
	case 0:
		err = errors.Join(err, fmt.Errorf("no volume backend specified"))
	case 1:
	default:
		err = errors.Join(err, fmt.Errorf("only one volume backend can be specified"))
	}
	return err
}

// VolumeBackendToOID converts a VolumeBackend[Handle] to a VolumeBackend[OID].
// It is used to convert the volume backend to the OID format when it is returned from the API.
func VolumeBackendToOID(x VolumeBackend[Handle]) (ret VolumeBackend[OID]) {
	ret = VolumeBackend[OID]{
		Local:  x.Local,
		Remote: x.Remote,
		Peer:   x.Peer,
		Git:    x.Git,
	}
	if x.Vault != nil {
		ret.Vault = &VolumeBackend_Vault[OID]{
			X:        x.Vault.X.OID,
			HashAlgo: x.Vault.HashAlgo,
			Secret:   x.Vault.Secret,
		}
	}
	return ret
}

// VolumeConfig contains parameters common to all Volumes.
// Not every volume backend allows them to be specified, but all Volumes have these Values set.
// e.g. the remote volume does not allow a max size to be specified, that's dictated by the remote node.
// However, the volume still has an effective max size, which is available if the volume has been mounted on the local Node.
type VolumeConfig struct {
	Schema   SchemaSpec `json:"schema"`
	HashAlgo HashAlgo   `json:"hash_algo"`
	MaxSize  int64      `json:"max_size"`
	Salted   bool       `json:"salted"`
}

func (v *VolumeConfig) Validate() error {
	if err := v.HashAlgo.Validate(); err != nil {
		return err
	}
	if v.MaxSize <= 0 {
		return fmt.Errorf("max size must be positive")
	}
	return nil
}

func DefaultVolumeParams() VolumeConfig {
	return VolumeConfig{
		Schema:   SchemaSpec{Name: Schema_NONE},
		HashAlgo: HashAlgo_BLAKE3_256,
		MaxSize:  1 << 22,
		Salted:   false,
	}
}

type VolumeBackend_Local struct {
	Schema   SchemaSpec `json:"schema"`
	HashAlgo HashAlgo   `json:"hash_algo"`
	MaxSize  int64      `json:"max_size"`
	Salted   bool       `json:"salted"`
}

func VolumeBackend_LocalFromConfig(x VolumeConfig) *VolumeBackend_Local {
	return &VolumeBackend_Local{
		Schema:   x.Schema,
		HashAlgo: x.HashAlgo,
		MaxSize:  x.MaxSize,
		Salted:   x.Salted,
	}
}

func (v *VolumeBackend_Local) Validate() error {
	vcfg := VolumeConfig{
		Schema:   v.Schema,
		HashAlgo: v.HashAlgo,
		MaxSize:  v.MaxSize,
		Salted:   v.Salted,
	}
	if err := vcfg.Validate(); err != nil {
		return err
	}
	return nil
}

type VolumeBackend_Remote struct {
	Endpoint Endpoint `json:"endpoint"`
	Volume   OID      `json:"volume"`
	HashAlgo HashAlgo `json:"hash_algo"`
}

type VolumeBackend_Peer struct {
	// Peer is the NodeID of the Node that controls the Volume.
	Peer PeerID `json:"peer"`
	// Volume is the OID of the Volume on the Node.
	Volume OID `json:"volume"`
	// HashAlgo is the HashAlgo used to hash the Volume.
	// It must match the HashAlgo in the VolumeInfo when inspecting the Volume.
	// It can also be empty to use whatever the remote Peer advertises.
	HashAlgo HashAlgo `json:"hash_algo"`
}

type VolumeBackend_Git struct {
	URL string `json:"url"`

	VolumeConfig
}

type VolumeBackend_Vault[T volSpecRef] struct {
	X        T        `json:"x"`
	Secret   Secret   `json:"secret"`
	HashAlgo HashAlgo `json:"hash_algo"`
}

func (v *VolumeBackend_Vault[T]) Validate() error {
	return v.HashAlgo.Validate()
}

type Secret [32]byte

func (s *Secret) UnmarshalText(data []byte) error {
	n, err := hex.Decode(data, s[:])
	if err != nil {
		return err
	}
	if n < len(s) {
		return fmt.Errorf("too short too contain 256 bit secret")
	}
	return nil
}

func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(s[:]))
}

func (s *Secret) UnmarshalJSON(data []byte) error {
	var hexString string
	if err := json.Unmarshal(data, &hexString); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}
	copy(s[:], decoded)
	return nil
}

type VolumeBackend_Consensus struct {
	Schema   SchemaSpec `json:"schema"`
	HashAlgo HashAlgo   `json:"hash_algo"`
	MaxSize  int64      `json:"max_size"`
}

type volSpecRef interface {
	Handle | OID
}

// DefaultLocalSpec provides sensible defaults for a local volume.
func DefaultLocalSpec() VolumeSpec {
	return VolumeSpec{
		Local: VolumeBackend_LocalFromConfig(DefaultVolumeParams()),
	}
}

func emptyIter[T any]() iter.Seq[T] {
	return func(yield func(T) bool) {}
}

func unitIter[T any](x T) iter.Seq[T] {
	return func(yield func(T) bool) {
		yield(x)
	}
}

// DEKSize is the size of a data encryption key
const DEKSize = 32

// DEK is a data encryption key.
type DEK [DEKSize]byte

func (d DEK) MarshalText() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(d[:]))
}

func (d *DEK) UnmarshalText(data []byte) error {
	var hexString string
	if err := json.Unmarshal(data, &hexString); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}
	copy(d[:], decoded)
	return nil
}

func (d DEK) String() string {
	d2, err := d.MarshalText()
	if err != nil {
		panic(err)
	}
	return string(d2)
}
