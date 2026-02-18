package blobcache

import (
	"context"
	"crypto/sha3"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"go.brendoncarroll.net/exp/sbe"
)

type Message struct {
	// Handles are handles associated with the message
	Handles []Handle `json:"handles"`
	// Bytes is arbitrary bytes delivered with the message.
	Bytes []byte `json:"bytes"`
}

func (m Message) Marshal(out []byte) []byte {
	out = sbe.AppendUint32(out, uint32(len(m.Handles)))
	for _, h := range m.Handles {
		out = h.Marshal(out)
	}
	out = append(out, m.Bytes...)
	return out
}

func (m *Message) Unmarshal(data []byte) error {
	numHandles, data, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}
	m.Handles = m.Handles[:0]
	for range numHandles {
		hdata, rest, err := sbe.ReadN(data, HandleSize)
		if err != nil {
			return err
		}
		var h Handle
		if err := h.Unmarshal(hdata); err != nil {
			return err
		}
		m.Handles = append(m.Handles, h)
		data = rest
	}
	m.Bytes = append(m.Bytes[:0], data...)
	return nil
}

// VolSubSpec specifies a Volume Subscription
type VolSubSpec struct {
	// OpenTx, if not-nil, causes a transaction with the requested parameters to be opened
	// every time the volume changes.
	OpenTx *TxParams
	// PremptCell causes the current contents of the Volume's cell to be
	// added to the Bytes portion of the message.  It will be length-prefixed with a 32bit integer.
	// This is best-effort and no cell data will be included if it is too large.
	PremptCell bool
	// PremptBlobs causes blobs, which the implementation thinks will be relevant,
	// to be appended, 32-bit-length-prefixed, to the Bytes portion of the message.
	// They will be after the cell.
	// If the cell is too large then no Blobs will be appended.
	PremptBlobs bool
}

func (vs VolSubSpec) Marshal(out []byte) []byte {
	data, err := json.Marshal(vs)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (vs *VolSubSpec) Unmarshal(data []byte) error {
	return json.Unmarshal(data, vs)
}

type QueueAPI interface {
	// CreateQueue creates a new queue and returns a handle to it.
	CreateQueue(ctx context.Context, host *Endpoint, qspec QueueSpec) (*Handle, error)
	// InspectQueue returns information about a queue.
	InspectQueue(ctx context.Context, qh Handle) (QueueInfo, error)
	// Dequeue reads from the queue.
	// It reads into buf until buf is full or another criteria is fulfilled.
	Dequeue(ctx context.Context, q Handle, buf []Message, opts DequeueOpts) (int, error)
	// Insert adds the messages to the end of the queue.
	Enqueue(ctx context.Context, q Handle, msgs []Message) (*InsertResp, error)

	// SubToVolume causes all changes to a Volume's cell to be
	// writen as message to the queue.
	SubToVolume(ctx context.Context, q Handle, vol Handle, spec VolSubSpec) error
}

type InsertResp struct {
	// Success is the number of messages that were inserted.
	Success uint32
}

type DequeueOpts struct {
	// Min is the minimum number of messages to read before returning.
	// set to 0 for non blocking.
	// If there are messages available at the start of the call,
	// they must be emitted even if Min is 0
	Min uint32 `json:"min"`
	// LeaveIn will cause the emitted messages to stay in the queue.
	LeaveIn bool `json:"leave_in"`
	// Skip drops this many messages before emitting any.
	Skip uint32 `json:"skip"`
	// MaxWait is the maximum amount of time to wait.
	// The call is guaranteed to return in this amount of time with something, or an error.
	MaxWait *time.Duration `json:"max_wait"`
}

func (no DequeueOpts) Marshal(out []byte) []byte {
	data, err := json.Marshal(no)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (no *DequeueOpts) Unmarshal(data []byte) error {
	return json.Unmarshal(data, no)
}

func (no DequeueOpts) Validate() error {
	return nil
}

// QueueInfo is info about a queue.
type QueueInfo struct {
	ID      OID               `json:"id"`
	Config  QueueConfig       `json:"config"`
	Backend QueueBackend[OID] `json:"backend"`
}

func (qi QueueInfo) Marshal(out []byte) []byte {
	data, err := json.Marshal(qi)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (qi *QueueInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, qi)
}

// QueueConfig contains parameters which all queues must have.
type QueueConfig struct {
	MaxDepth             uint32 `json:"max_depth"`
	MaxBytesPerMessage   uint32 `json:"max_bytes_per_message"`
	MaxHandlesPerMessage uint32 `json:"max_handles_per_message"`
}

type QueueBackend[T volSpecRef] struct {
	Memory *QueueBackend_Memory `json:"memory,omitempty"`
	Remote *QueueBackend_Remote `json:"remote,omitempty"`
}

func (qb QueueBackend[T]) Marshal(out []byte) []byte {
	data, err := json.Marshal(qb)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (qb *QueueBackend[T]) Unmarshal(data []byte) error {
	return json.Unmarshal(data, qb)
}

type QueueBackend_Memory struct {
	// MaxDepth
	MaxDepth uint32 `json:"max_depth"`
	// EvictOldest will cause the oldest message in the queue to be evicted
	// when the mailbox is full.
	// The default value (false) causes new messages to be dropped when the queue is full.
	EvictOldest bool `json:"evict_oldest"`

	MaxBytesPerMessage   uint32 `json:"max_bytes_per_message"`
	MaxHandlesPerMessage uint32 `json:"max_handles_per_message"`
}

type QueueBackend_Remote struct {
	Endpoint Endpoint
	OID      OID
}

type QueueSpec = QueueBackend[Handle]

// TID is a Topic ID
// It uniquely identifies a Topic
type TID [32]byte

func (tid TID) IsZero() bool {
	return tid == (TID{})
}

func (tid TID) String() string {
	return hex.EncodeToString(tid[:])
}

func (tid *TID) Unmarshal(data []byte) error {
	if len(data) < 32 {
		return fmt.Errorf("too small to be topic id")
	}
	copy(tid[:], data)
	return nil
}

func (tid TID) Key() [32]byte {
	return sha3.Sum256(tid[:])
}
