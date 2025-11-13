package blobcache

import (
	"context"
	"crypto/sha3"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type Message struct {
	// Endpoint is the endpoint where the message came from or is going.
	Endpoint Endpoint `json:"endpoint"`
	// Topic is the topic that the message is speaking on.
	Topic TID `json:"topic"`
	// Payload data to deliver.
	Payload []byte `json:"payload"`
}

func (m Message) Marshal(out []byte) []byte {
	out = m.Endpoint.Marshal(out)
	out = append(out, m.Topic[:]...)
	out = append(out, m.Payload...)
	return out
}

func (m *Message) Unmarshal(data []byte) error {
	if err := m.Endpoint.Unmarshal(data[:EndpointSize]); err != nil {
		return err
	}
	m.Topic = TID(data[OIDSize : OIDSize+32])
	m.Payload = data[OIDSize+32:]
	return nil
}

type QueueAPI interface {
	// CreateQueue creates a new queue and returns a handle to it.
	CreateQueue(ctx context.Context, host *Endpoint, qspec QueueSpec) (*Handle, error)
	// Next reads from the queue.
	// It reads into buf until buf is full or another criteria is fulfilled.
	Next(ctx context.Context, q Handle, buf []Message, opts NextOpts) (int, error)
	// Insert adds the messages to the end of the queue.
	Insert(ctx context.Context, from *Endpoint, q Handle, msgs []Message) (*InsertResp, error)

	// SubToVolume causes all changes to a Volume's cell to be
	// writen as message to the queue.
	SubToVolume(ctx context.Context, q Handle, vol Handle) error
}

type InsertResp struct {
	// Success is the number of messages that were inserted.
	Success uint32
}

type NextOpts struct {
	// Min is the minimum number of messages to read before returning.
	// set to 0 for non blocking.
	// If there are messages available at the start of the call,
	// they must be emitted even if Min is 0
	Min uint32 `json:"min"`
	// LeaveIn will cause the emitted messages to stay in the queue.
	LeaveIn bool `json:"leave_in"`
	// Skip drops this many messages before emitting any
	Skip uint32 `json:"skip"`
	// MaxWait is the maximum amount of time to wait.
	MaxWait *time.Duration `json:"max_wait"`
}

func (no NextOpts) Marshal(out []byte) []byte {
	data, err := json.Marshal(no)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (no *NextOpts) Unmarshal(data []byte) error {
	return json.Unmarshal(data, no)
}

// QueueInfo is info about a queue.
type QueueInfo struct {
	ID   OID               `json:"id"`
	Spec QueueBackend[OID] `json:"spec"`
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

type QueueBackend[T handleOrOID] struct {
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
	EvictOldest bool
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
