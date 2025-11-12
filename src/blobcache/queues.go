package blobcache

import (
	"context"
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

type QueueAPI interface {
	// CreateQueue creates a new queue and returns a handle to it.
	CreateQueue(ctx context.Context, host *Endpoint, qspec QueueSpec) (Handle, error)
	// Next reads from the queue.
	// It reads into buf until buf is full or another criteria is fulfilled.
	Next(ctx context.Context, q Handle, buf []Message, opts NextOpts) error
	// Enqueue adds the messages to the end of the queue.
	Enqueue(ctx context.Context, from *Endpoint, q Handle, msgs []Message) error

	// SubscribeToVolume causes all changes to a Volume's message to be
	// writen as message to the queue.
	SubscribeToVolume(ctx context.Context, q Handle, vol Handle) error

	// Publish sends data to all queues on the specified topic.
	Publish(ctx context.Context, from *Endpoint, tid TID, data []byte) error
	// Subscribe causes any messages published on topic to be delivered
	// to the queue.
	// Subscribe is idempotent.
	Subscribe(ctx context.Context, q Handle, tid TID) error
	// Unsubscribe removes the topic from the queues subscriptions.
	Unsubscribe(ctx context.Context, q Handle, tid TID) error
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

type QueueBackend[T handleOrOID] struct {
	Local  *QueueBackend_Local  `json:"local,omitempty"`
	Remote *QueueBackend_Remote `json:"remote,omitempty"`
}

type QueueBackend_Local struct {
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
