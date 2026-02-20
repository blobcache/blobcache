package bcsdk

import (
	"context"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
)

// Watcher watches a Volume for changes using a queue subscription.
// Changes are delivered as read-only transactions on the Out channel.
type Watcher struct {
	svc  blobcache.Service
	volh blobcache.Handle
	qh   blobcache.Handle

	out    chan *Tx
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWatcher creates a new Watcher that subscribes to volume changes.
// It creates a queue, subscribes it to the volume, and spawns a goroutine
// that sends a read-only Tx on the Out channel whenever the volume changes.
func NewWatcher(svc blobcache.Service, volh blobcache.Handle) (*Watcher, error) {
	ctx := context.Background()
	qh, err := svc.CreateQueue(ctx, nil, blobcache.QueueSpec{
		Memory: &blobcache.QueueBackend_Memory{
			MaxDepth:             1,
			EvictOldest:          true,
			MaxBytesPerMessage:   0,
			MaxHandlesPerMessage: 0,
		},
	})
	if err != nil {
		return nil, err
	}
	if err := svc.SubToVolume(ctx, *qh, volh, blobcache.VolSubSpec{}); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &Watcher{
		svc:    svc,
		volh:   volh,
		qh:     *qh,
		out:    make(chan *Tx, 1),
		cancel: cancel,
	}
	w.wg.Add(1)
	go w.loop(ctx)
	return w, nil
}

// Close stops the goroutine and closes the Out channel.
// It causes ForEach to return nil.
func (w *Watcher) Close() error {
	w.cancel()
	w.wg.Wait()
	return nil
}

// ForEach calls fn with a read-only transaction every time the volume changes.
// The Tx is closed after fn returns.
// If ForEach returns it will be because the context was cancelled or Close was called.
// The closed case returns nil, context errors are returned.
func (w *Watcher) ForEach(ctx context.Context, fn func(tx *Tx) error) error {
	for {
		select {
		case tx, ok := <-w.out:
			if !ok {
				return nil
			}
			err := fn(tx)
			tx.Abort(ctx)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Out returns a receive-only channel that emits a read-only Tx
// whenever the volume changes. The caller is responsible for
// aborting the Tx when done with it.
func (w *Watcher) Out() <-chan *Tx {
	return w.out
}

func (w *Watcher) loop(ctx context.Context) {
	defer w.wg.Done()
	defer close(w.out)

	buf := make([]blobcache.Message, 1)
	for {
		n, err := w.svc.Dequeue(ctx, w.qh, buf, blobcache.DequeueOpts{Min: 1})
		if err != nil {
			return
		}
		if n == 0 {
			continue
		}
		tx, err := BeginTx(ctx, w.svc, w.volh, blobcache.TxParams{
			Modify: false,
		})
		if err != nil {
			continue
		}
		select {
		case w.out <- tx:
		case <-ctx.Done():
			tx.Abort(ctx)
			return
		}
	}
}
