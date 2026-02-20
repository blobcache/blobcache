package bcsdk

import (
	"bytes"
	"context"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
)

// Watcher watches a Volume for changes, polling every second.
// Changes are delivered as read-only transactions on the Out channel.
type Watcher struct {
	svc  blobcache.Service
	volh blobcache.Handle

	out    chan *Tx
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWatcher creates a new Watcher that polls the volume for changes.
// It spawns a goroutine that polls every second and sends a read-only Tx
// on the Out channel whenever the volume's root changes.
func NewWatcher(svc blobcache.Service, volh blobcache.Handle) *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Watcher{
		svc:    svc,
		volh:   volh,
		out:    make(chan *Tx, 1),
		cancel: cancel,
	}
	w.wg.Add(1)
	go w.poll(ctx)
	return w
}

// Close stops the polling goroutine and closes the Out channel.
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

func (w *Watcher) poll(ctx context.Context) {
	defer w.wg.Done()
	defer close(w.out)

	var lastRoot []byte
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tx, err := BeginTx(ctx, w.svc, w.volh, blobcache.TxParams{
				Modify: false,
			})
			if err != nil {
				continue
			}
			var root []byte
			if err := tx.Load(ctx, &root); err != nil {
				tx.Abort(ctx)
				continue
			}
			if bytes.Equal(root, lastRoot) {
				tx.Abort(ctx)
				continue
			}
			lastRoot = append(lastRoot[:0], root...)
			select {
			case w.out <- tx:
			case <-ctx.Done():
				tx.Abort(ctx)
				return
			}
		}
	}
}
