package control

import (
	"context"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/sync/errgroup"
)

// Controller manages where to store blobs
type Controller struct {
	mu      sync.RWMutex
	sources map[string]Source
	sinks   map[string]Sink

	sourceStates map[string]*sourceState
	sinkStates   map[string]*sinkState
}

func New() *Controller {
	return &Controller{
		sources:      make(map[string]Source),
		sinks:        make(map[string]Sink),
		sourceStates: make(map[string]*sourceState),
		sinkStates:   make(map[string]*sinkState),
	}
}

func (c *Controller) AttachSink(name string, sink Sink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sinks[name] = sink
	c.sinkStates[name] = newSinkState()
}

func (c *Controller) AttachSource(name string, source Source) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sources[name] = source
	c.sourceStates[name] = newSourceState()
}

// Refresh causes the controller to reprocess all of the blobs
// in the attached sources.
func (c *Controller) Refresh(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, source := range c.sources {
		if err := cadata.ForEach(ctx, source.Set, func(id cadata.ID) error {
			return c.handleID(ctx, id)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Notify tells the controller than an ID has been added or deleted
// in one of the sources.
// Notify blocks while the controller updates the desired sets
// but does not wait until all sink sets are reconciled.
func (c *Controller) Notify(ctx context.Context, id cadata.ID) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handleID(ctx, id)
}

func (c *Controller) handleID(ctx context.Context, id cadata.ID) error {
	// first update all the desired sets
	sinkNames := c.whereShould(ctx, id)
	for _, sinkName := range sinkNames {
		sink := c.sinks[sinkName]
		sinkState := c.sinkStates[sinkName]
		if err := sink.Desired.Add(ctx, id); err != nil {
			return err
		}
		sinkState.addToPending(id)
		sink.Notify(id)
	}
	return nil
}

// Flush blocks until any blobs pending for source have been persisted by the appropriate sinks.
func (c *Controller) Flush(ctx context.Context, source string) error {
	c.mu.RLock()
	defer c.mu.RLock()
	eg := errgroup.Group{}
	for _, sinkStates := range c.sinkStates {
		sinkState := sinkStates
		eg.Go(func() error {
			sinkState.withPending(func(pending map[cadata.ID]struct{}) {

			})
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *Controller) whereShould(ctx context.Context, id cadata.ID) (ret []string) {
	// TODO: eventually take sources into account
	for _, source := range c.sources {
		exists, err := source.Set.Exists(ctx, id)
		if err != nil {
			exists = true
		}
		if exists {
			for name := range c.sinks {
				ret = append(ret, name)
			}
		}
	}
	return nil
}

type sinkState struct {
	mu      sync.Mutex
	pending map[cadata.ID]struct{}
}

func newSinkState() *sinkState {
	return &sinkState{
		pending: make(map[cadata.ID]struct{}),
	}
}

func (ss *sinkState) addToPending(id cadata.ID) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.pending[id] = struct{}{}
}

func (ss *sinkState) withPending(fn func(map[cadata.ID]struct{})) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	fn(ss.pending)
}

type sourceState struct {
	mu      sync.Mutex
	pending map[cadata.ID]struct{}
}

func newSourceState() *sourceState {
	return &sourceState{
		pending: make(map[cadata.ID]struct{}),
	}
}
