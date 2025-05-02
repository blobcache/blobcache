package control

import (
	"context"
	"sync"
	"time"

	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/sirupsen/logrus"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/errgroup"
)

// Controller manages where to store blobs
type Controller struct {
	log    *logrus.Logger
	getSet func(name string) cadata.Set

	mu           sync.RWMutex
	planner      *Planner
	sinks        map[string]Sink
	sourceStates map[string]*sourceState
	sinkStates   map[string]*sinkState

	ctx context.Context
	cf  context.CancelFunc
	eg  errgroup.Group
}

// New creates a new Controller
// getSet will be called to create sets for tracking the desired blobs in each sink.
func New(log *logrus.Logger, getSet func(name string) cadata.Set) *Controller {
	c := &Controller{
		log:    log,
		getSet: getSet,

		planner:      NewPlanner(log),
		sinks:        make(map[string]Sink),
		sourceStates: make(map[string]*sourceState),
		sinkStates:   make(map[string]*sinkState),
	}
	return c
}

func (c *Controller) AttachSink(name string, sink Sink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.planner.AttachSink(name, c.getSet(name), sink.Cost, func(id cadata.ID, isDelete bool) {
		c.fromPlanner(name, id, isDelete)
	})
	c.sinks[name] = sink
	c.sinkStates[name] = newSinkState(sink.Target)
	c.startSinkLoop(name)
}

func (c *Controller) AttachSource(name string, source Source) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.planner.AttachSource(name, source)
	c.sourceStates[name] = newSourceState()
}

// Refresh causes the controller to reprocess all of the blobs
// in the attached sources.
func (c *Controller) Refresh(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.planner.Refresh(ctx); err != nil {
		return err
	}
	eg := errgroup.Group{}
	for name, sink := range c.sinks {
		name := name
		sink := sink
		eg.Go(func() error {
			c.log.Infof("refreshing sink %s", name)
			return sink.Target.Refresh(ctx, c.unionStore(), c.getSet(name))
		})
	}
	return eg.Wait()
}

// Notify tells the controller than an ID has been added or deleted
// in one of the sources.
// Notify blocks while the controller updates the desired sets
// but does not wait until all sink sets are reconciled.
func (c *Controller) Notify(ctx context.Context, id cadata.ID) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.planner.SourceChange(ctx, id)
}

// Flush blocks until any blobs pending for source have been persisted by the appropriate sinks.
func (c *Controller) Flush(ctx context.Context, source string) error {
	c.mu.RLock()
	defer c.mu.RLock()
	eg := errgroup.Group{}
	for _, sinkStates := range c.sinkStates {
		sinkState := sinkStates
		eg.Go(func() error {
			return sinkState.flush(ctx)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *Controller) Close() error {
	for _, ss := range c.sinkStates {
		close(ss.flushReqs)
	}
	return c.eg.Wait()
}

func (c *Controller) fromPlanner(name string, id cadata.ID, isDelete bool) {
	// we will already have the read lock here
	ss := c.sinkStates[name]
	ss.addToPending(id, isDelete, time.Now())
}

func (c *Controller) startSinkLoop(name string) {
	ss := c.sinkStates[name]
	c.eg.Go(func() error {
		for range ss.flushReqs {
			if err := c.flushSink(ss); err != nil {
				c.log.Errorf("error flushing sink %s: %v", name, err)
			}
		}
		return nil
	})
}

func (c *Controller) flushSink(ss *sinkState) error {
	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	src := c.unionStore()
	ops := make([]Op, 0, len(ss.pending))
	for id, isDelete := range ss.pending {
		op := Op{ID: id}
		if !isDelete {
			op.Src = src
		}
		ops = append(ops, op)
	}
	if err := ss.target.Flush(ctx, ops); err != nil {
		return err
	}
	close(ss.doneFlush)
	ss.doneFlush = make(chan struct{})
	return nil
}

func (s *Controller) unionStore() ReadOnlyStore {
	var ret stores.ReadChain
	for _, sink := range s.sinks {
		ret = append(ret, sink.Target.Actual())
	}
	// sort by cost of retrieval
	return ret
}

type sinkState struct {
	target Target

	flushReqs chan struct{}
	mu        sync.Mutex
	pending   map[cadata.ID]bool
	oldest    time.Time
	doneFlush chan struct{}
}

func newSinkState(target Target) *sinkState {
	return &sinkState{
		target: target,

		flushReqs: make(chan struct{}, 1),
		pending:   make(map[cadata.ID]bool),
		doneFlush: make(chan struct{}),
	}
}

func (ss *sinkState) flush(ctx context.Context) error {
	ss.mu.Lock()
	ch := ss.doneFlush
	ss.mu.Unlock()
	ss.enqueueFlush()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (ss *sinkState) enqueueFlush() {
	select {
	case ss.flushReqs <- struct{}{}:
	default:
		// with a buffer of 1, there must already be one enqueued.
	}
}

func (ss *sinkState) addToPending(id cadata.ID, isDelete bool, now time.Time) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.pending) == 0 {
		ss.oldest = now
	}
	ss.pending[id] = isDelete
}

func (ss *sinkState) oldestDuration(now time.Time) time.Duration {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return now.Sub(ss.oldest)
}

func (ss *sinkState) numPending() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.pending)
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
