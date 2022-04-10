package control

import (
	"context"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/sirupsen/logrus"
)

type sinkInfo struct {
	Desired cadata.Set
	Notify  func(cadata.ID, bool)
	Cost    Cost
}

type Planner struct {
	log     *logrus.Logger
	sources map[string]Source
	sinks   map[string]sinkInfo
}

func NewPlanner(log *logrus.Logger) *Planner {
	return &Planner{
		log:     log,
		sources: make(map[string]Source),
		sinks:   make(map[string]sinkInfo),
	}
}

func (p *Planner) AttachSource(name string, source Source) {
	p.sources[name] = source
}

func (p *Planner) AttachSink(name string, desired cadata.Set, cost Cost, notify func(cadata.ID, bool)) {
	p.sinks[name] = sinkInfo{
		Desired: desired,
		Cost:    cost,
		Notify:  notify,
	}
}

func (p *Planner) SourceChange(ctx context.Context, id cadata.ID) error {
	for _, source := range p.sources {
		exists, err := source.Set.Exists(ctx, id)
		if err != nil {
			return err
		}
		if exists {
			return p.handleAdd(ctx, id)
		}
	}
	return p.handleDelete(ctx, id)
}

func (p *Planner) SinkChange(ctx context.Context, id cadata.ID) error {
	return nil
}

// Refresh causes the planner to reprocess all of the blobs
// in the attached sources.
func (p *Planner) Refresh(ctx context.Context) error {
	return p.RefreshSpan(ctx, state.ByteSpan{})
}

func (p *Planner) RefreshSpan(ctx context.Context, span state.ByteSpan) error {
	for name, source := range p.sources {
		log := p.log.WithFields(logrus.Fields{"source": name, "span": span})
		log.Info("replanning for source...")
		if err := cadata.ForEachSpan(ctx, source.Set, span, func(id cadata.ID) error {
			return p.handleAdd(ctx, id)
		}); err != nil {
			return err
		}
		log.Info("done replanning for source")
	}
	return nil
}

func (p *Planner) handleAdd(ctx context.Context, id cadata.ID) error {
	// first update all the desired sets
	sinkNames := p.whereShould(ctx, id)
	for _, sinkName := range sinkNames {
		sink := p.sinks[sinkName]
		alreadyExists, err := sink.Desired.Exists(ctx, id)
		if err != nil {
			alreadyExists = false // assume the worst
		}
		if err := sink.Desired.Add(ctx, id); err != nil {
			return err
		}
		if !alreadyExists {
			sink.Notify(id, false)
		}
	}
	return nil
}

func (p *Planner) handleDelete(ctx context.Context, id cadata.ID) error {
	return nil
}

func (p *Planner) whereShould(ctx context.Context, id cadata.ID) (ret []string) {
	// TODO: eventually take sources into account
	for _, source := range p.sources {
		exists, err := source.Set.Exists(ctx, id)
		if err != nil {
			exists = true
		}
		if exists {
			// TODO: configure this for now add to all sinks
			for name := range p.sinks {
				ret = append(ret, name)
			}
			break
		}
	}
	return ret
}
