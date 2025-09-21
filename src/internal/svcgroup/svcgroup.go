package svcgroup

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// Group is a group of services.
type Group struct {
	bgCtx  context.Context
	cf     context.CancelFunc
	wg     sync.WaitGroup
	isDone bool
}

func New(bgCtx context.Context) Group {
	bgCtx, cf := context.WithCancel(bgCtx)
	return Group{
		bgCtx: bgCtx,
		cf:    cf,
	}
}

// Always runs fn in a loop, restarting it if it returns an error.
func (g *Group) Always(fn func(context.Context) error) {
	if g.isDone {
		panic("Group already shutdown")
	}
	g.wg.Add(1)
	ctx := g.bgCtx
	go func() {
		defer g.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for ; ; <-ticker.C {
			if err := fn(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				logctx.Error(g.bgCtx, "error in service, restarting", zap.Error(err))
			} else {
				logctx.Info(g.bgCtx, "service finished")
				return
			}
		}
	}()
}

// Shutdown cancles the background context and waits for all services to finish.
func (g *Group) Shutdown() {
	g.isDone = true
	g.cf()
	g.wg.Wait()
}
