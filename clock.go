package leaselock

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can inject a deterministic fake clock.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker mirrors time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock is the production clock backed by the wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) NewTicker(d time.Duration) Ticker {
	return realTicker{t: time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// FakeClock is a manually advanced clock for deterministic tests.
// Time only moves when Advance is called; tests never sleep.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	nextID  int
	tickers map[int]*fakeTicker
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start, tickers: make(map[int]*fakeTicker)}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *FakeClock) NewTicker(d time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	t := &fakeTicker{
		c:        make(chan time.Time, 1),
		interval: d,
		deadline: f.now.Add(d),
	}
	f.tickers[id] = t
	return t
}

// Advance moves fake time forward and fires any tickers whose
// deadlines fall within the elapsed window.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for _, t := range f.tickers {
		for !t.deadline.After(f.now) {
			select {
			case t.c <- t.deadline:
			default:
			}
			t.deadline = t.deadline.Add(t.interval)
		}
	}
}

type fakeTicker struct {
	c        chan time.Time
	interval time.Duration
	deadline time.Time
}

func (t *fakeTicker) C() <-chan time.Time { return t.c }
func (t *fakeTicker) Stop()               {}
