package lease

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can control it without real sleeps.
type Clock interface {
	Now() time.Time
}

// RealClock is the wall-clock implementation used in production.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// ManualClock is a deterministic clock advanced explicitly by tests.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
