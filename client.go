package leaselock

import (
	"sync"
	"time"
)

// Client simulates one lock-holding process as a local goroutine.
// It acquires a lease and renews it in a background loop driven by the
// injectable clock. Pause/Resume simulate a frozen process (GC stop,
// network partition, debugger break): while paused no renewals happen.
type Client struct {
	ID            string
	renewInterval time.Duration
	ttl           time.Duration

	svc   *LockService
	clock Clock

	mu       sync.Mutex
	lease    Lease
	hasLease bool
	lost     bool
	paused   bool
	lastErr  error

	stopCh  chan struct{}
	stopped sync.Once

	// renewed receives one notification per renewal attempt (success
	// or failure), letting tests synchronize without sleeping.
	renewed chan struct{}
}

func NewClient(id string, svc *LockService, clock Clock, ttl, renewInterval time.Duration) *Client {
	return &Client{
		ID:            id,
		svc:           svc,
		clock:         clock,
		ttl:           ttl,
		renewInterval: renewInterval,
		stopCh:        make(chan struct{}),
		renewed:       make(chan struct{}, 64),
	}
}

// Start acquires the lease on resource and starts the renewal loop.
func (c *Client) Start(resource string) error {
	lease, err := c.svc.Acquire(resource, c.ID, c.ttl)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.lease = lease
	c.hasLease = true
	c.mu.Unlock()
	// Create the ticker synchronously so no Advance can be missed
	// between Start returning and the loop goroutine starting.
	ticker := c.clock.NewTicker(c.renewInterval)
	go c.renewLoop(resource, ticker)
	return nil
}

func (c *Client) renewLoop(resource string, ticker Ticker) {
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C():
			c.mu.Lock()
			paused := c.paused
			has := c.hasLease
			lease := c.lease
			c.mu.Unlock()
			if paused || !has {
				continue
			}
			_, err := c.svc.Renew(resource, c.ID, lease.Token, c.ttl)
			c.mu.Lock()
			c.lastErr = err
			if err != nil {
				// The lease is gone (expired and possibly re-granted
				// to someone else). This client must stop acting as
				// the holder; its fencing token is now stale.
				c.lost = true
				c.hasLease = false
			}
			c.mu.Unlock()
			select {
			case c.renewed <- struct{}{}:
			default:
			}
		}
	}
}

// Renewed returns a channel notified after each renewal attempt.
func (c *Client) Renewed() <-chan struct{} { return c.renewed }

// Pause simulates a frozen process: renewals stop happening.
func (c *Client) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
}

// Resume wakes a paused process. The next renewal attempt will fail
// if the lease expired while paused.
func (c *Client) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
}

// Stop terminates the renewal loop and releases the lease if still held.
func (c *Client) Stop(resource string) {
	c.stopped.Do(func() { close(c.stopCh) })
	c.mu.Lock()
	lease, has := c.lease, c.hasLease
	c.mu.Unlock()
	if has {
		_ = c.svc.Release(resource, c.ID, lease.Token)
	}
}

// Lost reports whether this client has discovered it lost the lease.
func (c *Client) Lost() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lost
}

// Token returns the fencing token this client believes it holds.
func (c *Client) Token() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lease.Token
}

// WriteTo performs a fenced write to a guarded resource using the
// client's current token. A paused-and-recovered client whose lease
// was taken over will be rejected with ErrStaleToken.
func (c *Client) WriteTo(g *GuardedResource, value string) error {
	c.mu.Lock()
	token := c.lease.Token
	c.mu.Unlock()
	return g.Write(token, value)
}
