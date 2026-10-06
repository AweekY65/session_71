// Package client simulates lock clients running as goroutines. A client
// can be paused (modelling a GC stop-the-world pause, network partition or
// process freeze) and later resumed, which is the classic fencing-token
// failure scenario.
package client

import (
	"sync"
	"time"

	"leaselock/lease"
)

// Client is a simulated lock client.
type Client struct {
	ID  string
	svc *lease.Service
	ttl time.Duration

	mu       sync.Mutex
	paused   bool
	lease    lease.Lease
	hasLease bool
}

func New(id string, svc *lease.Service, ttl time.Duration) *Client {
	return &Client{ID: id, svc: svc, ttl: ttl}
}

// Pause freezes the client: renewals stop happening (as if the goroutine
// were suspended). The lease keeps its original expiry and will lapse.
func (c *Client) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
}

// Resume wakes a paused client. Its in-memory view (including the old
// fencing token) is unchanged, mirroring a process that does not know it
// lost the lease.
func (c *Client) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
}

func (c *Client) Paused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

// Acquire tries to take the lease on resource.
func (c *Client) Acquire(resource string) (bool, error) {
	c.mu.Lock()
	if c.paused {
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()

	l, ok, err := c.svc.Acquire(resource, c.ID, c.ttl)
	if err != nil || !ok {
		return ok, err
	}
	c.mu.Lock()
	c.lease, c.hasLease = l, true
	c.mu.Unlock()
	return true, nil
}

// Renew extends the held lease. A paused client does nothing (it is
// frozen), so its lease will expire on the service side.
func (c *Client) Renew(resource string) (bool, error) {
	c.mu.Lock()
	if c.paused || !c.hasLease {
		c.mu.Unlock()
		return false, nil
	}
	l := c.lease
	c.mu.Unlock()

	nl, ok, err := c.svc.Renew(resource, c.ID, l.Token, c.ttl)
	if err != nil {
		return false, err
	}
	if ok {
		c.mu.Lock()
		c.lease = nl
		c.mu.Unlock()
	}
	return ok, nil
}

// Release gives up the held lease.
func (c *Client) Release(resource string) (bool, error) {
	c.mu.Lock()
	if c.paused || !c.hasLease {
		c.mu.Unlock()
		return false, nil
	}
	l := c.lease
	c.mu.Unlock()

	ok, err := c.svc.Release(resource, c.ID, l.Token)
	if ok {
		c.mu.Lock()
		c.hasLease = false
		c.mu.Unlock()
	}
	return ok, err
}

// Token returns the fencing token the client currently believes in.
func (c *Client) Token() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lease.Token
}

// Write performs a fenced write to the downstream store using the client's
// current token. A recovered stale holder is rejected here even though it
// still "thinks" it holds the lock.
func (c *Client) Write(store *lease.FencedStore, resource, value string) error {
	c.mu.Lock()
	token := c.lease.Token
	c.mu.Unlock()
	return store.Write(resource, token, value)
}
