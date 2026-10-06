package leaselock

import "sync"

// GuardedResource models a downstream resource (e.g. a database or
// file) that enforces fencing: it remembers the highest fencing token
// it has ever seen and rejects any write carrying an older token.
// This is what actually protects data when a paused lock holder wakes
// up after losing its lease.
type GuardedResource struct {
	mu       sync.Mutex
	name     string
	maxToken uint64
	writes   []string
}

func NewGuardedResource(name string) *GuardedResource {
	return &GuardedResource{name: name}
}

// Write applies value only if token is at least as new as any token
// seen before. A stale token (from a previous lease holder) is
// rejected with ErrStaleToken.
func (g *GuardedResource) Write(token uint64, value string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if token < g.maxToken {
		return ErrStaleToken
	}
	g.maxToken = token
	g.writes = append(g.writes, value)
	return nil
}

// Writes returns the accepted write log.
func (g *GuardedResource) Writes() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, len(g.writes))
	copy(out, g.writes)
	return out
}

// MaxToken returns the highest fencing token seen so far.
func (g *GuardedResource) MaxToken() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxToken
}
