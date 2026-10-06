package lease

import "time"

// Lease represents an exclusive, time-bounded grant on a resource.
type Lease struct {
	Resource  string    `json:"resource"`
	Holder    string    `json:"holder"`
	Token     uint64    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Expired reports whether the lease is no longer valid at the given time.
// A lease is valid on [acquired, ExpiresAt); at ExpiresAt it is expired.
func (l Lease) Expired(now time.Time) bool {
	return !now.Before(l.ExpiresAt)
}
