package leaselock

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrLeaseHeld means another owner currently holds a valid lease.
	ErrLeaseHeld = errors.New("leaselock: lease is held by another owner")
	// ErrLeaseNotHeld means no lease record matches the owner/token.
	ErrLeaseNotHeld = errors.New("leaselock: no matching lease held")
	// ErrLeaseExpired means the lease expired and cannot be renewed.
	ErrLeaseExpired = errors.New("leaselock: lease has expired")
	// ErrStaleToken means a fencing token is older than one already seen.
	ErrStaleToken = errors.New("leaselock: stale fencing token")
)

// LockService implements lease-based distributed locking entirely
// against a local Store. A single mutex serializes check-and-set
// operations, which is sufficient because all clients are local
// goroutines/processes sharing this service instance (or its file).
type LockService struct {
	mu    sync.Mutex
	store Store
	clock Clock
}

func NewLockService(store Store, clock Clock) *LockService {
	return &LockService{store: store, clock: clock}
}

// Acquire grants a lease on resource to owner for ttl.
// Fails with ErrLeaseHeld if a valid (unexpired) lease exists.
// Every successful grant issues a strictly increasing fencing token.
func (s *LockService) Acquire(resource, owner string, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("leaselock: ttl must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	if existing, ok, err := s.store.GetLease(resource); err != nil {
		return Lease{}, err
	} else if ok && !existing.Expired(now) {
		return Lease{}, ErrLeaseHeld
	}

	token, err := s.store.NextToken(resource)
	if err != nil {
		return Lease{}, err
	}
	lease := Lease{
		Resource:  resource,
		Owner:     owner,
		Token:     token,
		GrantedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	if err := s.store.PutLease(lease); err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// Renew extends the lease held by owner with the given fencing token.
// An expired lease can never be renewed: the caller must re-acquire.
func (s *LockService) Renew(resource, owner string, token uint64, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("leaselock: ttl must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	lease, ok, err := s.store.GetLease(resource)
	if err != nil {
		return Lease{}, err
	}
	if !ok || lease.Owner != owner || lease.Token != token {
		return Lease{}, ErrLeaseNotHeld
	}
	if lease.Expired(now) {
		return Lease{}, ErrLeaseExpired
	}
	lease.ExpiresAt = now.Add(ttl)
	if err := s.store.PutLease(lease); err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// Release drops the lease held by owner with the given token.
// Releasing an already-expired record owned by the caller is allowed
// (it is a no-op cleanup); releasing someone else's lease is not.
func (s *LockService) Release(resource, owner string, token uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok, err := s.store.GetLease(resource)
	if err != nil {
		return err
	}
	if !ok {
		return ErrLeaseNotHeld
	}
	if lease.Owner != owner || lease.Token != token {
		return ErrLeaseNotHeld
	}
	return s.store.DeleteLease(resource)
}

// Inspect returns the current lease record for a resource and whether
// it is still valid at the service's clock. Intended for observability.
func (s *LockService) Inspect(resource string) (lease Lease, valid bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok, err := s.store.GetLease(resource)
	if err != nil || !ok {
		return Lease{}, false, err
	}
	return l, !l.Expired(s.clock.Now()), nil
}
