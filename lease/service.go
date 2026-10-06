package lease

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// persistedState is the on-disk representation of the lock service.
type persistedState struct {
	NextToken uint64           `json:"next_token"`
	Leases    map[string]Lease `json:"leases"`
}

// Service is a fully local, single-process lease lock manager.
// State lives in memory and is optionally mirrored to a JSON file so
// the service can be restarted without losing fencing-token monotonicity.
type Service struct {
	mu    sync.Mutex
	clock Clock
	path  string // empty means in-memory only
	state persistedState
}

// NewService creates a Service. If path is non-empty and the file exists,
// state is loaded from it; leases already expired at load time are discarded.
func NewService(clock Clock, path string) (*Service, error) {
	s := &Service{
		clock: clock,
		path:  path,
		state: persistedState{NextToken: 1, Leases: map[string]Lease{}},
	}
	if path == "" {
		return s, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	var loaded persistedState
	if err := json.Unmarshal(data, &loaded); err != nil {
		return nil, fmt.Errorf("decode state file: %w", err)
	}
	if loaded.Leases == nil {
		loaded.Leases = map[string]Lease{}
	}
	if loaded.NextToken == 0 {
		loaded.NextToken = 1
	}
	// Never resurrect leases that expired while the process was down.
	now := clock.Now()
	for name, l := range loaded.Leases {
		if l.Expired(now) {
			delete(loaded.Leases, name)
		}
	}
	s.state = loaded
	return s, nil
}

// persist writes the current state to disk atomically (tmp file + rename).
// Caller must hold s.mu.
func (s *Service) persist() error {
	if s.path == "" {
		return nil
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Acquire attempts to take the lease on resource for holder with the given
// TTL. It fails if a valid (unexpired) lease already exists. On success it
// returns the new lease carrying a strictly increasing fencing token.
func (s *Service) Acquire(resource, holder string, ttl time.Duration) (Lease, bool, error) {
	if ttl <= 0 {
		return Lease{}, false, fmt.Errorf("ttl must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.state.Leases[resource]; ok && !existing.Expired(s.clock.Now()) {
		return Lease{}, false, nil
	}
	l := Lease{
		Resource:  resource,
		Holder:    holder,
		Token:     s.state.NextToken,
		ExpiresAt: s.clock.Now().Add(ttl),
	}
	s.state.NextToken++
	s.state.Leases[resource] = l
	if err := s.persist(); err != nil {
		return Lease{}, false, err
	}
	return l, true, nil
}

// Renew extends the lease on resource. It succeeds only if the caller
// presents the current holder identity and fencing token and the lease
// has not yet expired. Expired leases can never be renewed.
func (s *Service) Renew(resource, holder string, token uint64, ttl time.Duration) (Lease, bool, error) {
	if ttl <= 0 {
		return Lease{}, false, fmt.Errorf("ttl must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.state.Leases[resource]
	if !ok || l.Expired(s.clock.Now()) {
		return Lease{}, false, nil
	}
	if l.Holder != holder || l.Token != token {
		return Lease{}, false, nil
	}
	l.ExpiresAt = s.clock.Now().Add(ttl)
	s.state.Leases[resource] = l
	if err := s.persist(); err != nil {
		return Lease{}, false, err
	}
	return l, true, nil
}

// Release drops the lease if holder and token match. Releasing an already
// expired lease is a harmless no-op that still reports success.
func (s *Service) Release(resource, holder string, token uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.state.Leases[resource]
	if !ok {
		return false, nil
	}
	if l.Holder != holder || l.Token != token {
		return false, nil
	}
	delete(s.state.Leases, resource)
	if err := s.persist(); err != nil {
		return false, err
	}
	return true, nil
}

// Current returns the lease on resource if one exists and is still valid.
func (s *Service) Current(resource string) (Lease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.state.Leases[resource]
	if !ok || l.Expired(s.clock.Now()) {
		return Lease{}, false
	}
	return l, true
}

// StatePath returns the persistence file path ("" if in-memory only).
func (s *Service) StatePath() string { return s.path }

// StateDir is a helper for callers that want a state file inside a dir.
func StatePathIn(dir string) string { return filepath.Join(dir, "lease-state.json") }
