package leaselock

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Lease is a lock grant on a resource with an explicit expiry and a
// strictly increasing fencing token.
type Lease struct {
	Resource  string    `json:"resource"`
	Owner     string    `json:"owner"`
	Token     uint64    `json:"token"`
	GrantedAt time.Time `json:"granted_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Expired reports whether the lease is no longer valid at the given time.
func (l Lease) Expired(now time.Time) bool {
	return !now.Before(l.ExpiresAt)
}

// Store persists lease records and fencing-token counters.
// All state lives in local memory or a local JSON file only.
type Store interface {
	// GetLease returns the stored lease record for a resource, whether
	// or not it is expired. Expiry is judged by the caller's clock.
	GetLease(resource string) (Lease, bool, error)
	PutLease(l Lease) error
	DeleteLease(resource string) error
	// NextToken returns the next fencing token for a resource. The
	// counter is monotonic and durable, even across restarts.
	NextToken(resource string) (uint64, error)
}

type storeState struct {
	Leases map[string]Lease  `json:"leases"`
	Tokens map[string]uint64 `json:"tokens"`
}

func newStoreState() storeState {
	return storeState{
		Leases: make(map[string]Lease),
		Tokens: make(map[string]uint64),
	}
}

// MemoryStore keeps all state in process memory.
type MemoryStore struct {
	mu    sync.Mutex
	state storeState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: newStoreState()}
}

func (m *MemoryStore) GetLease(resource string) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.state.Leases[resource]
	return l, ok, nil
}

func (m *MemoryStore) PutLease(l Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Leases[l.Resource] = l
	return nil
}

func (m *MemoryStore) DeleteLease(resource string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.state.Leases, resource)
	return nil
}

func (m *MemoryStore) NextToken(resource string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Tokens[resource]++
	return m.state.Tokens[resource], nil
}

// FileStore persists state to a local JSON file. Every mutation is
// written atomically (temp file + rename) before returning, so a
// restart never loses a granted lease or an issued fencing token.
type FileStore struct {
	mu    sync.Mutex
	path  string
	state storeState
}

func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, state: newStoreState()}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store file: %w", err)
	}
	if len(data) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return nil, fmt.Errorf("decode store file: %w", err)
	}
	if s.state.Leases == nil {
		s.state.Leases = make(map[string]Lease)
	}
	if s.state.Tokens == nil {
		s.state.Tokens = make(map[string]uint64)
	}
	return s, nil
}

// flush must be called with mu held.
func (f *FileStore) flush() error {
	data, err := json.MarshalIndent(f.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	// fsync the directory so the rename itself is durable.
	if dir, err := os.Open(filepath.Dir(f.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (f *FileStore) GetLease(resource string) (Lease, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.state.Leases[resource]
	return l, ok, nil
}

func (f *FileStore) PutLease(l Lease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Leases[l.Resource] = l
	return f.flush()
}

func (f *FileStore) DeleteLease(resource string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.state.Leases, resource)
	return f.flush()
}

func (f *FileStore) NextToken(resource string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Tokens[resource]++
	if err := f.flush(); err != nil {
		return 0, err
	}
	return f.state.Tokens[resource], nil
}
