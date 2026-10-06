package lease

import (
	"errors"
	"fmt"
	"sync"
)

// ErrStaleFencingToken is returned when a write carries a fencing token
// that is not newer than the highest token already seen for the resource.
var ErrStaleFencingToken = errors.New("stale fencing token")

// FencedStore models a downstream resource (e.g. a file or database) that
// validates fencing tokens, so a paused-then-recovered former lock holder
// cannot corrupt state after losing its lease.
type FencedStore struct {
	mu       sync.Mutex
	data     map[string]string
	maxToken map[string]uint64
}

func NewFencedStore() *FencedStore {
	return &FencedStore{data: map[string]string{}, maxToken: map[string]uint64{}}
}

// Write accepts the write only if token is strictly greater than every
// token previously seen for this resource.
func (s *FencedStore) Write(resource string, token uint64, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token <= s.maxToken[resource] {
		return fmt.Errorf("%w: token %d <= last token %d for %q",
			ErrStaleFencingToken, token, s.maxToken[resource], resource)
	}
	s.maxToken[resource] = token
	s.data[resource] = value
	return nil
}

func (s *FencedStore) Read(resource string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[resource]
	return v, ok
}
