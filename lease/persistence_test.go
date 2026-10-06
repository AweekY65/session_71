package lease

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPersistenceSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	clock := NewManualClock(t0)

	svc, err := NewService(clock, path)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	l, ok, err := svc.Acquire("res", "a", time.Hour)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	// Simulate a restart: new Service over the same file, same clock.
	svc2, err := NewService(clock, path)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	cur, ok := svc2.Current("res")
	if !ok {
		t.Fatal("valid lease must survive restart")
	}
	if cur.Holder != "a" || cur.Token != l.Token {
		t.Fatalf("restored lease = %+v, want holder=a token=%d", cur, l.Token)
	}
	// Mutual exclusion still enforced after restart.
	if _, ok, _ := svc2.Acquire("res", "b", time.Hour); ok {
		t.Fatal("another acquire must fail while restored lease is valid")
	}
	// Fencing token monotonicity survives restart.
	clock.Advance(2 * time.Hour) // let restored lease expire
	l2, ok, err := svc2.Acquire("res", "b", time.Hour)
	if err != nil || !ok {
		t.Fatalf("acquire after expiry: ok=%v err=%v", ok, err)
	}
	if l2.Token <= l.Token {
		t.Fatalf("token after restart = %d, must be > %d", l2.Token, l.Token)
	}
}

func TestExpiredLeaseNotRestoredAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	clock := NewManualClock(t0)

	svc, err := NewService(clock, path)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if _, ok, _ := svc.Acquire("res", "a", 10*time.Second); !ok {
		t.Fatal("acquire failed")
	}

	// Process "down" while time passes beyond the lease expiry.
	clock.Advance(time.Minute)

	svc2, err := NewService(clock, path)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if _, ok := svc2.Current("res"); ok {
		t.Fatal("expired lease must not be restored after restart")
	}
	if _, ok, _ := svc2.Acquire("res", "b", 10*time.Second); !ok {
		t.Fatal("resource must be acquirable after expired lease is pruned")
	}
}
