package client

import (
	"errors"
	"testing"
	"time"

	"leaselock/lease"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestPausedClientLosesLeaseAndIsFencedOut simulates the classic fencing
// scenario: a client pauses longer than its lease, another client takes
// over, and the recovered stale holder is rejected by the fencing token.
func TestPausedClientLosesLeaseAndIsFencedOut(t *testing.T) {
	clock := lease.NewManualClock(t0)
	svc, err := lease.NewService(clock, "")
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	store := lease.NewFencedStore()
	const res = "db"
	ttl := 10 * time.Second

	old := New("old-holder", svc, ttl)
	fresh := New("new-holder", svc, ttl)

	ok, err := old.Acquire(res)
	if err != nil || !ok {
		t.Fatalf("old acquire: ok=%v err=%v", ok, err)
	}
	oldToken := old.Token()

	// Old holder freezes (GC pause / partition) and stops renewing.
	old.Pause()
	clock.Advance(15 * time.Second) // lease expires while paused

	// New holder can now acquire, with a strictly newer token.
	ok, err = fresh.Acquire(res)
	if err != nil || !ok {
		t.Fatalf("new acquire after expiry: ok=%v err=%v", ok, err)
	}
	if fresh.Token() <= oldToken {
		t.Fatalf("new token %d must be > old token %d", fresh.Token(), oldToken)
	}
	if err := fresh.Write(store, res, "fresh-data"); err != nil {
		t.Fatalf("fresh write: %v", err)
	}

	// Old holder wakes up believing it still owns the lock.
	old.Resume()
	if ok, _ := old.Renew(res); ok {
		t.Fatal("stale holder must not be able to renew an expired lease")
	}
	if ok, _ := old.Acquire(res); ok {
		t.Fatal("stale holder must not reacquire while new lease is valid")
	}
	err = old.Write(store, res, "stale-data")
	if !errors.Is(err, lease.ErrStaleFencingToken) {
		t.Fatalf("stale write must be fenced out, got %v", err)
	}
	if v, _ := store.Read(res); v != "fresh-data" {
		t.Fatalf("store corrupted: %q", v)
	}
}

// TestClientRenewKeepsLeaseAlive shows a healthy client renewing within
// the TTL keeps the lease and blocks competitors.
func TestClientRenewKeepsLeaseAlive(t *testing.T) {
	clock := lease.NewManualClock(t0)
	svc, _ := lease.NewService(clock, "")
	ttl := 10 * time.Second

	a := New("a", svc, ttl)
	b := New("b", svc, ttl)

	if ok, _ := a.Acquire("res"); !ok {
		t.Fatal("a acquire failed")
	}
	// Renew every 8s for 3 rounds: total elapsed 24s > TTL, still held.
	for i := 0; i < 3; i++ {
		clock.Advance(8 * time.Second)
		if ok, _ := a.Renew("res"); !ok {
			t.Fatalf("renew round %d failed", i)
		}
	}
	if ok, _ := b.Acquire("res"); ok {
		t.Fatal("b must not acquire while a keeps renewing")
	}
	if ok, _ := a.Release("res"); !ok {
		t.Fatal("a release failed")
	}
	if ok, _ := b.Acquire("res"); !ok {
		t.Fatal("b must acquire after a releases")
	}
}
