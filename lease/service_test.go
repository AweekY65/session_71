package lease

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestService(t *testing.T) (*Service, *ManualClock) {
	t.Helper()
	clock := NewManualClock(t0)
	svc, err := NewService(clock, "")
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, clock
}

func TestAcquireMutualExclusion(t *testing.T) {
	svc, _ := newTestService(t)

	if _, ok, err := svc.Acquire("res", "a", 10*time.Second); err != nil || !ok {
		t.Fatalf("first acquire should succeed: ok=%v err=%v", ok, err)
	}
	if _, ok, err := svc.Acquire("res", "b", 10*time.Second); err != nil || ok {
		t.Fatalf("second acquire while held must fail: ok=%v err=%v", ok, err)
	}
	// A different resource is independent.
	if _, ok, err := svc.Acquire("other", "b", 10*time.Second); err != nil || !ok {
		t.Fatalf("acquire on different resource should succeed: ok=%v err=%v", ok, err)
	}
}

func TestConcurrentAcquireExactlyOneWinner(t *testing.T) {
	svc, _ := newTestService(t)

	const contenders = 32
	var wg sync.WaitGroup
	tokens := make(chan uint64, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l, ok, err := svc.Acquire("res", fmt.Sprintf("c%d", i), 10*time.Second)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if ok {
				tokens <- l.Token
			}
		}(i)
	}
	wg.Wait()
	close(tokens)

	var winners []uint64
	for tok := range tokens {
		winners = append(winners, tok)
	}
	if len(winners) != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", len(winners))
	}
	if winners[0] != 1 {
		t.Fatalf("first token should be 1, got %d", winners[0])
	}
}

func TestRenewExtendsLease(t *testing.T) {
	svc, clock := newTestService(t)

	l, ok, err := svc.Acquire("res", "a", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	clock.Advance(8 * time.Second) // 2s before expiry
	nl, ok, err := svc.Renew("res", "a", l.Token, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("renew before expiry should succeed: ok=%v err=%v", ok, err)
	}
	want := t0.Add(18 * time.Second)
	if !nl.ExpiresAt.Equal(want) {
		t.Fatalf("expiry = %v, want %v", nl.ExpiresAt, want)
	}
	// Still held past the original expiry.
	clock.Advance(5 * time.Second) // t=13s > original 10s
	if _, ok, err := svc.Acquire("res", "b", 10*time.Second); err != nil || ok {
		t.Fatalf("resource must still be locked after renew: ok=%v err=%v", ok, err)
	}
}

func TestRenewAfterExpiryFails(t *testing.T) {
	svc, clock := newTestService(t)

	l, ok, _ := svc.Acquire("res", "a", 10*time.Second)
	if !ok {
		t.Fatal("acquire failed")
	}
	clock.Advance(11 * time.Second) // lease expired
	if _, ok, err := svc.Renew("res", "a", l.Token, 10*time.Second); err != nil || ok {
		t.Fatalf("renewing an expired lease must fail: ok=%v err=%v", ok, err)
	}
}

func TestRenewWrongHolderOrTokenFails(t *testing.T) {
	svc, _ := newTestService(t)

	l, ok, _ := svc.Acquire("res", "a", 10*time.Second)
	if !ok {
		t.Fatal("acquire failed")
	}
	if _, ok, _ := svc.Renew("res", "b", l.Token, 10*time.Second); ok {
		t.Fatal("renew by wrong holder must fail")
	}
	if _, ok, _ := svc.Renew("res", "a", l.Token+99, 10*time.Second); ok {
		t.Fatal("renew with wrong token must fail")
	}
}

func TestReleaseThenReacquire(t *testing.T) {
	svc, _ := newTestService(t)

	l, ok, _ := svc.Acquire("res", "a", 10*time.Second)
	if !ok {
		t.Fatal("acquire failed")
	}
	if ok, _ := svc.Release("res", "b", l.Token); ok {
		t.Fatal("release by wrong holder must fail")
	}
	if ok, err := svc.Release("res", "a", l.Token); err != nil || !ok {
		t.Fatalf("release by holder should succeed: ok=%v err=%v", ok, err)
	}
	l2, ok, err := svc.Acquire("res", "b", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("reacquire after release should succeed: ok=%v err=%v", ok, err)
	}
	if l2.Token != l.Token+1 {
		t.Fatalf("token after reacquire = %d, want %d", l2.Token, l.Token+1)
	}
}

func TestFencingTokensStrictlyIncreasing(t *testing.T) {
	svc, clock := newTestService(t)

	var prev uint64
	for i := 0; i < 5; i++ {
		l, ok, err := svc.Acquire("res", fmt.Sprintf("h%d", i), 5*time.Second)
		if err != nil || !ok {
			t.Fatalf("acquire %d: ok=%v err=%v", i, ok, err)
		}
		if l.Token <= prev {
			t.Fatalf("token %d not greater than previous %d", l.Token, prev)
		}
		prev = l.Token
		clock.Advance(6 * time.Second) // let it expire, next holder takes over
	}
}

func TestFencedStoreRejectsStaleToken(t *testing.T) {
	store := NewFencedStore()

	if err := store.Write("res", 7, "new"); err != nil {
		t.Fatalf("fresh write: %v", err)
	}
	if err := store.Write("res", 7, "dup"); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("equal token must be rejected: %v", err)
	}
	if err := store.Write("res", 3, "old"); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("older token must be rejected: %v", err)
	}
	if err := store.Write("res", 8, "newer"); err != nil {
		t.Fatalf("newer token must be accepted: %v", err)
	}
	v, _ := store.Read("res")
	if v != "newer" {
		t.Fatalf("store = %q, want %q", v, "newer")
	}
}
