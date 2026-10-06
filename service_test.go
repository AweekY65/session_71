package leaselock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

var testBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestService() (*LockService, *FakeClock) {
	clock := NewFakeClock(testBase)
	return NewLockService(NewMemoryStore(), clock), clock
}

// 竞争获取：N 个 goroutine 同时抢同一资源，恰好一个成功。
func TestAcquireContention(t *testing.T) {
	svc, _ := newTestService()

	const contenders = 32
	var wg sync.WaitGroup
	results := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := string(rune('A'+i%26)) + string(rune('0'+i/26))
			_, err := svc.Acquire("res", owner, 10*time.Second)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	successes, held := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrLeaseHeld):
			held++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || held != contenders-1 {
		t.Fatalf("want 1 success and %d ErrLeaseHeld, got %d and %d",
			contenders-1, successes, held)
	}
}

// 续约：未过期前续约成功并延长过期时间。
func TestRenewExtendsLease(t *testing.T) {
	svc, clock := newTestService()

	lease, err := svc.Acquire("res", "A", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(6 * time.Second)

	renewed, err := svc.Renew("res", "A", lease.Token, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wantExpiry := testBase.Add(16 * time.Second)
	if !renewed.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("want expiry %v, got %v", wantExpiry, renewed.ExpiresAt)
	}

	// t=15s：仍在续约后的有效期内，别人拿不到。
	clock.Advance(9 * time.Second)
	if _, err := svc.Acquire("res", "B", 10*time.Second); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	// t=17s：超过续约后的过期时间，B 可以拿到。
	clock.Advance(2 * time.Second)
	if _, err := svc.Acquire("res", "B", 10*time.Second); err != nil {
		t.Fatalf("want acquire after expiry, got %v", err)
	}
}

// 过期租约不得续期。
func TestExpiredLeaseCannotRenew(t *testing.T) {
	svc, clock := newTestService()

	lease, err := svc.Acquire("res", "A", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)

	if _, err := svc.Renew("res", "A", lease.Token, 10*time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("want ErrLeaseExpired, got %v", err)
	}
}

// 错误的 owner/token 不能续约或释放他人的锁。
func TestRenewAndReleaseRequireMatchingOwnerAndToken(t *testing.T) {
	svc, _ := newTestService()
	lease, err := svc.Acquire("res", "A", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Renew("res", "B", lease.Token, 10*time.Second); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("renew with wrong owner: want ErrLeaseNotHeld, got %v", err)
	}
	if _, err := svc.Renew("res", "A", lease.Token+99, 10*time.Second); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("renew with wrong token: want ErrLeaseNotHeld, got %v", err)
	}
	if err := svc.Release("res", "B", lease.Token); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("release with wrong owner: want ErrLeaseNotHeld, got %v", err)
	}
}

// 释放后重获：release 之后其他客户端立即可获取。
func TestReleaseThenReacquire(t *testing.T) {
	svc, _ := newTestService()

	lease, err := svc.Acquire("res", "A", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Acquire("res", "B", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	if err := svc.Release("res", "A", lease.Token); err != nil {
		t.Fatal(err)
	}
	lease2, err := svc.Acquire("res", "B", time.Minute)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	if lease2.Token <= lease.Token {
		t.Fatalf("token must increase: %d -> %d", lease.Token, lease2.Token)
	}
}

// fencing token 严格单调递增（释放-重获与过期-重获两条路径）。
func TestFencingTokenMonotonic(t *testing.T) {
	svc, clock := newTestService()

	var prev uint64
	// 路径一：release 后重新获取。
	for i := 0; i < 5; i++ {
		lease, err := svc.Acquire("res", "A", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if lease.Token <= prev {
			t.Fatalf("token not increasing: %d -> %d", prev, lease.Token)
		}
		prev = lease.Token
		if err := svc.Release("res", "A", lease.Token); err != nil {
			t.Fatal(err)
		}
	}
	// 路径二：不释放，等过期后被别人获取。
	lease, err := svc.Acquire("res", "A", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Token <= prev {
		t.Fatalf("token not increasing: %d -> %d", prev, lease.Token)
	}
	clock.Advance(6 * time.Second)
	lease2, err := svc.Acquire("res", "B", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease2.Token <= lease.Token {
		t.Fatalf("token not increasing across expiry: %d -> %d", lease.Token, lease2.Token)
	}
}
