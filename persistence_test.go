package leaselock

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 重启恢复：未过期的 lease 在重启后仍然有效，他人无法抢占。
func TestRestartKeepsValidLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases.json")
	clock := NewFakeClock(testBase)

	svc1 := NewLockService(mustFileStore(t, path), clock)
	lease, err := svc1.Acquire("res", "A", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：同一状态文件、同一时钟，重新构建服务。
	clock.Advance(4 * time.Second)
	svc2 := NewLockService(mustFileStore(t, path), clock)

	got, valid, err := svc2.Inspect("res")
	if err != nil {
		t.Fatal(err)
	}
	if !valid || got.Owner != "A" || got.Token != lease.Token {
		t.Fatalf("valid lease must survive restart, got %+v valid=%v", got, valid)
	}
	if _, err := svc2.Acquire("res", "B", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("B must not acquire while A's lease is valid, got %v", err)
	}
	// A 重启后可以用原 token 继续续约。
	if _, err := svc2.Renew("res", "A", lease.Token, 10*time.Second); err != nil {
		t.Fatalf("A must renew after restart: %v", err)
	}
}

// 重启恢复：已过期的 lease 不得被错误恢复为有效。
func TestRestartDoesNotResurrectExpiredLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases.json")
	clock := NewFakeClock(testBase)

	svc1 := NewLockService(mustFileStore(t, path), clock)
	old, err := svc1.Acquire("res", "A", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 时钟走过 lease 有效期后“重启”。
	clock.Advance(11 * time.Second)
	svc2 := NewLockService(mustFileStore(t, path), clock)

	if _, valid, err := svc2.Inspect("res"); err != nil || valid {
		t.Fatalf("expired lease must not be valid after restart, valid=%v err=%v", valid, err)
	}
	// 旧持有者不得续约。
	if _, err := svc2.Renew("res", "A", old.Token, 10*time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired lease must not renew after restart, got %v", err)
	}
	// 新持有者可获取，且 fencing token 继续递增。
	next, err := svc2.Acquire("res", "B", 10*time.Second)
	if err != nil {
		t.Fatalf("B must acquire after restart with expired lease: %v", err)
	}
	if next.Token <= old.Token {
		t.Fatalf("token must keep increasing across restart: %d -> %d", old.Token, next.Token)
	}
}

// fencing token 计数器必须持久化：重启后不得回退。
func TestTokenCounterSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases.json")
	clock := NewFakeClock(testBase)

	var last uint64
	for round := 0; round < 3; round++ {
		svc := NewLockService(mustFileStore(t, path), clock)
		lease, err := svc.Acquire("res", "A", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if lease.Token <= last {
			t.Fatalf("token regressed across restart: %d -> %d", last, lease.Token)
		}
		last = lease.Token
		if err := svc.Release("res", "A", lease.Token); err != nil {
			t.Fatal(err)
		}
	}
	if last != 3 {
		t.Fatalf("want 3 tokens issued, got %d", last)
	}
}

func mustFileStore(t *testing.T, path string) *FileStore {
	t.Helper()
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
