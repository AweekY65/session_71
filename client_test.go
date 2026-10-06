package leaselock

import (
	"errors"
	"testing"
	"time"
)

// waitRenew 等待客户端完成一次续约尝试。超时只是防止 goroutine
// 调度异常导致测试挂死的安全网，测试正确性不依赖真实时间。
func waitRenew(t *testing.T, c *Client) {
	t.Helper()
	select {
	case <-c.Renewed():
	case <-time.After(10 * time.Second):
		t.Fatal("client did not attempt renewal")
	}
}

// 客户端后台续约循环：只要持续续约，租约一直有效，别人拿不到锁。
func TestClientRenewLoopKeepsLeaseAlive(t *testing.T) {
	svc, clock := newTestService()
	guard := NewGuardedResource("db")

	a := NewClient("A", svc, clock, 10*time.Second, 4*time.Second)
	if err := a.Start("res"); err != nil {
		t.Fatal(err)
	}
	defer a.Stop("res")

	// 经过 3 个续约周期（t=12s），远超初始 TTL=10s，租约仍然有效。
	for i := 0; i < 3; i++ {
		clock.Advance(4 * time.Second)
		waitRenew(t, a)
	}
	if _, err := svc.Acquire("res", "B", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("lease should still be held by A, got %v", err)
	}
	if a.Lost() {
		t.Fatal("A should not have lost the lease")
	}
	if err := a.WriteTo(guard, "a-write"); err != nil {
		t.Fatalf("A write should succeed: %v", err)
	}
}

// 核心场景：客户端暂停超过 lease 时长后恢复。
// 新持有者必须能拿到锁并写入；旧客户端恢复后续约失败，
// 其携带旧 fencing token 的写入必须被下游资源拒绝。
func TestStaleClientRecoveryAfterPause(t *testing.T) {
	svc, clock := newTestService()
	guard := NewGuardedResource("db")

	a := NewClient("A", svc, clock, 10*time.Second, 4*time.Second)
	if err := a.Start("res"); err != nil {
		t.Fatal(err)
	}
	defer a.Stop("res")

	// t=4s：A 正常续约一次。
	clock.Advance(4 * time.Second)
	waitRenew(t, a)
	staleToken := a.Token()

	// A 被“冻结”（GC 停顿/网络分区/调试器），停止续约。
	a.Pause()
	// t=24s：远超租约过期时间（上次续约后过期于 t=14s）。
	clock.Advance(20 * time.Second)

	// 新持有者 B 获取锁，token 必须更大。
	b := NewClient("B", svc, clock, 10*time.Second, 4*time.Second)
	if err := b.Start("res"); err != nil {
		t.Fatalf("B must acquire after A's lease expired: %v", err)
	}
	defer b.Stop("res")
	if b.Token() <= staleToken {
		t.Fatalf("B token %d must exceed stale token %d", b.Token(), staleToken)
	}
	if err := b.WriteTo(guard, "b-write"); err != nil {
		t.Fatalf("B write must succeed: %v", err)
	}

	// A 恢复：续约必须失败并发现自己已失去锁。
	a.Resume()
	clock.Advance(4 * time.Second)
	waitRenew(t, a)
	if !a.Lost() {
		t.Fatal("A must discover it lost the lease after resume")
	}

	// A 用旧 token 写入下游资源，必须被 fencing 拒绝。
	if err := a.WriteTo(guard, "stale-write"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale write must be rejected with ErrStaleToken, got %v", err)
	}

	// B 仍然可以正常写入。
	if err := b.WriteTo(guard, "b-write-2"); err != nil {
		t.Fatalf("B write after A resumed must succeed: %v", err)
	}

	got := guard.Writes()
	want := []string{"b-write", "b-write-2"}
	if len(got) != len(want) {
		t.Fatalf("guard writes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("guard writes = %v, want %v", got, want)
		}
	}
}

// 暂停时间未超过 lease 时，恢复后续约仍然成功，锁不丢失。
func TestClientPauseShorterThanLeaseKeepsLock(t *testing.T) {
	svc, clock := newTestService()

	a := NewClient("A", svc, clock, 10*time.Second, 4*time.Second)
	if err := a.Start("res"); err != nil {
		t.Fatal(err)
	}
	defer a.Stop("res")

	a.Pause()
	clock.Advance(5 * time.Second) // 暂停 5s < TTL 10s
	a.Resume()
	clock.Advance(4 * time.Second) // t=9s，触发续约
	waitRenew(t, a)

	if a.Lost() {
		t.Fatal("A must keep the lease when pause is shorter than TTL")
	}
	if _, err := svc.Acquire("res", "B", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("lease should still be held by A, got %v", err)
	}
}
