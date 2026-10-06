// demo 用真实时钟演示完整流程：A 持锁 -> A 被“冻结” -> 租约过期 ->
// B 拿到更大的 fencing token -> A 恢复后用旧 token 写入被拒绝。
package main

import (
	"fmt"
	"time"

	"leaselock"
)

func main() {
	clock := leaselock.RealClock{}
	store := leaselock.NewMemoryStore()
	svc := leaselock.NewLockService(store, clock)
	guard := leaselock.NewGuardedResource("orders-db")

	ttl := 300 * time.Millisecond
	a := leaselock.NewClient("A", svc, clock, ttl, 100*time.Millisecond)
	if err := a.Start("orders"); err != nil {
		panic(err)
	}
	fmt.Printf("[t=%v] A acquired lease, fencing token=%d\n", ms(), a.Token())
	must(a.WriteTo(guard, "write-by-A"))

	// 模拟 A 进程冻结（GC 停顿 / 网络分区），停止续约。
	a.Pause()
	fmt.Printf("[t=%v] A paused (no renewals)\n", ms())
	time.Sleep(500 * time.Millisecond) // 超过 TTL，租约过期

	b := leaselock.NewClient("B", svc, clock, ttl, 100*time.Millisecond)
	if err := b.Start("orders"); err != nil {
		panic(err)
	}
	fmt.Printf("[t=%v] B acquired lease, fencing token=%d\n", ms(), b.Token())
	must(b.WriteTo(guard, "write-by-B"))

	// A 恢复，仍拿着旧 token。
	a.Resume()
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("[t=%v] A resumed, lost lease=%v\n", ms(), a.Lost())
	if err := a.WriteTo(guard, "stale-write-by-A"); err != nil {
		fmt.Printf("[t=%v] A's stale write rejected: %v\n", ms(), err)
	}

	fmt.Printf("accepted writes: %v\n", guard.Writes())
	a.Stop("orders")
	b.Stop("orders")
}

var start = time.Now()

func ms() time.Duration { return time.Since(start).Round(50 * time.Millisecond) }

func must(err error) {
	if err != nil {
		panic(err)
	}
}
