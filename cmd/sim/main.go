// Command sim runs a deterministic, fully local lease-lock simulation.
// It uses a ManualClock, so the whole scenario runs instantly with no
// real sleeping and no external services.
package main

import (
	"errors"
	"fmt"
	"time"

	"leaselock/client"
	"leaselock/lease"
)

func main() {
	clock := lease.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	svc, err := lease.NewService(clock, "") // in-memory only
	must(err)
	store := lease.NewFencedStore()

	const resource = "orders-db"
	ttl := 10 * time.Second

	alice := client.New("alice", svc, ttl)
	bob := client.New("bob", svc, ttl)

	step("alice acquires the lease")
	ok, err := alice.Acquire(resource)
	must(err)
	fmt.Printf("  acquired=%v token=%d\n", ok, alice.Token())

	step("bob tries to acquire while alice holds it -> denied")
	ok, err = bob.Acquire(resource)
	must(err)
	fmt.Printf("  acquired=%v\n", ok)

	step("alice renews, then pauses (GC stop-the-world)")
	ok, err = alice.Renew(resource)
	must(err)
	fmt.Printf("  renewed=%v\n", ok)
	alice.Pause()

	step("clock advances past the lease TTL while alice is frozen")
	clock.Advance(15 * time.Second)

	step("bob acquires now that alice's lease expired -> newer token")
	ok, err = bob.Acquire(resource)
	must(err)
	fmt.Printf("  acquired=%v token=%d\n", ok, bob.Token())

	step("bob writes to the fenced store")
	must(bob.Write(store, resource, "bob's write"))

	step("alice wakes up and still believes she holds the lock")
	alice.Resume()
	ok, err = alice.Renew(resource)
	must(err)
	fmt.Printf("  stale renew succeeded=%v (must be false)\n", ok)

	step("alice tries to write with her old fencing token -> rejected")
	err = alice.Write(store, resource, "alice's stale write")
	fmt.Printf("  rejected=%v (ErrStaleFencingToken=%v)\n",
		err != nil, errors.Is(err, lease.ErrStaleFencingToken))

	v, _ := store.Read(resource)
	fmt.Printf("\nfinal store value: %q (only bob's write survived)\n", v)
}

func step(msg string) { fmt.Printf("==> %s\n", msg) }

func must(err error) {
	if err != nil {
		panic(err)
	}
}
