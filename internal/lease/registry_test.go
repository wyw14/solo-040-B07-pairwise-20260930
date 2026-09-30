package lease

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAssignIdempotentForSameOwner(t *testing.T) {
	registry := NewRegistry()
	vesselID := uuid.New()
	now := time.Unix(100, 0).UTC()

	first, created, err := registry.Assign(vesselID, "officer-A", 30*time.Second, now)
	if err != nil || !created {
		t.Fatalf("first assign: created=%v err=%v", created, err)
	}
	// A retries (e.g. client timeout): must get the same lease and token.
	second, created, err := registry.Assign(vesselID, "officer-A", 30*time.Second, now.Add(time.Second))
	if err != nil || created {
		t.Fatalf("retry assign: created=%v err=%v", created, err)
	}
	if first != second {
		t.Fatalf("retry changed the lease:\n%+v\n%+v", first, second)
	}
	if second.FencingToken != first.FencingToken {
		t.Fatalf("fencing token advanced on retry: %d vs %d", second.FencingToken, first.FencingToken)
	}
}

func TestAssignRejectsOtherOwnerWhileActive(t *testing.T) {
	registry := NewRegistry()
	vesselID := uuid.New()
	now := time.Unix(100, 0).UTC()

	if _, _, err := registry.Assign(vesselID, "officer-A", 30*time.Second, now); err != nil {
		t.Fatalf("A assign: %v", err)
	}
	if _, _, err := registry.Assign(vesselID, "officer-B", 30*time.Second, now.Add(time.Second)); err != ErrLeaseInUse {
		t.Fatalf("expected ErrLeaseInUse, got %v", err)
	}
	current, ok := registry.Get(vesselID)
	if !ok || current.Owner != "officer-A" {
		t.Fatalf("lease was stolen: %+v", current)
	}

	// B still cannot release A's lease; A can, then B may take over.
	if registry.Release(vesselID, "officer-B", current.FencingToken) {
		t.Fatal("B released A's lease")
	}
	if !registry.Release(vesselID, "officer-A", current.FencingToken) {
		t.Fatal("A could not release its own lease")
	}
	reassigned, created, err := registry.Assign(vesselID, "officer-B", 30*time.Second, now.Add(2*time.Second))
	if err != nil || !created {
		t.Fatalf("B assign after release: created=%v err=%v", created, err)
	}
	if reassigned.FencingToken != current.FencingToken+1 {
		t.Fatalf("new owner expected token %d, got %d", current.FencingToken+1, reassigned.FencingToken)
	}
}

func TestAssignAllowedAgainAfterExpiry(t *testing.T) {
	registry := NewRegistry()
	vesselID := uuid.New()
	now := time.Unix(100, 0).UTC()

	first, _, err := registry.Assign(vesselID, "officer-A", time.Second, now)
	if err != nil {
		t.Fatalf("A assign: %v", err)
	}
	after, created, err := registry.Assign(vesselID, "officer-B", time.Second, now.Add(2*time.Second))
	if err != nil || !created {
		t.Fatalf("assign after expiry: created=%v err=%v", created, err)
	}
	if after.FencingToken != first.FencingToken+1 || after.Owner != "officer-B" {
		t.Fatalf("post-expiry assign unexpected: %+v (first token %d)", after, first.FencingToken)
	}
}

type assignResult struct {
	owner string
	token uint64
	err   error
}

func TestConcurrentAssignsDoNotOverwrite(t *testing.T) {
	registry := NewRegistry()
	vesselID := uuid.New()
	now := time.Unix(100, 0).UTC()

	done := make(chan assignResult, 200)
	launch := func(owner string, count int) {
		for i := 0; i < count; i++ {
			go func() {
				lease, _, err := registry.Assign(vesselID, owner, time.Minute, now)
				done <- assignResult{owner: lease.Owner, token: lease.FencingToken, err: err}
			}()
		}
	}
	launch("officer-A", 100)
	launch("officer-B", 100)

	winners := map[string]int{}
	for i := 0; i < 200; i++ {
		r := <-done
		if r.err != nil && r.err != ErrLeaseInUse {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.err == nil {
			if r.token != 1 {
				t.Fatalf("winner held token %d, want 1", r.token)
			}
			winners[r.owner]++
		}
	}
	// Exactly one owner's requests succeed; every request from the loser is
	// rejected (the winner's own retries are all idempotent hits on token 1).
	if !((winners["officer-A"] > 0 && winners["officer-B"] == 0) ||
		(winners["officer-B"] > 0 && winners["officer-A"] == 0)) {
		t.Fatalf("both owners obtained the lease: %v", winners)
	}
}
