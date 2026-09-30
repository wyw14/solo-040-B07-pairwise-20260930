package interlock

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func TestReserveSameDirectionDoesNotOverwriteOwner(t *testing.T) {
	arbiter := NewArbiter()
	var commits int32

	first, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-A", "req-A", func() (uuid.UUID, error) {
		atomic.AddInt32(&commits, 1)
		return uuid.New(), nil
	})
	if err != nil {
		t.Fatalf("officer A reserve: %v", err)
	}

	// Officer B requests the same direction with a fresh request id: it must
	// fail and must not overwrite A's ownership.
	_, err = arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-B", "req-B", func() (uuid.UUID, error) {
		atomic.AddInt32(&commits, 1)
		return uuid.New(), nil
	})
	var inFlight *InFlightError
	if !errors.As(err, &inFlight) {
		t.Fatalf("expected InFlightError, got %v", err)
	}
	if inFlight.Existing.Owner != "officer-A" || inFlight.Existing.RequestID != "req-A" {
		t.Fatalf("in-flight reservation points at %+v, want officer-A/req-A", inFlight.Existing)
	}
	if atomic.LoadInt32(&commits) != 1 {
		t.Fatalf("B's rejected request must not generate a command, commits=%d", commits)
	}

	current, ok := arbiter.Current("chamber-1")
	if !ok || current.Owner != "officer-A" || current.RequestID != "req-A" {
		t.Fatalf("reservation was overwritten: %+v", current)
	}
	if current.CommandID != first.CommandID {
		t.Fatalf("reservation command identity changed: %s vs %s", current.CommandID, first.CommandID)
	}

	// B cannot release A's reservation; A can.
	if arbiter.Release("chamber-1", "officer-B", "req-B") {
		t.Fatal("officer B released a reservation it does not own")
	}
	if !arbiter.Release("chamber-1", "officer-A", "req-A") {
		t.Fatal("officer A could not release its own reservation")
	}
}

func TestReserveOpposedDirectionRejected(t *testing.T) {
	arbiter := NewArbiter()
	_, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-A", "req-A", func() (uuid.UUID, error) {
		return uuid.New(), nil
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	_, err = arbiter.Reserve("chamber-1", model.DirectionDownstream, "officer-B", "req-B", func() (uuid.UUID, error) {
		t.Fatal("commit must not run for an opposed direction")
		return uuid.Nil, nil
	})
	if !errors.Is(err, ErrOpposedDirection) {
		t.Fatalf("expected ErrOpposedDirection, got %v", err)
	}
}

func TestReserveReplayReturnsOriginalReservation(t *testing.T) {
	arbiter := NewArbiter()
	var commits int32
	commit := func() (uuid.UUID, error) {
		atomic.AddInt32(&commits, 1)
		return uuid.New(), nil
	}

	first, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-A", "req-A", commit)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	second, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-A", "req-A", commit)
	if err != nil {
		t.Fatalf("replayed reserve: %v", err)
	}
	if first != second {
		t.Fatalf("replay returned different reservation:\n%+v\n%+v", first, second)
	}
	if commits != 1 {
		t.Fatalf("commit ran %d times, want exactly once", commits)
	}
}

func TestReserveReplayRejectsDifferentOwner(t *testing.T) {
	arbiter := NewArbiter()
	_, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-A", "req-A", func() (uuid.UUID, error) {
		return uuid.New(), nil
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	_, err = arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-B", "req-A", func() (uuid.UUID, error) {
		return uuid.New(), nil
	})
	if !errors.Is(err, ErrRequestMismatch) {
		t.Fatalf("expected ErrRequestMismatch, got %v", err)
	}
}

func TestReleaseRequiresMatchingRequestID(t *testing.T) {
	arbiter := NewArbiter()
	if _, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "officer-A", "req-A", func() (uuid.UUID, error) {
		return uuid.New(), nil
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if arbiter.Release("chamber-1", "officer-A", "stale-token") {
		t.Fatal("release with a non-matching request id succeeded")
	}
	if _, ok := arbiter.Current("chamber-1"); !ok {
		t.Fatal("reservation disappeared after failed release")
	}
}

func TestConcurrentReservesOnlyOneWins(t *testing.T) {
	arbiter := NewArbiter()
	var commits int32
	const n = 32

	var wg sync.WaitGroup
	results := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, err := arbiter.Reserve("chamber-1", model.DirectionUpstream,
				"officer-"+string(rune('A'+i%2)), "req-"+string(rune('A'+i)),
				func() (uuid.UUID, error) {
					atomic.AddInt32(&commits, 1)
					return uuid.New(), nil
				})
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	wins, conflicts := 0, 0
	for err := range results {
		var inFlight *InFlightError
		switch {
		case err == nil:
			wins++
		case errors.As(err, &inFlight):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 || conflicts != n-1 {
		t.Fatalf("expected exactly 1 win and %d conflicts, got %d wins and %d conflicts", n-1, wins, conflicts)
	}
	if commits != 1 {
		t.Fatalf("commit ran %d times, want exactly once", commits)
	}
}
