package interlock

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func newCommand() model.DeviceCommand {
	return model.DeviceCommand{ID: uuid.New(), ChamberID: "chamber-1", Device: model.DeviceUpstreamGate}
}

func TestReserveFirstApplicationCommits(t *testing.T) {
	arbiter := NewArbiter()
	commits := 0
	reservation, replayed, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		commits++
		return newCommand(), nil
	})
	if err != nil || replayed {
		t.Fatalf("first reserve: replayed=%v err=%v", replayed, err)
	}
	if commits != 1 || reservation.Owner != "operator-a" || reservation.CommandID == uuid.Nil {
		t.Fatalf("unexpected reservation: %+v commits=%d", reservation, commits)
	}
}

// The original incident: B applies in the same direction while A holds the
// reservation. It must be rejected, A stays the owner, and no new command is
// generated.
func TestReserveSameDirectionDifferentOwnerRejected(t *testing.T) {
	arbiter := NewArbiter()
	first, _, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		return newCommand(), nil
	})
	if err != nil {
		t.Fatalf("operator A reserve: %v", err)
	}

	commits := 0
	_, _, err = arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-b", func() (model.DeviceCommand, error) {
		commits++
		return newCommand(), nil
	})
	if !errors.Is(err, ErrReservationHeld) {
		t.Fatalf("operator B err = %v, want ErrReservationHeld", err)
	}
	if commits != 0 {
		t.Fatalf("rejected application ran commit %d times", commits)
	}

	current, ok := arbiter.Current("chamber-1")
	if !ok || current.Owner != "operator-a" || current.CommandID != first.CommandID {
		t.Fatalf("reservation was overwritten: %+v ok=%v", current, ok)
	}
}

func TestReserveOpposedDirectionRejected(t *testing.T) {
	arbiter := NewArbiter()
	_, _, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		return newCommand(), nil
	})
	if err != nil {
		t.Fatalf("upstream reserve: %v", err)
	}
	_, _, err = arbiter.Reserve("chamber-1", model.DirectionDownstream, "operator-a", func() (model.DeviceCommand, error) {
		return newCommand(), nil
	})
	if !errors.Is(err, ErrOpposedReservation) {
		t.Fatalf("opposed reserve err = %v, want ErrOpposedReservation", err)
	}
}

// A legitimate retry from the same owner returns the original reservation and
// must never invoke commit again (no duplicate command/epoch).
func TestReserveSameOwnerRetryReturnsOriginal(t *testing.T) {
	arbiter := NewArbiter()
	first, _, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		return newCommand(), nil
	})
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	commits := 0
	retry, replayed, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		commits++
		return newCommand(), nil
	})
	if err != nil || !replayed {
		t.Fatalf("retry: replayed=%v err=%v", replayed, err)
	}
	if commits != 0 || retry.CommandID != first.CommandID || retry.Owner != "operator-a" {
		t.Fatalf("retry must return the original reservation without commit: %+v", retry)
	}
}

// Concurrent applications (duplicate clicks, parallel goroutines) must commit
// exactly one command, regardless of scheduling.
func TestReserveConcurrentSameDirectionCommitsOnce(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		arbiter := NewArbiter()
		var commits int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, _, _ = arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
					commits++
					return newCommand(), nil
				})
			}()
		}
		close(start)
		wg.Wait()
		if commits != 1 {
			t.Fatalf("attempt %d: commit ran %d times, want exactly 1", attempt, commits)
		}
	}
}

func TestReserveCommitFailureLeavesNoReservation(t *testing.T) {
	arbiter := NewArbiter()
	commitErr := errors.New("journal unavailable")
	_, _, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		return model.DeviceCommand{}, commitErr
	})
	if !errors.Is(err, commitErr) {
		t.Fatalf("err = %v, want commit error", err)
	}
	if _, ok := arbiter.Current("chamber-1"); ok {
		t.Fatal("failed commit must not leave a reservation behind")
	}
}

func TestReleaseEnforcesOwnerAtomically(t *testing.T) {
	arbiter := NewArbiter()
	first, _, err := arbiter.Reserve("chamber-1", model.DirectionUpstream, "operator-a", func() (model.DeviceCommand, error) {
		return newCommand(), nil
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// B cannot release A's reservation — this is the exact failure in the
	// incident where A could no longer release after B overwrote ownership.
	if _, err := arbiter.Release("chamber-1", "operator-b"); err == nil {
		t.Fatal("operator B must not release operator A's reservation")
	}
	if current, ok := arbiter.Current("chamber-1"); !ok || current.Owner != "operator-a" {
		t.Fatal("failed release must leave the reservation intact")
	}

	released, err := arbiter.Release("chamber-1", "operator-a")
	if err != nil {
		t.Fatalf("operator A release: %v", err)
	}
	if released.CommandID != first.CommandID {
		t.Fatal("release must return the reservation carrying the original command")
	}
	if _, ok := arbiter.Current("chamber-1"); ok {
		t.Fatal("reservation must be gone after owner release")
	}
	if _, err := arbiter.Release("chamber-1", "operator-a"); err == nil {
		t.Fatal("second release must fail once the reservation is withdrawn")
	}
}
