package interlock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func newTestCoordinator(now func() time.Time) *Coordinator {
	state := gate.NewState("chamber-1")
	commands := gate.NewCommands()
	recorder := journal.NewRecorder(journal.NewMemoryStore(), now)
	gates := gate.NewController(state, commands, recorder, now)
	return NewCoordinator(DefaultPolicy(), NewArbiter(), gates,
		func(string) float64 { return 0 }, func(string) bool { return false })
}

func testCycle() model.TransitCycle {
	cycle, _ := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Unix(1, 0).UTC())
	return cycle
}

func TestRequestOpenDuplicateRequestReturnsSameCommand(t *testing.T) {
	now := func() time.Time { return time.Unix(100, 0).UTC() }
	coordinator := newTestCoordinator(now)
	cycle := testCycle()
	ctx := context.Background()

	first, err := coordinator.RequestOpen(ctx, cycle, model.DirectionUpstream, "officer-A", "client-1")
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	second, err := coordinator.RequestOpen(ctx, cycle, model.DirectionUpstream, "officer-A", "client-1")
	if err != nil {
		t.Fatalf("legitimate retry: %v", err)
	}
	if first.ID != second.ID || first.Epoch != second.Epoch {
		t.Fatalf("retry produced a different command: %s epoch %d vs %s epoch %d",
			first.ID, first.Epoch, second.ID, second.Epoch)
	}
	if pending := coordinator.gates.Commands().Pending("chamber-1"); len(pending) != 1 {
		t.Fatalf("expected one pending command, got %d", len(pending))
	}

	reservation, ok := coordinator.Reservation("chamber-1")
	if !ok {
		t.Fatal("reservation missing")
	}
	if reservation.Owner != "officer-A" || reservation.CommandID != first.ID {
		t.Fatalf("reservation mismatch: %+v", reservation)
	}
}

func TestRequestOpenSecondOfficerRejectedAndCannotRelease(t *testing.T) {
	now := func() time.Time { return time.Unix(100, 0).UTC() }
	coordinator := newTestCoordinator(now)
	cycle := testCycle()
	ctx := context.Background()

	first, err := coordinator.RequestOpen(ctx, cycle, model.DirectionUpstream, "officer-A", "client-A")
	if err != nil {
		t.Fatalf("officer A open: %v", err)
	}
	if _, err := coordinator.RequestOpen(ctx, cycle, model.DirectionUpstream, "officer-B", "client-B"); err == nil {
		t.Fatal("officer B overwrote A's in-flight reservation")
	}

	reservation, _ := coordinator.Reservation("chamber-1")
	if reservation.Owner != "officer-A" {
		t.Fatalf("owner became %q", reservation.Owner)
	}
	if pending := coordinator.gates.Commands().Pending("chamber-1"); len(pending) != 1 || pending[0].ID != first.ID {
		t.Fatalf("B's request must not create a command: %+v", pending)
	}

	if err := coordinator.Release("chamber-1", "officer-B", "client-B"); err == nil {
		t.Fatal("officer B released officer A's reservation")
	}
	if _, ok := coordinator.Reservation("chamber-1"); !ok {
		t.Fatal("A's reservation was removed by B")
	}
	if err := coordinator.Release("chamber-1", "officer-A", ""); err != nil {
		t.Fatalf("officer A release: %v", err)
	}
}

func TestConcurrentOpenRequestsSingleCommand(t *testing.T) {
	now := func() time.Time { return time.Unix(100, 0).UTC() }
	coordinator := newTestCoordinator(now)
	cycle := testCycle()
	ctx := context.Background()

	const n = 24
	var wg sync.WaitGroup
	type result struct {
		cmd model.DeviceCommand
		err error
	}
	results := make(chan result, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			cmd, err := coordinator.RequestOpen(ctx, cycle, model.DirectionUpstream,
				"officer", uuid.NewString())
			results <- result{cmd, err}
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	var winnerID uuid.UUID
	for r := range results {
		if r.err != nil {
			var inFlight *InFlightError
			if !errors.As(r.err, &inFlight) {
				t.Fatalf("unexpected error: %v", r.err)
			}
			continue
		}
		successes++
		winnerID = r.cmd.ID
	}
	if successes != 1 {
		t.Fatalf("expected 1 successful request, got %d", successes)
	}
	pending := coordinator.gates.Commands().Pending("chamber-1")
	if len(pending) != 1 || pending[0].ID != winnerID {
		t.Fatalf("expected exactly one command (%s), got %+v", winnerID, pending)
	}
}
