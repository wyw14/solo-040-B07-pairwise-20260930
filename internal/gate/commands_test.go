package gate

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func testCycle(t *testing.T) model.TransitCycle {
	t.Helper()
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Unix(100, 0))
	if err != nil {
		t.Fatalf("new cycle: %v", err)
	}
	return cycle
}

func TestIssueIdempotentKeyReturnsOriginalCommand(t *testing.T) {
	commands := NewCommands()
	cycle := testCycle(t)
	now := time.Unix(100, 0)

	first, replayed, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "operator-a/req-1", now)
	if err != nil || replayed {
		t.Fatalf("first issue: replayed=%v err=%v", replayed, err)
	}
	if first.Epoch != 1 {
		t.Fatalf("first epoch = %d, want 1", first.Epoch)
	}

	second, replayed, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "operator-a/req-1", now.Add(time.Second))
	if err != nil {
		t.Fatalf("retry issue: %v", err)
	}
	if !replayed {
		t.Fatal("retry with the same idempotency key must be flagged as replayed")
	}
	if second.ID != first.ID || second.Epoch != first.Epoch {
		t.Fatal("retry must resolve to the original command, not mint a new epoch")
	}
}

func TestIssueDistinctKeysProduceDistinctCommands(t *testing.T) {
	commands := NewCommands()
	cycle := testCycle(t)

	first, _, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "a", time.Unix(100, 0))
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}
	second, replayed, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "b", time.Unix(100, 0))
	if err != nil || replayed {
		t.Fatalf("second issue: replayed=%v err=%v", replayed, err)
	}
	if first.ID == second.ID || second.Epoch != 2 {
		t.Fatal("different idempotency keys must produce distinct commands with monotonic epochs")
	}
}

func TestIssueSameKeyDifferentActionIsNotReplay(t *testing.T) {
	commands := NewCommands()
	cycle := testCycle(t)
	now := time.Unix(100, 0)

	open, _, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "k", now)
	if err != nil {
		t.Fatalf("open issue: %v", err)
	}
	// Same chamber/cycle/key but a close command must not be mistaken for the
	// open retry: the key is scoped to the command shape.
	closeCmd, replayed, err := commands.Issue(cycle, model.DeviceUpstreamGate, "close", 0, "k", now)
	if err != nil || replayed || closeCmd.ID == open.ID {
		t.Fatalf("different action with same key must issue a new command: replayed=%v err=%v", replayed, err)
	}
}

func TestCancelCommand(t *testing.T) {
	commands := NewCommands()
	cycle := testCycle(t)
	command, _, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "k", time.Unix(100, 0))
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	cancelled, err := commands.Cancel(command.ID, time.Unix(101, 0))
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.State != model.CommandCancelled {
		t.Fatalf("state = %s, want cancelled", cancelled.State)
	}
	if err := cancelled.SetState(model.CommandSettled, time.Unix(102, 0)); err == nil {
		t.Fatal("cancelled command must not accept further transitions")
	}
}
