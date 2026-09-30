package gate

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func testCommandCycle() model.TransitCycle {
	cycle, _ := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Unix(1, 0).UTC())
	return cycle
}

func TestIssueIdempotentByRequestID(t *testing.T) {
	commands := NewCommands()
	cycle := testCommandCycle()
	now := time.Unix(10, 0).UTC()

	first, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "client-1", now)
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}
	second, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "client-1", now.Add(time.Second))
	if err != nil {
		t.Fatalf("retry issue: %v", err)
	}
	if first.ID != second.ID || first.Epoch != second.Epoch {
		t.Fatalf("retry minted a new command: %s/%d vs %s/%d",
			first.ID, first.Epoch, second.ID, second.Epoch)
	}
	if len(commands.Pending("chamber-1")) != 1 {
		t.Fatal("retry appended a duplicate pending command")
	}
}

func TestIssueDistinctRequestIDsAdvanceEpoch(t *testing.T) {
	commands := NewCommands()
	cycle := testCommandCycle()
	now := time.Unix(10, 0).UTC()

	one, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "client-1", now)
	if err != nil {
		t.Fatalf("issue one: %v", err)
	}
	two, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "client-2", now)
	if err != nil {
		t.Fatalf("issue two: %v", err)
	}
	if one.ID == two.ID || two.Epoch != one.Epoch+1 {
		t.Fatalf("distinct requests did not advance: epochs %d %d", one.Epoch, two.Epoch)
	}
}

func TestIssueReplayRejectsConflictingAction(t *testing.T) {
	commands := NewCommands()
	cycle := testCommandCycle()
	now := time.Unix(10, 0).UTC()

	if _, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, "client-1", now); err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := commands.Issue(cycle, model.DeviceUpstreamGate, "close", 0, "client-1", now); err == nil {
		t.Fatal("request id reused for a different action was accepted")
	}
}
