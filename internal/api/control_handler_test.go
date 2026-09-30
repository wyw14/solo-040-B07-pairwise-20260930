package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/wyw14/cry-96/internal/model"
)

func newTestRouter(t *testing.T) (*Runtime, http.Handler) {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	vessel, err := model.NewVessel("测试船", "TS-1", 100)
	if err != nil {
		t.Fatalf("new vessel: %v", err)
	}
	plan, err := model.NewConvoyPlan("chamber-1", model.DirectionUpstream, []model.Vessel{vessel}, runtime.Now())
	if err != nil {
		t.Fatalf("new plan: %v", err)
	}
	if _, err := runtime.Cycles.Start(context.Background(), plan); err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	return runtime, Router(runtime, "")
}

func openGate(t *testing.T, router http.Handler, operator, idempotencyKey string) (int, model.DeviceCommand) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/chambers/chamber-1/gates/upstream/open", nil)
	if operator != "" {
		request.Header.Set("X-Operator-ID", operator)
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	var command model.DeviceCommand
	if response.Code == http.StatusAccepted || response.Code == http.StatusOK {
		if err := json.Unmarshal(response.Body.Bytes(), &command); err != nil {
			t.Fatalf("decode command: %v body=%s", err, response.Body.String())
		}
	}
	return response.Code, command
}

func releaseReservation(t *testing.T, router http.Handler, operator string, body []byte) int {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/chambers/chamber-1/reservation/release", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if operator != "" {
		request.Header.Set("X-Operator-ID", operator)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response.Code
}

func pendingCount(t *testing.T, router http.Handler) int {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/api/interlocks", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode interlocks: %v", err)
	}
	var chamber struct {
		PendingCommands []model.DeviceCommand `json:"pending_commands"`
	}
	if err := json.Unmarshal(payload["chamber-1"], &chamber); err != nil {
		t.Fatalf("decode chamber: %v", err)
	}
	return len(chamber.PendingCommands)
}

// Full replay of the reported incident.
func TestGateOpenReservationOwnershipIncident(t *testing.T) {
	_, router := newTestRouter(t)

	// Operator A applies and holds the reservation.
	statusA, commandA := openGate(t, router, "operator-a", "")
	if statusA != http.StatusAccepted {
		t.Fatalf("A first open status = %d, want 202", statusA)
	}

	// A's legitimate retry (e.g. lost response) recognizes the original
	// request instead of generating a second command.
	statusRetry, commandRetry := openGate(t, router, "operator-a", "")
	if statusRetry != http.StatusOK || commandRetry.ID != commandA.ID {
		t.Fatalf("A retry status=%d id-match=%v, want 200 and the original command", statusRetry, commandRetry.ID == commandA.ID)
	}
	if pendingCount(t, router) != 1 {
		t.Fatal("retry must not create a second in-flight command")
	}

	// Operator B applying in the same direction must not steal the reservation.
	statusB, _ := openGate(t, router, "operator-b", "")
	if statusB != http.StatusConflict {
		t.Fatalf("B open status = %d, want 409", statusB)
	}
	if pendingCount(t, router) != 1 {
		t.Fatal("rejected B application must not create a command")
	}

	// B cannot release A's command; anonymous callers are rejected outright.
	if status := releaseReservation(t, router, "operator-b", nil); status != http.StatusConflict {
		t.Fatalf("B release status = %d, want 409", status)
	}
	if status := releaseReservation(t, router, "", nil); status != http.StatusBadRequest {
		t.Fatalf("anonymous release status = %d, want 400", status)
	}

	// A still owns the reservation and can release its own command.
	if status := releaseReservation(t, router, "operator-a", nil); status != http.StatusOK {
		t.Fatalf("A release status = %d, want 200", status)
	}
	// Release is one-shot: a second release cannot touch state again.
	if status := releaseReservation(t, router, "operator-a", nil); status != http.StatusConflict {
		t.Fatalf("A second release status = %d, want 409", status)
	}
}

func TestGateOpenIdempotencyKeySurvivesRetries(t *testing.T) {
	_, router := newTestRouter(t)

	status1, command1 := openGate(t, router, "operator-a", "convoy-7/open-upstream")
	status2, command2 := openGate(t, router, "operator-a", "convoy-7/open-upstream")
	if status1 != http.StatusAccepted || status2 != http.StatusOK {
		t.Fatalf("statuses = %d/%d, want 202/200", status1, status2)
	}
	if command1.ID != command2.ID || command1.Epoch != command2.Epoch {
		t.Fatal("same idempotency key must resolve to the original command and epoch")
	}
	if pendingCount(t, router) != 1 {
		t.Fatal("idempotent retries must not stack in-flight commands")
	}
}

// Even when the equipment snapshot would no longer admit a fresh open request
// (the opposite gate is observed opening), the original owner's retry must
// still resolve to the in-flight command rather than 409.
func TestRetryRecognizedAfterSnapshotBecomesInvalid(t *testing.T) {
	runtime, router := newTestRouter(t)

	status, command := openGate(t, router, "operator-a", "")
	if status != http.StatusAccepted {
		t.Fatalf("first open status = %d, want 202", status)
	}
	// Mutate the world so a *new* open would fail policy: the opposite gate is
	// no longer closed.
	if err := runtime.Gates.Observe("chamber-1", model.DirectionDownstream, model.GateOpening); err != nil {
		t.Fatalf("observe opposite gate: %v", err)
	}
	statusRetry, retried := openGate(t, router, "operator-a", "")
	if statusRetry != http.StatusOK || retried.ID != command.ID {
		t.Fatalf("retry under invalid snapshot: status=%d same-command=%v, want 200 and original command",
			statusRetry, retried.ID == command.ID)
	}
	// A different operator is still rejected and never steals the reservation.
	if statusB, _ := openGate(t, router, "operator-b", ""); statusB != http.StatusConflict {
		t.Fatalf("other operator status = %d, want 409", statusB)
	}
}

func TestConcurrentOpenRequestsDoNotOverwriteReservation(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		_, router := newTestRouter(t)
		var wg sync.WaitGroup
		start := make(chan struct{})
		statuses := make([]int, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				// Eight distinct operators racing for the same gate: only one wins.
				statuses[i], _ = openGate(t, router, "operator-"+string(rune('a'+i)), "")
			}(i)
		}
		close(start)
		wg.Wait()

		accepted, rejected, winner := 0, 0, ""
		for i, status := range statuses {
			switch status {
			case http.StatusAccepted:
				accepted++
				winner = "operator-" + string(rune('a'+i))
			case http.StatusConflict:
				rejected++
			default:
				t.Fatalf("attempt %d goroutine %d: unexpected status %d", attempt, i, status)
			}
		}
		if accepted != 1 || rejected != 7 {
			t.Fatalf("attempt %d: accepted=%d rejected=%d, want exactly 1 winner", attempt, accepted, rejected)
		}
		if pendingCount(t, router) != 1 {
			t.Fatalf("attempt %d: exactly one command must be in flight", attempt)
		}
		// A different operator must not be able to release the winner's reservation.
		loser := "operator-b"
		if winner == "operator-b" {
			loser = "operator-a"
		}
		if status := releaseReservation(t, router, loser, nil); status != http.StatusConflict {
			t.Fatalf("attempt %d: loser release status = %d, want 409", attempt, status)
		}
	}
}
