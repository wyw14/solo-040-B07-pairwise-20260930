package interlock

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type Reservation struct {
	ChamberID string          `json:"chamber_id"`
	Direction model.Direction `json:"direction"`
	Owner     string          `json:"owner"`
	RequestID string          `json:"request_id"`
	CommandID uuid.UUID       `json:"command_id"`
}

var (
	// ErrOpposedDirection is returned when the other gate direction is reserved.
	ErrOpposedDirection = errors.New("opposed gate direction is already reserved")
	// ErrRequestMismatch is returned when a request id is replayed with different
	// owner/direction parameters than the original request.
	ErrRequestMismatch = errors.New("request id is bound to a different owner or direction")
)

// InFlightError reports that an operation is already reserved for the chamber.
// The embedded reservation identifies the in-flight request so callers never
// have to guess which command they are racing with.
type InFlightError struct {
	Existing Reservation
}

func (e *InFlightError) Error() string {
	return fmt.Sprintf("gate operation already in flight for chamber %s (owner=%s request=%s)",
		e.Existing.ChamberID, e.Existing.Owner, e.Existing.RequestID)
}

// Arbiter atomically checks and reserves a chamber direction. Reservation,
// owner verification and command generation happen under one lock, so a
// duplicate or concurrent request can never overwrite an in-flight operation.
type Arbiter struct {
	mu     sync.Mutex
	active map[string]Reservation
}

func NewArbiter() *Arbiter { return &Arbiter{active: make(map[string]Reservation)} }

// Reserve reserves the chamber direction for owner and runs commit exactly
// once per request id. Replaying the same chamber/request id returns the
// original reservation (and its command id) without invoking commit again.
// A different request id while a reservation is in flight is rejected,
// whether or not the direction or owner matches.
func (a *Arbiter) Reserve(chamberID string, direction model.Direction, owner, requestID string, commit func() (uuid.UUID, error)) (Reservation, error) {
	if chamberID == "" || !direction.Valid() || owner == "" || requestID == "" || commit == nil {
		return Reservation{}, errors.New("reservation requires chamber, direction, owner, request id and commit")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.active[chamberID]; ok {
		if current.RequestID == requestID {
			if current.Owner != owner || current.Direction != direction {
				return Reservation{}, ErrRequestMismatch
			}
			return current, nil
		}
		if current.Direction != direction {
			return Reservation{}, ErrOpposedDirection
		}
		return Reservation{}, &InFlightError{Existing: current}
	}
	commandID, err := commit()
	if err != nil {
		return Reservation{}, err
	}
	reservation := Reservation{
		ChamberID: chamberID, Direction: direction, Owner: owner,
		RequestID: requestID, CommandID: commandID,
	}
	a.active[chamberID] = reservation
	return reservation, nil
}

// Release removes the reservation only when owner (and, when given, requestID)
// matches the holder. A non-empty requestID that does not match fails the
// release even if the owner does.
func (a *Arbiter) Release(chamberID, owner, requestID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, ok := a.active[chamberID]
	if !ok || current.Owner != owner {
		return false
	}
	if requestID != "" && current.RequestID != requestID {
		return false
	}
	delete(a.active, chamberID)
	return true
}

func (a *Arbiter) Current(chamberID string) (Reservation, bool) {
	a.mu.Lock()
	reservation, ok := a.active[chamberID]
	a.mu.Unlock()
	return reservation, ok
}
