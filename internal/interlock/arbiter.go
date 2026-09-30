package interlock

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

var (
	ErrOpposedReservation = errors.New("opposed gate direction is already reserved")
	ErrReservationHeld    = errors.New("gate direction is already reserved by another operator")
)

type Reservation struct {
	ChamberID string          `json:"chamber_id"`
	Direction model.Direction `json:"direction"`
	Owner     string          `json:"owner"`
	CommandID uuid.UUID       `json:"command_id"`
}

// Arbiter atomically checks and reserves a chamber direction. The mutex
// covers the full check-commit-store sequence, so duplicate or concurrent
// applications can never overwrite an in-flight reservation or generate a
// second command for it.
type Arbiter struct {
	mu     sync.Mutex
	active map[string]Reservation
}

func NewArbiter() *Arbiter { return &Arbiter{active: make(map[string]Reservation)} }

// Reserve checks, commits and stores a reservation in one critical section.
// commit runs while the arbiter lock is held and must return the command that
// represents this operation; if it fails, no reservation is stored. A repeat
// application from the same owner for the same direction is treated as a
// legitimate retry: the existing reservation (and its command) is returned
// without invoking commit again.
func (a *Arbiter) Reserve(chamberID string, direction model.Direction, owner string, commit func() (model.DeviceCommand, error)) (Reservation, bool, error) {
	if chamberID == "" || !direction.Valid() || owner == "" || commit == nil {
		return Reservation{}, false, errors.New("reservation requires chamber, direction, owner and commit")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.active[chamberID]; ok {
		if current.Owner == owner && current.Direction == direction {
			return current, true, nil
		}
		if current.Direction != direction {
			return Reservation{}, false, ErrOpposedReservation
		}
		return Reservation{}, false, ErrReservationHeld
	}
	command, err := commit()
	if err != nil {
		return Reservation{}, false, err
	}
	reservation := Reservation{
		ChamberID: chamberID, Direction: direction, Owner: owner, CommandID: command.ID,
	}
	a.active[chamberID] = reservation
	return reservation, false, nil
}

// Release atomically verifies ownership and withdraws the reservation,
// returning it so the caller can retire the associated command. There is no
// separate read-then-release window for another request to race through.
func (a *Arbiter) Release(chamberID, owner string) (Reservation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, ok := a.active[chamberID]
	if !ok {
		return Reservation{}, errors.New("no active reservation")
	}
	if current.Owner != owner {
		return Reservation{}, errors.New("reservation is held by another operator")
	}
	delete(a.active, chamberID)
	return current, nil
}

func (a *Arbiter) Current(chamberID string) (Reservation, bool) {
	a.mu.Lock()
	reservation, ok := a.active[chamberID]
	a.mu.Unlock()
	return reservation, ok
}
