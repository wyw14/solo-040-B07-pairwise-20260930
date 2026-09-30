package interlock

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
)

// Coordinator keeps policy evaluation and command reservation in one flow.
type Coordinator struct {
	policy    Policy
	arbiter   *Arbiter
	gates     *gate.Controller
	level     func(string) float64
	emergency func(string) bool
}

func NewCoordinator(policy Policy, arbiter *Arbiter, gates *gate.Controller, level func(string) float64, emergency func(string) bool) *Coordinator {
	return &Coordinator{policy: policy, arbiter: arbiter, gates: gates, level: level, emergency: emergency}
}

func (c *Coordinator) Snapshot(chamberID string) EquipmentSnapshot {
	level := 0.0
	if c.level != nil {
		level = c.level(chamberID)
	}
	emergency := false
	if c.emergency != nil {
		emergency = c.emergency(chamberID)
	}
	return EquipmentSnapshot{
		ChamberID: chamberID, Gates: c.gates.State().Snapshot(chamberID),
		LevelM: level, Emergency: emergency,
	}
}

func (c *Coordinator) RequestOpen(ctx context.Context, cycle model.TransitCycle, direction model.Direction, owner, requestID string) (model.DeviceCommand, error) {
	var command model.DeviceCommand
	_, err := c.arbiter.Reserve(cycle.ChamberID, direction, owner, requestID, func() (uuid.UUID, error) {
		// Policy, reservation and command generation run as one atomic
		// critical section; a replayed request id never reaches this callback.
		if err := c.policy.ValidateOpen(c.Snapshot(cycle.ChamberID), direction); err != nil {
			return uuid.Nil, err
		}
		issued, issueErr := c.gates.Request(ctx, cycle, direction, true, requestID)
		command = issued
		return issued.ID, issueErr
	})
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if command.ID == uuid.Nil {
		// Replayed request id: recover the original command so a legitimate
		// retry receives the same command identity it was first issued.
		if reservation, ok := c.arbiter.Current(cycle.ChamberID); ok && reservation.RequestID == requestID {
			if existing, found := c.gates.Commands().Get(reservation.CommandID); found {
				command = existing
			}
		}
	}
	return command, nil
}

func (c *Coordinator) RequestClose(ctx context.Context, cycle model.TransitCycle, direction model.Direction, requestID string) (model.DeviceCommand, error) {
	if err := c.policy.ValidateClose(c.Snapshot(cycle.ChamberID), direction); err != nil {
		return model.DeviceCommand{}, err
	}
	return c.gates.Request(ctx, cycle, direction, false, requestID)
}

func (c *Coordinator) Release(chamberID, owner, requestID string) error {
	if !c.arbiter.Release(chamberID, owner, requestID) {
		return fmt.Errorf("reservation owner %q / request %q does not control chamber %s", owner, requestID, chamberID)
	}
	return nil
}

func (c *Coordinator) Reservation(chamberID string) (Reservation, bool) {
	return c.arbiter.Current(chamberID)
}

func (c *Coordinator) Observe(chamberID string, direction model.Direction, position model.GatePosition, _ time.Time) error {
	return c.gates.Observe(chamberID, direction, position)
}
