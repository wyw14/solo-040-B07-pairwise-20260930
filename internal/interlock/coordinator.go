package interlock

import (
	"context"
	"fmt"
	"time"

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

// RequestOpen reserves the direction and generates the gate command as one
// atomic flow. A repeat call from the same owner while its reservation is
// current is a legitimate retry and short-circuits to the original command
// with replayed=true — safety validation and command generation only run on
// the first application, so a changed snapshot can never hide the in-flight
// request from a retrying operator.
func (c *Coordinator) RequestOpen(ctx context.Context, cycle model.TransitCycle, direction model.Direction, owner, requestKey string) (command model.DeviceCommand, replayed bool, err error) {
	reservation, replayed, err := c.arbiter.Reserve(cycle.ChamberID, direction, owner, func() (model.DeviceCommand, error) {
		if err := c.policy.ValidateOpen(c.Snapshot(cycle.ChamberID), direction); err != nil {
			return model.DeviceCommand{}, err
		}
		issued, _, issueErr := c.gates.Request(ctx, cycle, direction, true, requestKey)
		return issued, issueErr
	})
	if err != nil {
		return model.DeviceCommand{}, false, err
	}
	command, ok := c.gates.Commands().Get(reservation.CommandID)
	if !ok {
		return model.DeviceCommand{}, false, fmt.Errorf("command %s missing for reservation", reservation.CommandID)
	}
	return command, replayed, nil
}

func (c *Coordinator) RequestClose(ctx context.Context, cycle model.TransitCycle, direction model.Direction) (model.DeviceCommand, error) {
	if err := c.policy.ValidateClose(c.Snapshot(cycle.ChamberID), direction); err != nil {
		return model.DeviceCommand{}, err
	}
	command, _, err := c.gates.Request(ctx, cycle, direction, false, "")
	return command, err
}

// Release verifies the caller owns the reservation, withdraws it atomically
// and cancels the command generated for it so no in-flight operation outlives
// its reservation.
func (c *Coordinator) Release(ctx context.Context, chamberID, owner string, now time.Time) (Reservation, model.DeviceCommand, error) {
	reservation, err := c.arbiter.Release(chamberID, owner)
	if err != nil {
		return Reservation{}, model.DeviceCommand{}, err
	}
	command, err := c.gates.Commands().Cancel(reservation.CommandID, now)
	if err != nil {
		return Reservation{}, model.DeviceCommand{}, err
	}
	return reservation, command, nil
}

func (c *Coordinator) Reservation(chamberID string) (Reservation, bool) {
	return c.arbiter.Current(chamberID)
}

func (c *Coordinator) Observe(chamberID string, direction model.Direction, position model.GatePosition, _ time.Time) error {
	return c.gates.Observe(chamberID, direction, position)
}
