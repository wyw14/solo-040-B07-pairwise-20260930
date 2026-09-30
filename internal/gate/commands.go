package gate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Commands owns command identity, lifecycle and late-ack classification.
type Commands struct {
	mu        sync.RWMutex
	byID      map[uuid.UUID]model.DeviceCommand
	byChamber map[string][]uuid.UUID
	byRequest map[string]uuid.UUID
	nextEpoch map[string]uint64
}

func NewCommands() *Commands {
	return &Commands{
		byID:      make(map[uuid.UUID]model.DeviceCommand),
		byChamber: make(map[string][]uuid.UUID),
		byRequest: make(map[string]uuid.UUID),
		nextEpoch: make(map[string]uint64),
	}
}

func epochKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

func requestKey(chamberID string, device model.DeviceKind, requestID string) string {
	return epochKey(chamberID, device) + ":" + requestID
}

// Issue generates a command for the request. When requestID is non-empty and a
// command was already issued for the same chamber/device/request, that original
// command is returned unchanged: retries never consume a new epoch or produce a
// new identity.
func (c *Commands) Issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, requestID string, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if requestID != "" {
		if existingID, ok := c.byRequest[requestKey(cycle.ChamberID, device, requestID)]; ok {
			existing := c.byID[existingID]
			if existing.CycleID != cycle.ID || existing.Generation != cycle.Generation ||
				existing.Action != action {
				return model.DeviceCommand{}, errors.New("request id is already bound to a different command")
			}
			return existing, nil
		}
	}
	key := epochKey(cycle.ChamberID, device)
	c.nextEpoch[key]++
	command, err := model.NewDeviceCommand(cycle, c.nextEpoch[key], device, action, target, requestID, now)
	if err != nil {
		c.nextEpoch[key]--
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	c.byChamber[cycle.ChamberID] = append(c.byChamber[cycle.ChamberID], command.ID)
	if requestID != "" {
		c.byRequest[requestKey(cycle.ChamberID, device, requestID)] = command.ID
	}
	return command, nil
}

func (c *Commands) Accept(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandAccepted, now)
}

func (c *Commands) Execute(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandExecuting, now)
}

func (c *Commands) transition(commandID uuid.UUID, state model.CommandState, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.SetState(state, now); err != nil {
		return err
	}
	c.byID[commandID] = command
	return nil
}

func (c *Commands) Settle(ack model.DeviceAck, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[ack.CommandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	if !ack.Matches(command) {
		return model.DeviceCommand{}, errors.New("ack identity does not match command generation")
	}
	if !ack.Settled {
		return model.DeviceCommand{}, errors.New("device has not settled")
	}
	if err := command.SetState(model.CommandSettled, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	return command, nil
}

func (c *Commands) CancelChamber(chamberID string, now time.Time) []model.DeviceCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cancelled []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.State.Terminal() {
			continue
		}
		if command.SetState(model.CommandCancelled, now) == nil {
			c.byID[commandID] = command
			cancelled = append(cancelled, command)
		}
	}
	return cancelled
}

func (c *Commands) Pending(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var pending []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if !command.State.Terminal() {
			pending = append(pending, command)
		}
	}
	return pending
}

func (c *Commands) Get(commandID uuid.UUID) (model.DeviceCommand, bool) {
	c.mu.RLock()
	command, ok := c.byID[commandID]
	c.mu.RUnlock()
	return command, ok
}
