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
	mu          sync.Mutex
	byID        map[uuid.UUID]model.DeviceCommand
	byChamber   map[string][]uuid.UUID
	nextEpoch   map[string]uint64
	byClientKey map[string]uuid.UUID
}

func NewCommands() *Commands {
	return &Commands{
		byID:        make(map[uuid.UUID]model.DeviceCommand),
		byChamber:   make(map[string][]uuid.UUID),
		nextEpoch:   make(map[string]uint64),
		byClientKey: make(map[string]uuid.UUID),
	}
}

func epochKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

// clientKey scopes an operator-supplied idempotency key to the exact command
// shape it was first issued with, so a retried request can never resolve to a
// command generated for a different device or action.
func clientKey(cycle model.TransitCycle, device model.DeviceKind, action, requestKey string) string {
	return cycle.ChamberID + "|" + cycle.ID.String() + "|" + string(device) + "|" + action + "|" + requestKey
}

// Issue generates a command for a cycle. When requestKey is non-empty, a
// repeated issue with the same cycle/device/action/key returns the original
// command without consuming a new epoch, so legitimate retries and duplicate
// submissions are recognized as the same in-flight operation.
func (c *Commands) Issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, requestKey string, now time.Time) (model.DeviceCommand, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if requestKey != "" {
		key := clientKey(cycle, device, action, requestKey)
		if commandID, ok := c.byClientKey[key]; ok {
			return c.byID[commandID], true, nil
		}
	}
	epochStoreKey := epochKey(cycle.ChamberID, device)
	c.nextEpoch[epochStoreKey]++
	command, err := model.NewDeviceCommand(cycle, c.nextEpoch[epochStoreKey], device, action, target, now)
	if err != nil {
		return model.DeviceCommand{}, false, err
	}
	c.byID[command.ID] = command
	c.byChamber[cycle.ChamberID] = append(c.byChamber[cycle.ChamberID], command.ID)
	if requestKey != "" {
		c.byClientKey[clientKey(cycle, device, action, requestKey)] = command.ID
	}
	return command, false, nil
}

// Cancel marks a single non-terminal command cancelled. It is a no-op-without-
// error when the command is already settled so that releasing a reservation
// whose command completed in the meantime stays idempotent.
func (c *Commands) Cancel(commandID uuid.UUID, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if command.State == model.CommandSettled {
		return command, nil
	}
	if err := command.SetState(model.CommandCancelled, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[commandID] = command
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
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
	command, ok := c.byID[commandID]
	c.mu.Unlock()
	return command, ok
}
