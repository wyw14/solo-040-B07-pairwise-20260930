package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// gateRequestInput identifies the operator and the client request. Owner is
// the duty officer identity used for reservation ownership; ClientToken is the
// caller-chosen idempotency key so a legitimate retry is recognized instead of
// issuing a second command.
type gateRequestInput struct {
	Owner       string `json:"owner"`
	ClientToken string `json:"client_token"`
}

func decodeGateRequest(r *http.Request) (gateRequestInput, error) {
	var input gateRequestInput
	if r.Body == nil {
		return input, nil
	}
	if err := decodeJSON(r, &input); err != nil {
		if errors.Is(err, io.EOF) {
			return gateRequestInput{}, nil
		}
		return gateRequestInput{}, err
	}
	return input, nil
}

func openGateHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() {
			writeError(w, http.StatusBadRequest, errors.New("invalid gate direction"))
			return
		}
		input, err := decodeGateRequest(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.Owner == "" {
			writeError(w, http.StatusBadRequest, errors.New("owner is required"))
			return
		}
		if input.ClientToken == "" {
			input.ClientToken = "http:" + requestID(r.Context())
		}
		command, err := runtime.Interlocks.RequestOpen(r.Context(), mustCurrent(runtime, chamberID), direction, input.Owner, input.ClientToken)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, command)
	}
}

func mustCurrent(runtime *Runtime, chamberID string) model.TransitCycle {
	cycleValue, _ := runtime.Cycles.Current(chamberID)
	return cycleValue
}

func commandAckHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		commandID, err := uuid.Parse(chi.URLParam(r, "commandID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		command, ok := runtime.GateCommands.Get(commandID)
		if !ok || command.ChamberID != chi.URLParam(r, "chamberID") {
			writeError(w, http.StatusNotFound, errors.New("command not found"))
			return
		}
		ack := model.DeviceAck{
			CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
			Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
			Settled: true, ObservedAt: runtime.Now().UTC(),
		}
		if err := runtime.Cycles.ApplyGateAck(r.Context(), ack); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, ack)
	}
}

func emergencyHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := runtime.Safety.EmergencyStop(r.Context(), chi.URLParam(r, "chamberID"))
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, record)
	}
}
