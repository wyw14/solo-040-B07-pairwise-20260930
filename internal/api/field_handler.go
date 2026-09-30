package api

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

func closeGateHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() {
			writeError(w, http.StatusBadRequest, errors.New("invalid gate direction"))
			return
		}
		cycleValue, ok := runtime.CycleService.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		command, err := runtime.Interlocks.RequestClose(r.Context(), cycleValue, direction)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, command)
	}
}

type positionInput struct {
	Position model.GatePosition `json:"position"`
}

func gateObservationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input positionInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() || !validPosition(input.Position) {
			writeError(w, http.StatusBadRequest, errors.New("invalid direction or position"))
			return
		}
		if err := runtime.Interlocks.Observe(chi.URLParam(r, "chamberID"), direction, input.Position, runtime.Now()); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, input)
	}
}

func validPosition(position model.GatePosition) bool {
	return position == model.GateOpen || position == model.GateOpening ||
		position == model.GateClosed || position == model.GateClosing
}

type releaseReservationInput struct {
	Owner string `json:"owner"`
}

// releaseReservationHandler releases the caller's own reservation. Ownership
// is taken from the authenticated operator (X-Operator-ID) or an explicit body
// field, and the coordinator verifies it against the stored reservation inside
// one atomic check-and-delete — there is no read-then-release race and operator
// B can never release operator A's command.
func releaseReservationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		owner := strings.TrimSpace(r.Header.Get("X-Operator-ID"))
		if owner == "" {
			var input releaseReservationInput
			if err := decodeJSON(r, &input); err != nil && !errors.Is(err, io.EOF) {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			owner = strings.TrimSpace(input.Owner)
		}
		if owner == "" {
			writeError(w, http.StatusBadRequest, errors.New("owner (X-Operator-ID or owner field) is required"))
			return
		}
		reservation, command, err := runtime.Interlocks.Release(r.Context(), chamberID, owner, runtime.Now())
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"reservation": reservation, "command": command})
	}
}

type pendingInput struct {
	Count int `json:"count"`
}

func equipmentPendingHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input pendingInput
		if err := decodeJSON(r, &input); err != nil || input.Count <= 0 || input.Count > 128 {
			writeError(w, http.StatusBadRequest, errors.New("count must be between 1 and 128"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		runtime.Safety.Barrier().Add(chamberID, input.Count)
		writeJSON(w, http.StatusAccepted, map[string]int{"pending": runtime.Safety.Barrier().Pending(chamberID)})
	}
}

func equipmentSettledHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		runtime.Safety.Barrier().Settled(chamberID)
		writeJSON(w, http.StatusOK, map[string]int{"pending": runtime.Safety.Barrier().Pending(chamberID)})
	}
}

type handoffInput struct {
	CycleID    string `json:"cycle_id"`
	Generation uint64 `json:"generation"`
}

func signalHandoffHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input handoffInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		cycleID, err := parseUUID(input.CycleID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		handoff := signal.Handoff{
			ChamberID: chi.URLParam(r, "chamberID"), CycleID: cycleID,
			Generation: input.Generation, ObservedAt: time.Now().UTC(),
		}
		if err := runtime.Signals.ConfirmHandoff(r.Context(), handoff); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, handoff)
	}
}
