package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func chiURLParam(r *http.Request, name string) string { return chi.URLParam(r, name) }

// operatorID identifies the duty operator across requests so that reservation
// ownership survives a request/response cycle. Callers send X-Operator-ID; an
// anonymous fallback scoped to the request keeps unauthenticated probes working
// but can never release a reservation opened by a named operator.
func operatorID(r *http.Request) string {
	if operator := strings.TrimSpace(r.Header.Get("X-Operator-ID")); operator != "" {
		return operator
	}
	return "http:" + requestID(r.Context())
}

// idempotencyKey lets a client (operator console) tag a logical request so a
// retry after a lost response resolves to the original command.
func idempotencyKey(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("Idempotency-Key"))
}

func parseUUID(value string) (uuid.UUID, error) {
	if value == "" {
		return uuid.Nil, errors.New("uuid value is required")
	}
	return uuid.Parse(value)
}

func durationSeconds(value int) time.Duration {
	if value <= 0 {
		value = 30
	}
	if value > 3600 {
		value = 3600
	}
	return time.Duration(value) * time.Second
}
