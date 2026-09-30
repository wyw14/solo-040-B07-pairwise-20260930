package lease

import (
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type ExpiredLease struct {
	Lease     model.Lease `json:"lease"`
	ExpiredAt time.Time   `json:"expired_at"`
}

// Sweeper discovers expired leases without changing the fencing sequence.
type Sweeper struct {
	registry *Registry
	now      func() time.Time
}

func NewSweeper(registry *Registry, now func() time.Time) *Sweeper {
	if now == nil {
		now = time.Now
	}
	return &Sweeper{registry: registry, now: now}
}

func (s *Sweeper) Expired() []ExpiredLease {
	now := s.now().UTC()
	var result []ExpiredLease
	for _, lease := range s.registry.Snapshot() {
		if !lease.Active(now) {
			result = append(result, ExpiredLease{Lease: lease, ExpiredAt: now})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Lease.VesselID.String() < result[j].Lease.VesselID.String()
	})
	return result
}

func (s *Sweeper) Reassign(vesselID uuid.UUID, previousOwner, newOwner string, ttl time.Duration) (model.Lease, error) {
	now := s.now()
	current, ok := s.registry.Get(vesselID)
	if ok && current.Owner == previousOwner && current.Active(now) {
		return model.Lease{}, ErrLeaseStillActive
	}
	lease, _, err := s.registry.Assign(vesselID, newOwner, ttl, now)
	return lease, err
}
