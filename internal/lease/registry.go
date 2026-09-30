package lease

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Registry owns monotonically increasing fencing tokens per vessel.
type Registry struct {
	mu        sync.RWMutex
	leases    map[uuid.UUID]model.Lease
	nextToken map[uuid.UUID]uint64
}

// ErrLeaseInUse is returned when an active lease is held by another owner.
var ErrLeaseInUse = errors.New("vessel lease is held by another owner and still active")

func NewRegistry() *Registry {
	return &Registry{
		leases:    make(map[uuid.UUID]model.Lease),
		nextToken: make(map[uuid.UUID]uint64),
	}
}

// Assign grants a lease for the vessel. A duplicate request from the same owner
// while its lease is active returns that lease unchanged (same fencing token,
// same expiry) so retries are recognizable. A request from another owner while
// an active lease exists is rejected: assignments never overwrite an in-flight
// owner. Only after release or expiry is a new token minted.
// The boolean reports whether a new lease was created.
func (r *Registry) Assign(vesselID uuid.UUID, owner string, ttl time.Duration, now time.Time) (model.Lease, bool, error) {
	if vesselID == uuid.Nil || owner == "" || ttl <= 0 {
		return model.Lease{}, false, errors.New("assignment requires vessel, owner and positive ttl")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now = now.UTC()
	if current, ok := r.leases[vesselID]; ok && current.Active(now) {
		if current.Owner == owner {
			return current, false, nil
		}
		return model.Lease{}, false, ErrLeaseInUse
	}
	r.nextToken[vesselID]++
	lease := model.Lease{
		VesselID: vesselID, Owner: owner, FencingToken: r.nextToken[vesselID],
		ExpiresAt: now.Add(ttl),
	}
	r.leases[vesselID] = lease
	return lease, true, nil
}

func (r *Registry) Renew(vesselID uuid.UUID, owner string, token uint64, ttl time.Duration, now time.Time) (model.Lease, error) {
	if ttl <= 0 {
		return model.Lease{}, errors.New("renewal ttl must be positive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok {
		return model.Lease{}, fmt.Errorf("no lease for vessel %s", vesselID)
	}
	if lease.Owner != owner || lease.FencingToken != token {
		return model.Lease{}, errors.New("lease owner or fencing token is stale")
	}
	if !lease.Active(now) {
		return model.Lease{}, errors.New("lease has expired")
	}
	lease.ExpiresAt = now.UTC().Add(ttl)
	r.leases[vesselID] = lease
	return lease, nil
}

func (r *Registry) Get(vesselID uuid.UUID) (model.Lease, bool) {
	r.mu.RLock()
	lease, ok := r.leases[vesselID]
	r.mu.RUnlock()
	return lease, ok
}

func (r *Registry) Release(vesselID uuid.UUID, owner string, token uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok || lease.Owner != owner || lease.FencingToken != token {
		return false
	}
	delete(r.leases, vesselID)
	return true
}

func (r *Registry) Snapshot() []model.Lease {
	r.mu.RLock()
	result := make([]model.Lease, 0, len(r.leases))
	for _, lease := range r.leases {
		result = append(result, lease)
	}
	r.mu.RUnlock()
	return result
}
