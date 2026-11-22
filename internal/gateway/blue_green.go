package gateway

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrSwapEpochConflict   = errors.New("gateway: blue/green swap epoch conflict (CAS failed)")
	ErrSwapNotFound        = errors.New("gateway: blue/green database mapping not found")
	ErrRollbackUnavailable = errors.New("gateway: blue/green rollback unavailable or retention window expired")
)

// BlueGreenAlias maps a stable client database name (connection string dbname)
// to the currently active tenant ID, retaining the previous tenant for rollback.
type BlueGreenAlias struct {
	DatabaseName     string    `json:"database_name"`
	ActiveTenantID   string    `json:"active_tenant_id"`
	PreviousTenantID string    `json:"previous_tenant_id,omitempty"`
	Epoch            int64     `json:"epoch"`
	SwappedAt        time.Time `json:"swapped_at"`
	RetentionUntil   time.Time `json:"retention_until,omitempty"`
}

func (d *TenantDiscovery) resolveAliasLocked(dbOrTenant string) string {
	if d.aliases == nil {
		return dbOrTenant
	}
	if a, ok := d.aliases[dbOrTenant]; ok && a.ActiveTenantID != "" {
		return a.ActiveTenantID
	}
	return dbOrTenant
}

// GetDatabaseAlias returns the current blue/green mapping for dbName.
func (d *TenantDiscovery) GetDatabaseAlias(dbName string) (BlueGreenAlias, bool) {
	d.cacheMu.RLock()
	defer d.cacheMu.RUnlock()
	if d.aliases == nil {
		return BlueGreenAlias{}, false
	}
	a, ok := d.aliases[dbName]
	return a, ok
}

// SwapDatabaseTenant atomically switches dbName -> newTenantID using epoch CAS fencing.
// Two concurrent swaps with the same expectedEpoch cannot both win.
func (d *TenantDiscovery) SwapDatabaseTenant(dbName, newTenantID string, expectedEpoch int64, retention time.Duration) (BlueGreenAlias, error) {
	if dbName == "" || newTenantID == "" {
		return BlueGreenAlias{}, fmt.Errorf("gateway: database_name and new_tenant_id are required")
	}
	if retention <= 0 {
		retention = time.Hour
	}

	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if d.aliases == nil {
		d.aliases = make(map[string]BlueGreenAlias)
	}
	cur, exists := d.aliases[dbName]
	if !exists {
		cur = BlueGreenAlias{
			DatabaseName:   dbName,
			ActiveTenantID: dbName,
			Epoch:          0,
		}
	}
	if cur.Epoch != expectedEpoch {
		return BlueGreenAlias{}, fmt.Errorf("%w: db=%s have_epoch=%d want_epoch=%d", ErrSwapEpochConflict, dbName, cur.Epoch, expectedEpoch)
	}
	now := time.Now().UTC()
	next := BlueGreenAlias{
		DatabaseName:     dbName,
		ActiveTenantID:   newTenantID,
		PreviousTenantID: cur.ActiveTenantID,
		Epoch:            cur.Epoch + 1,
		SwappedAt:        now,
		RetentionUntil:   now.Add(retention),
	}
	d.aliases[dbName] = next
	return next, nil
}

// RollbackDatabaseTenant atomically reverts dbName back to PreviousTenantID within the retention window.
func (d *TenantDiscovery) RollbackDatabaseTenant(dbName string, expectedEpoch int64) (BlueGreenAlias, error) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if d.aliases == nil {
		return BlueGreenAlias{}, ErrSwapNotFound
	}
	cur, exists := d.aliases[dbName]
	if !exists {
		return BlueGreenAlias{}, ErrSwapNotFound
	}
	if cur.Epoch != expectedEpoch {
		return BlueGreenAlias{}, fmt.Errorf("%w: db=%s have_epoch=%d want_epoch=%d", ErrSwapEpochConflict, dbName, cur.Epoch, expectedEpoch)
	}
	if cur.PreviousTenantID == "" {
		return BlueGreenAlias{}, ErrRollbackUnavailable
	}
	now := time.Now().UTC()
	if !cur.RetentionUntil.IsZero() && now.After(cur.RetentionUntil) {
		return BlueGreenAlias{}, fmt.Errorf("%w: expired at %s", ErrRollbackUnavailable, cur.RetentionUntil.Format(time.RFC3339))
	}
	next := BlueGreenAlias{
		DatabaseName:     dbName,
		ActiveTenantID:   cur.PreviousTenantID,
		PreviousTenantID: cur.ActiveTenantID,
		Epoch:            cur.Epoch + 1,
		SwappedAt:        now,
		RetentionUntil:   cur.RetentionUntil,
	}
	d.aliases[dbName] = next
	return next, nil
}
