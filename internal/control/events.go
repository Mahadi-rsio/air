package control

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
)

const (
	tenantEventUpsert = "UPSERT"
	tenantEventDelete = "DELETE"

	maxEventHistory    = 1024
	maxStreamBacklog   = 1000
	subscriberBufSize  = 256
)

// TenantEvent represents a committed tenant routing/lifecycle change.
type TenantEvent struct {
	Version         int64
	Type            pb.TenantEventType
	TenantID        string
	State           string
	Host            string
	DirectPort      int32
	FencingEpoch    int64
	EmittedAtUnixMs int64
}

type tenantEventBroker struct {
	mu             sync.Mutex
	nextMemVersion int64
	nextSubID      uint64
	history        []TenantEvent
	subs           map[uint64]chan TenantEvent
}

func (b *tenantEventBroker) publish(ev TenantEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ev.Version <= 0 {
		b.nextMemVersion++
		ev.Version = b.nextMemVersion
	} else if ev.Version > b.nextMemVersion {
		b.nextMemVersion = ev.Version
	}
	if ev.EmittedAtUnixMs == 0 {
		ev.EmittedAtUnixMs = time.Now().UnixMilli()
	}

	b.history = append(b.history, ev)
	if len(b.history) > maxEventHistory {
		trim := len(b.history) - maxEventHistory
		b.history = append([]TenantEvent(nil), b.history[trim:]...)
	}

	for id, ch := range b.subs {
		select {
		case ch <- ev:
		default:
			// Slow consumer: drop its channel so it reconnects and performs full resync.
			close(ch)
			delete(b.subs, id)
		}
	}
}

func (b *tenantEventBroker) currentMemVersion() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.nextMemVersion
}

func (b *tenantEventBroker) subscribe() (uint64, chan TenantEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs == nil {
		b.subs = make(map[uint64]chan TenantEvent)
	}
	b.nextSubID++
	id := b.nextSubID
	ch := make(chan TenantEvent, subscriberBufSize)
	b.subs[id] = ch
	return id, ch
}

func (b *tenantEventBroker) unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

func (b *tenantEventBroker) memBacklog(afterVersion int64) ([]TenantEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.history) == 0 {
		if afterVersion > b.nextMemVersion {
			return nil, true
		}
		return nil, false
	}
	oldest := b.history[0].Version
	if afterVersion > 0 && afterVersion < oldest-1 {
		return nil, true
	}
	var out []TenantEvent
	for _, ev := range b.history {
		if ev.Version > afterVersion {
			out = append(out, ev)
		}
	}
	return out, false
}

func parseEventType(raw string) pb.TenantEventType {
	switch raw {
	case tenantEventDelete:
		return pb.TenantEventType_TENANT_EVENT_DELETE
	case tenantEventUpsert:
		return pb.TenantEventType_TENANT_EVENT_UPSERT
	default:
		return pb.TenantEventType_TENANT_EVENT_UNSPECIFIED
	}
}

func toPBEvent(ev TenantEvent) *pb.TenantChangeEvent {
	var route *pb.TenantRouteResponse
	if ev.Type != pb.TenantEventType_TENANT_EVENT_RESYNC_REQUIRED {
		route = &pb.TenantRouteResponse{
			TenantId: ev.TenantID,
			State:    ev.State,
			Host:     ev.Host,
			Port:     ev.DirectPort,
		}
	}
	return &pb.TenantChangeEvent{
		Version:         ev.Version,
		Type:            ev.Type,
		Route:           route,
		FencingEpoch:    ev.FencingEpoch,
		EmittedAtUnixMs: ev.EmittedAtUnixMs,
	}
}

func (cp *ControlPlane) emitTenantEvent(ctx context.Context, tenantID, eventType, state string, directPort int, fencingEpoch int64) {
	host := cp.backendHost
	if host == "" {
		host = "127.0.0.1"
	}
	if fencingEpoch <= 0 {
		fencingEpoch = 1
	}

	ev := TenantEvent{
		Type:            parseEventType(eventType),
		TenantID:        tenantID,
		State:           state,
		Host:            host,
		DirectPort:      int32(directPort),
		FencingEpoch:    fencingEpoch,
		EmittedAtUnixMs: time.Now().UnixMilli(),
	}

	if cp.db != nil {
		err := cp.db.QueryRow(ctx, `
			INSERT INTO tenant_events (tenant_id, event_type, state, host, direct_port, fencing_epoch)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING version, (EXTRACT(EPOCH FROM emitted_at) * 1000)::BIGINT
		`, tenantID, eventType, state, host, directPort, fencingEpoch).Scan(&ev.Version, &ev.EmittedAtUnixMs)
		if err != nil {
			log.Printf("[events] insert tenant_events failed for %s (%s): %v", tenantID, eventType, err)
		}
	}

	cp.eventBroker.publish(ev)
}

// CurrentEventVersion returns the latest committed tenant_events version.
func (cp *ControlPlane) CurrentEventVersion(ctx context.Context) int64 {
	if cp.db != nil {
		var v int64
		if err := cp.db.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM tenant_events`).Scan(&v); err == nil {
			return v
		}
	}
	return cp.eventBroker.currentMemVersion()
}

// SubscribeTenantEvents returns missed events since afterVersion and a live channel for subsequent events.
// If the gap exceeds maxStreamBacklog or history was pruned, needResync is true.
func (cp *ControlPlane) SubscribeTenantEvents(ctx context.Context, afterVersion int64) (backlog []TenantEvent, needResync bool, ch <-chan TenantEvent, cancel func()) {
	subID, subCh := cp.eventBroker.subscribe()
	cancelFn := func() {
		cp.eventBroker.unsubscribe(subID)
	}

	if cp.db == nil {
		bl, resync := cp.eventBroker.memBacklog(afterVersion)
		if resync {
			cancelFn()
			return nil, true, nil, func() {}
		}
		return bl, false, subCh, cancelFn
	}

	rows, err := cp.db.Query(ctx, `
		SELECT version, event_type, tenant_id, state, host, direct_port, fencing_epoch,
		       (EXTRACT(EPOCH FROM emitted_at) * 1000)::BIGINT
		FROM tenant_events
		WHERE version > $1
		ORDER BY version ASC
		LIMIT $2
	`, afterVersion, maxStreamBacklog+1)
	if err != nil {
		log.Printf("[events] query backlog failed: %v", err)
		return nil, false, subCh, cancelFn
	}
	defer rows.Close()

	for rows.Next() {
		var ev TenantEvent
		var rawType string
		if err := rows.Scan(&ev.Version, &rawType, &ev.TenantID, &ev.State, &ev.Host, &ev.DirectPort, &ev.FencingEpoch, &ev.EmittedAtUnixMs); err != nil {
			cancelFn()
			return nil, true, nil, func() {}
		}
		ev.Type = parseEventType(rawType)
		backlog = append(backlog, ev)
		if len(backlog) > maxStreamBacklog {
			cancelFn()
			return nil, true, nil, func() {}
		}
	}
	if err := rows.Err(); err != nil {
		cancelFn()
		return nil, true, nil, func() {}
	}

	return backlog, false, subCh, cancelFn
}
