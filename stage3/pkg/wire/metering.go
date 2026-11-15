package wire

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketBillingEvents = []byte("billing_events")
	bucketBillingLedger = []byte("billing_ledger")
)

type BillingEvent struct {
	TenantID       string `json:"tenant_id"`
	WindowID       string `json:"window_id"`
	Sequence       uint64 `json:"sequence"`
	ReadUnits      int64  `json:"read_units"`
	BytesSent      int64  `json:"bytes_sent"`
	IdempotencyKey string `json:"idempotency_key"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
}

// MeteringLedger persists RU windows in bbolt and enforces idempotent queue ingestion.
type MeteringLedger struct {
	mu     sync.Mutex
	dbPath string
	db     *bolt.DB
}

func OpenMeteringLedger(dbPath string) (*MeteringLedger, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt metering ledger: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketBillingEvents); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketBillingLedger); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &MeteringLedger{dbPath: dbPath, db: db}, nil
}

func (m *MeteringLedger) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.db != nil {
		err := m.db.Close()
		m.db = nil
		return err
	}
	return nil
}

// ComputeWireRUs calculates deterministic Request Units for a query response:
// 1 base RU + ceil(bytesSent / 16384). Internal wakeups/healthchecks are never billed.
func ComputeWireRUs(bytesSent int64) int64 {
	if bytesSent <= 0 {
		return 1
	}
	extra := (bytesSent + 16383) / 16384
	return 1 + extra
}

func MakeIdempotencyKey(tenantID, windowID string, seq uint64) string {
	h := sha256.New()
	h.Write([]byte(tenantID))
	h.Write([]byte("|"))
	h.Write([]byte(windowID))
	h.Write([]byte("|"))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	h.Write(buf[:])
	return hex.EncodeToString(h.Sum(nil))
}

// AppendWindow writes an idempotent RU window to the local bbolt WAL before ACKing.
func (m *MeteringLedger) AppendWindow(tenantID, windowID string, seq uint64, bytesSent int64) (BillingEvent, error) {
	ru := ComputeWireRUs(bytesSent)
	ev := BillingEvent{
		TenantID:       tenantID,
		WindowID:       windowID,
		Sequence:       seq,
		ReadUnits:      ru,
		BytesSent:      bytesSent,
		IdempotencyKey: MakeIdempotencyKey(tenantID, windowID, seq),
		CreatedAtUnix:  time.Now().UnixNano(),
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return ev, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.db == nil {
		return ev, nil
	}
	err = m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBillingEvents)
		return b.Put([]byte(ev.IdempotencyKey), raw)
	})
	return ev, err
}

// IngestQueueBatch simulates Cloudflare Queue /pull -> INSERT ON CONFLICT DO NOTHING -> /ack.
// Returns (insertedCount, deduplicatedCount, error).
func (m *MeteringLedger) IngestQueueBatch(events []BillingEvent) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inserted := 0
	deduped := 0
	err := m.db.Update(func(tx *bolt.Tx) error {
		ledger := tx.Bucket(bucketBillingLedger)
		for _, ev := range events {
			key := []byte(ev.IdempotencyKey)
			if ledger.Get(key) != nil {
				deduped++
				continue
			}
			raw, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			if err := ledger.Put(key, raw); err != nil {
				return err
			}
			inserted++
		}
		return nil
	})
	return inserted, deduped, err
}

// PendingWALEvents reads all local bbolt WAL events (e.g., after Gateway SIGKILL/restart).
func (m *MeteringLedger) PendingWALEvents() ([]BillingEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []BillingEvent
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBillingEvents)
		return b.ForEach(func(_, v []byte) error {
			var ev BillingEvent
			if err := json.Unmarshal(v, &ev); err != nil {
				return err
			}
			out = append(out, ev)
			return nil
		})
	})
	return out, err
}

// TotalLedgerRUs returns total RUs committed in the deduplicated ledger for a tenant.
func (m *MeteringLedger) TotalLedgerRUs(tenantID string) (int64, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	count := 0
	err := m.db.View(func(tx *bolt.Tx) error {
		ledger := tx.Bucket(bucketBillingLedger)
		return ledger.ForEach(func(_, v []byte) error {
			var ev BillingEvent
			if err := json.Unmarshal(v, &ev); err != nil {
				return err
			}
			if tenantID == "" || ev.TenantID == tenantID {
				total += ev.ReadUnits
				count++
			}
			return nil
		})
	})
	return total, count, err
}
