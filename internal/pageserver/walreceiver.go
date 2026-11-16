package pageserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// WALReceiver streams physical WAL from a PostgreSQL 17 primary into a Timeline.
type WALReceiver struct {
	dsn      string
	timeline *Timeline
	startLSN uint64
}

// NewWALReceiver creates a physical replication receiver for timeline.
func NewWALReceiver(dsn string, timeline *Timeline, startLSN uint64) *WALReceiver {
	return &WALReceiver{
		dsn:      dsn,
		timeline: timeline,
		startLSN: startLSN,
	}
}

// IngestWALPages parses 8192-byte-aligned PG17 WAL pages and applies records to timeline.
func (r *WALReceiver) IngestWALPages(ctx context.Context, walPages []byte) (int, error) {
	recs, err := ParseWALPages(walPages)
	if err != nil {
		return 0, err
	}
	for _, rec := range recs {
		r.timeline.ApplyWALRecord(rec)
	}
	if r.timeline.inMem.ApproxBytes >= r.timeline.maxInMemBytes {
		if _, err := r.timeline.FlushInMemory(ctx); err != nil {
			return len(recs), err
		}
	}
	return len(recs), nil
}

// Run connects to PostgreSQL using replication=database and streams WAL until ctx is cancelled.
func (r *WALReceiver) Run(ctx context.Context) error {
	cfg, err := pgconn.ParseConfig(r.dsn)
	if err != nil {
		return err
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	startStr := formatLSN(r.startLSN)
	if r.startLSN == 0 {
		res := conn.Exec(ctx, "IDENTIFY_SYSTEM")
		rows, err := res.ReadAll()
		if err != nil {
			return fmt.Errorf("pageserver: IDENTIFY_SYSTEM: %w", err)
		}
		if len(rows) > 0 && len(rows[0].Rows) > 0 && len(rows[0].Rows[0]) >= 3 {
			startStr = string(rows[0].Rows[0][2])
		} else {
			startStr = "0/0"
		}
	}

	sql := fmt.Sprintf("START_REPLICATION %s", startStr)
	conn.Frontend().Send(&pgproto3.Query{String: sql})
	if err := conn.Frontend().Flush(); err != nil {
		return err
	}

	// Wait for CopyBothResponse.
	for {
		msg, err := conn.Frontend().Receive()
		if err != nil {
			return err
		}
		if _, ok := msg.(*pgproto3.CopyBothResponse); ok {
			break
		}
		if errResp, ok := msg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("pageserver: START_REPLICATION error: %s", errResp.Message)
		}
	}

	var pageBuf []byte
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		msg, err := conn.Frontend().Receive()
		if err != nil {
			return err
		}
		cd, ok := msg.(*pgproto3.CopyData)
		if !ok || len(cd.Data) == 0 {
			continue
		}
		switch cd.Data[0] {
		case 'w':
			if len(cd.Data) < 25 {
				continue
			}
			walEnd := binary.BigEndian.Uint64(cd.Data[9:17])
			walPayload := cd.Data[25:]
			pageBuf = append(pageBuf, walPayload...)
			if len(pageBuf) >= XLogPageSize {
				aligned := (len(pageBuf) / XLogPageSize) * XLogPageSize
				_, _ = r.IngestWALPages(ctx, pageBuf[:aligned])
				pageBuf = append([]byte(nil), pageBuf[aligned:]...)
			}
			_ = walEnd
		case 'k':
			if len(cd.Data) >= 18 && cd.Data[17] != 0 {
				_ = r.sendStandbyStatus(conn, r.timeline.LastLSN())
			}
		}
	}
}

func (r *WALReceiver) sendStandbyStatus(conn *pgconn.PgConn, lsn uint64) error {
	// 'r' + writeLSN(8) + flushLSN(8) + applyLSN(8) + timestamp(8) + reply(1) = 34 bytes
	var pkt [34]byte
	pkt[0] = 'r'
	binary.BigEndian.PutUint64(pkt[1:9], lsn)
	binary.BigEndian.PutUint64(pkt[9:17], lsn)
	binary.BigEndian.PutUint64(pkt[17:25], lsn)
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	binary.BigEndian.PutUint64(pkt[25:33], uint64(time.Since(pgEpoch).Microseconds()))
	pkt[33] = 0
	conn.Frontend().Send(&pgproto3.CopyData{Data: pkt[:]})
	return conn.Frontend().Flush()
}

func formatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
