package pageserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// WALBatchSink receives decoded PG17 WAL record batches (implemented by remote gRPC pageserver clients).
type WALBatchSink interface {
	IngestWALBatch(ctx context.Context, recs []*DecodedWALRecord, sawCheckpoint bool) (uint64, error)
}

// WALReceiver streams physical WAL from a PostgreSQL 17 primary into a Timeline or remote WALBatchSink.
type WALReceiver struct {
	dsn      string
	timeline *Timeline
	sink     WALBatchSink
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

// NewRemoteWALReceiver creates a physical replication receiver that forwards decoded WAL batches to sink.
func NewRemoteWALReceiver(dsn string, sink WALBatchSink, startLSN uint64) *WALReceiver {
	return &WALReceiver{
		dsn:      dsn,
		sink:     sink,
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

	startLSN := r.startLSN
	if startLSN == 0 {
		res := conn.Exec(ctx, "IDENTIFY_SYSTEM")
		rows, err := res.ReadAll()
		if err != nil {
			return fmt.Errorf("pageserver: IDENTIFY_SYSTEM: %w", err)
		}
		if len(rows) > 0 && len(rows[0].Rows) > 0 && len(rows[0].Rows[0]) >= 3 {
			var hi, lo uint32
			if _, sErr := fmt.Sscanf(string(rows[0].Rows[0][2]), "%X/%X", &hi, &lo); sErr == nil {
				startLSN = (uint64(hi) << 32) | uint64(lo)
			}
		}
	}
	// Align START_REPLICATION to an 8192-byte WAL page boundary so the stream
	// begins with a valid XLogPageHeaderData.
	alignedStart := startLSN & ^uint64(XLogPageSize-1)
	startStr := formatLSN(alignedStart)

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

	var parser WALStreamParser
	lastLSN := startLSN
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
			recs, pErr := parser.Push(walPayload)
			if pErr != nil {
				n := len(walPayload)
				if n > 16 {
					n = 16
				}
				return fmt.Errorf("pageserver: parse wal stream at %x (len=%d head=%x): %w", binary.BigEndian.Uint64(cd.Data[1:9]), len(walPayload), walPayload[:n], pErr)
			}
			sawCheckpoint := false
			for _, rec := range recs {
				if rec.Rmid == RmXLogID {
					op := rec.Info & XLogInfoOpMask
					if op == 0x00 || op == 0x10 {
						sawCheckpoint = true
					}
				}
			}
			if r.sink != nil {
				if len(recs) > 0 {
					lsn, sErr := r.sink.IngestWALBatch(ctx, recs, sawCheckpoint)
					if sErr != nil {
						return fmt.Errorf("pageserver: remote wal ingest: %w", sErr)
					}
					if lsn > lastLSN {
						lastLSN = lsn
					}
				}
			} else if r.timeline != nil {
				for _, rec := range recs {
					for _, blk := range rec.Blocks {
						r.timeline.EnsureRelSizeSeeded(ctx, blk.Rel.SpcOID, blk.Rel.DbOID, blk.Rel.RelNumber, blk.ForkNum)
					}
					r.timeline.ApplyWALRecord(rec)
				}
				if sawCheckpoint {
					if _, fErr := r.timeline.FlushInMemory(ctx); fErr != nil {
						return fmt.Errorf("pageserver: checkpoint flush: %w", fErr)
					}
				} else if err := r.timeline.EnforceInMemBudget(ctx); err != nil {
					return fmt.Errorf("pageserver: walreceiver budget flush: %w", err)
				}
				lastLSN = r.timeline.LastLSN()
			}
			_ = walEnd
		case 'k':
			if len(cd.Data) >= 18 && cd.Data[17] != 0 {
				if r.timeline != nil {
					lastLSN = r.timeline.LastLSN()
				}
				_ = r.sendStandbyStatus(conn, lastLSN)
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
