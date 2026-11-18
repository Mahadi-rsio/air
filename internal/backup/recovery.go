package backup

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	RecoveryTargetLatest = "latest"
	RecoveryTargetLSN    = "lsn"
	RecoveryTargetTime   = "time"
)

// RecoveryTarget is a validated PostgreSQL PITR target.
type RecoveryTarget struct {
	Kind string `json:"kind"`
	LSN  string `json:"lsn,omitempty"`
	Time string `json:"time,omitempty"`

	lsnValue  uint64
	timeValue time.Time
}

// NewLatestRecoveryTarget requests the latest recoverable state.
func NewLatestRecoveryTarget() RecoveryTarget { return RecoveryTarget{Kind: RecoveryTargetLatest} }

// NewLSNRecoveryTarget validates an LSN target.
func NewLSNRecoveryTarget(lsn string) (RecoveryTarget, error) {
	v, err := ParseLSN(lsn)
	if err != nil {
		return RecoveryTarget{}, err
	}
	return RecoveryTarget{Kind: RecoveryTargetLSN, LSN: FormatLSN(v), lsnValue: v}, nil
}

// NewTimeRecoveryTarget validates a timestamp target. RFC3339 and PostgreSQL's
// "YYYY-MM-DD HH:MM:SS[.ffffff][+TZ]" forms are accepted.
func NewTimeRecoveryTarget(ts string) (RecoveryTarget, error) {
	ts = strings.TrimSpace(ts)
	if ts == "" {
		return RecoveryTarget{}, fmt.Errorf("%w: empty target time", ErrInvalidRecoveryTarget)
	}
	var parsed time.Time
	var err error
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if parsed, err = time.Parse(layout, ts); err == nil {
			break
		}
	}
	if err != nil {
		return RecoveryTarget{}, fmt.Errorf("%w: %q", ErrInvalidRecoveryTarget, ts)
	}
	return RecoveryTarget{Kind: RecoveryTargetTime, Time: parsed.UTC().Format(time.RFC3339Nano), timeValue: parsed.UTC()}, nil
}

// ParseRecoveryTarget chooses exactly one of latest, LSN, or time.
func ParseRecoveryTarget(latest bool, lsn, targetTime string) (RecoveryTarget, error) {
	lsn = strings.TrimSpace(lsn)
	targetTime = strings.TrimSpace(targetTime)
	n := 0
	if latest {
		n++
	}
	if lsn != "" {
		n++
	}
	if targetTime != "" {
		n++
	}
	if n != 1 {
		return RecoveryTarget{}, fmt.Errorf("%w: specify exactly one of latest, target_lsn, or target_time", ErrInvalidRecoveryTarget)
	}
	if latest {
		return NewLatestRecoveryTarget(), nil
	}
	if lsn != "" {
		return NewLSNRecoveryTarget(lsn)
	}
	return NewTimeRecoveryTarget(targetTime)
}

// Validate checks the target kind and value.
func (t RecoveryTarget) Validate() error {
	switch t.Kind {
	case RecoveryTargetLatest:
		return nil
	case RecoveryTargetLSN:
		_, err := ParseLSN(t.LSN)
		return err
	case RecoveryTargetTime:
		if strings.TrimSpace(t.Time) == "" {
			return fmt.Errorf("%w: empty time", ErrInvalidRecoveryTarget)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidRecoveryTarget, t.Kind)
	}
}

// ValidateAgainstManifest refuses targets that cannot possibly be recovered from this backup.
// A target before the backup's minimum recovery LSN/time is not recoverable.
func (t RecoveryTarget) ValidateAgainstManifest(m *BackupManifest) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if m == nil {
		return errors.New("backup: nil manifest")
	}
	switch t.Kind {
	case RecoveryTargetLatest:
		return nil
	case RecoveryTargetLSN:
		req, err := ParseLSN(t.LSN)
		if err != nil {
			return err
		}
		min, _ := ParseLSN(m.ConsistencyLSN)
		start, _ := ParseLSN(m.WALStartLSN)
		end, _ := ParseLSN(m.WALEndLSN)
		if req < min {
			return fmt.Errorf("%w: target %s precedes backup minimum recovery LSN %s", ErrTargetOutOfRange, t.LSN, m.ConsistencyLSN)
		}
		if req < start {
			return fmt.Errorf("%w: target %s precedes retained WAL start %s", ErrTargetOutOfRange, t.LSN, m.WALStartLSN)
		}
		if req > end {
			return fmt.Errorf("%w: target %s exceeds archived WAL end %s", ErrTargetOutOfRange, t.LSN, m.WALEndLSN)
		}
		return nil
	case RecoveryTargetTime:
		_, err := NewTimeRecoveryTarget(t.Time)
		if err != nil {
			return err
		}
		parsed, _ := time.Parse(time.RFC3339Nano, t.Time)
		if !m.ConsistencyTime.IsZero() && parsed.Before(m.ConsistencyTime) {
			return fmt.Errorf("%w: target time %s precedes backup minimum recovery time %s", ErrTargetOutOfRange, t.Time, m.ConsistencyTime.UTC().Format(time.RFC3339Nano))
		}
		if parsed.After(time.Now().Add(5 * time.Minute)) {
			return fmt.Errorf("%w: target time %s is in the future", ErrTargetOutOfRange, t.Time)
		}
		return nil
	}
	return nil
}

// recoveryConf renders PostgreSQL recovery settings for this target.
func (t RecoveryTarget) recoveryConf() (string, error) {
	if err := t.Validate(); err != nil {
		return "", err
	}
	switch t.Kind {
	case RecoveryTargetLatest:
		return "", nil
	case RecoveryTargetLSN:
		return fmt.Sprintf("recovery_target_lsn = '%s'\nrecovery_target_inclusive = off\n", t.LSN), nil
	case RecoveryTargetTime:
		return fmt.Sprintf("recovery_target_time = '%s'\nrecovery_target_inclusive = off\n", t.Time), nil
	}
	return "", fmt.Errorf("%w", ErrInvalidRecoveryTarget)
}

// String returns a stable human-readable target.
func (t RecoveryTarget) String() string {
	switch t.Kind {
	case RecoveryTargetLSN:
		return "lsn=" + t.LSN
	case RecoveryTargetTime:
		return "time=" + t.Time
	default:
		return "latest"
	}
}
