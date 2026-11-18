package backup

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	// WALSegmentSize is PostgreSQL's default 16 MiB segment size.
	WALSegmentSize uint64 = 16 * 1024 * 1024
	walSegmentRe13        = `^([0-9A-F]{8})([0-9A-F]{8})([0-9A-F]{8})$`
)

var walSegmentNameRe = regexp.MustCompile(walSegmentRe13)

// ParseLSN parses PostgreSQL's textual "HIGH/LOW" LSN format.
func ParseLSN(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, fmt.Errorf("%w: malformed LSN %q", ErrInvalidRecoveryTarget, s)
	}
	hi, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: malformed LSN %q", ErrInvalidRecoveryTarget, s)
	}
	lo, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: malformed LSN %q", ErrInvalidRecoveryTarget, s)
	}
	return (hi << 32) | lo, nil
}

// FormatLSN renders an LSN in PostgreSQL's textual HIGH/LOW format.
func FormatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// IsWALSegmentName reports whether name is a standard 24-hex-character PostgreSQL WAL segment name.
func IsWALSegmentName(name string) bool {
	return walSegmentNameRe.MatchString(strings.ToUpper(strings.TrimSpace(name)))
}

// WALSegmentStartLSN parses a 24-hex WAL segment name and returns its starting LSN.
func WALSegmentStartLSN(name string) (uint64, error) {
	m := walSegmentNameRe.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(name)))
	if m == nil {
		return 0, fmt.Errorf("%w: invalid WAL segment name %q", ErrInvalidBackupManifest, name)
	}
	log, err := strconv.ParseUint(m[2], 16, 32)
	if err != nil {
		return 0, err
	}
	seg, err := strconv.ParseUint(m[3], 16, 32)
	if err != nil {
		return 0, err
	}
	if seg > 255 {
		return 0, fmt.Errorf("%w: WAL segment index out of range %q", ErrInvalidBackupManifest, name)
	}
	return ((log * 256) + seg) * WALSegmentSize, nil
}

// ValidateWALContinuity verifies that all 24-hex WAL segment refs share one timeline and form an
// unbroken ascending segment sequence covering [startLSN, endLSN]. Auxiliary timeline history
// (.history) and backup label (.backup) files are ignored for 16 MiB segment arithmetic.
func ValidateWALContinuity(refs []WALRef, startLSN, endLSN string) error {
	if len(refs) == 0 {
		return fmt.Errorf("%w: no WAL segments", ErrWALDiscontinuity)
	}
	start, err := ParseLSN(startLSN)
	if err != nil {
		return err
	}
	end, err := ParseLSN(endLSN)
	if err != nil {
		return err
	}
	if end < start {
		return fmt.Errorf("%w: end LSN %s precedes start LSN %s", ErrWALDiscontinuity, endLSN, startLSN)
	}

	type seg struct {
		name  string
		start uint64
		tl    string
	}
	segs := make([]seg, 0, len(refs))
	timeline := ""
	for _, r := range refs {
		trimmed := strings.TrimSpace(r.SegmentName)
		if walHistoryRe.MatchString(trimmed) || walBackupLabelRe.MatchString(trimmed) {
			continue
		}
		m := walSegmentNameRe.FindStringSubmatch(strings.ToUpper(trimmed))
		if m == nil {
			return fmt.Errorf("%w: invalid WAL segment name %q", ErrWALDiscontinuity, r.SegmentName)
		}
		if timeline == "" {
			timeline = m[1]
		} else if m[1] != timeline {
			return fmt.Errorf("%w: mixed timeline IDs %s and %s", ErrWALDiscontinuity, timeline, m[1])
		}
		if r.TimelineID != "" && !strings.EqualFold(r.TimelineID, m[1]) {
			return fmt.Errorf("%w: WAL ref timeline %s does not match segment %s", ErrWALDiscontinuity, r.TimelineID, r.SegmentName)
		}
		s, err := WALSegmentStartLSN(r.SegmentName)
		if err != nil {
			return err
		}
		segs = append(segs, seg{name: r.SegmentName, start: s, tl: m[1]})
	}
	if len(segs) == 0 {
		return fmt.Errorf("%w: no 24-hex WAL segments", ErrWALDiscontinuity)
	}
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].start != segs[j].start {
			return segs[i].start < segs[j].start
		}
		return segs[i].name < segs[j].name
	})

	firstSegment := start - (start % WALSegmentSize)
	if segs[0].start > firstSegment {
		return fmt.Errorf("%w: missing WAL segment covering %s (first retained %s)", ErrMissingWALSegment, FormatLSN(firstSegment), segs[0].name)
	}
	for i := 1; i < len(segs); i++ {
		want := segs[i-1].start + WALSegmentSize
		if segs[i].start != want {
			if segs[i].start < want {
				return fmt.Errorf("%w: duplicate/overlapping WAL segment %s", ErrWALDiscontinuity, segs[i].name)
			}
			return fmt.Errorf("%w: missing WAL segment after %s before %s", ErrMissingWALSegment, segs[i-1].name, segs[i].name)
		}
	}
	lastCovered := segs[len(segs)-1].start + WALSegmentSize
	if lastCovered < end {
		return fmt.Errorf("%w: WAL archive ends at %s before required end LSN %s", ErrMissingWALSegment, FormatLSN(lastCovered), endLSN)
	}
	return nil
}
