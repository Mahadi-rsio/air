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

// ValidateWALContinuity verifies that all WAL refs share one timeline and form an
// unbroken ascending segment sequence covering [startLSN, endLSN].
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
		m := walSegmentNameRe.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(r.SegmentName)))
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
	sort.Slice(segs, func(i, j int) bool { return segs[i].start < segs[j].start })

	firstSegment := start - (start % WALSegmentSize)
	lastCovered := segs[len(segs)-1].start
	if lastCovered > end {
		lastCovered = end
	}
	for i, s := range segs {
		if i == 0 {
			if s.start < firstSegment {
				return fmt.Errorf("%w: first retained segment %s starts before required %s", ErrWALDiscontinuity, s.name, FormatLSN(firstSegment))
			}
			if s.start > firstSegment {
				return fmt.Errorf("%w: missing WAL segment covering %s (first retained %s)", ErrMissingWALSegment, FormatLSN(firstSegment), s.name)
			}
			continue
		}
		want := segs[i-1].start + WALSegmentSize
		if s.start != want {
			if s.start < want {
				return fmt.Errorf("%w: duplicate/overlapping WAL segment %s", ErrWALDiscontinuity, s.name)
			}
			return fmt.Errorf("%w: missing WAL segment after %s before %s", ErrMissingWALSegment, segs[i-1].name, s.name)
		}
	}
	if lastCovered < end {
		return fmt.Errorf("%w: WAL archive ends at %s before required end LSN %s", ErrMissingWALSegment, FormatLSN(lastCovered), endLSN)
	}
	return nil
}
