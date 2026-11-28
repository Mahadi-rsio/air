package autoscaler

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const stateFileName = "autoscaler_state.json"

// OperationKind identifies scale_out vs scale_in operations.
type OperationKind string

const (
	OpKindScaleOut OperationKind = "scale_out"
	OpKindScaleIn  OperationKind = "scale_in"
)

// OperationState represents the persisted state machine of an autoscaler operation.
type OperationState string

const (
	OpStateIntent         OperationState = "intent"
	OpStateVolumeReady    OperationState = "volume_ready"
	OpStateMachineCreated OperationState = "machine_created"
	OpStateReadyVerified  OperationState = "ready_verified"
	OpStateAdmitted       OperationState = "admitted"
	OpStateUnresolved     OperationState = "unresolved"
	OpStateFailed         OperationState = "failed"

	OpStateDraining OperationState = "draining"
	OpStateFlushed  OperationState = "flushed"
	OpStateStopped  OperationState = "stopped"
	OpStateDeleted  OperationState = "deleted"
	OpStateDeferred OperationState = "deferred"
)

// Operation records intent and progress of a single scaling operation.
type Operation struct {
	OpID            string         `json:"op_id"`
	Kind            OperationKind  `json:"kind"`
	State           OperationState `json:"state"`
	MachineID       string         `json:"machine_id,omitempty"`
	VolumeID        string         `json:"volume_id,omitempty"`
	CreateAttempted bool           `json:"create_attempted,omitempty"`
	CreatedAtMs     int64          `json:"created_at"`
	UpdatedAtMs     int64          `json:"updated_at"`
	LastError       string         `json:"last_error,omitempty"`
	Details         map[string]any `json:"details,omitempty"`
}

// MachineVolume tracks known Machine-to-volume associations and roles.
type MachineVolume struct {
	MachineID   string `json:"machine_id"`
	VolumeID    string `json:"volume_id"`
	Role        string `json:"role"`
	Region      string `json:"region"`
	SizeGB      int    `json:"size_gb"`
	VMSize      string `json:"vm_size"`
	State       string `json:"state"`
	CreatedAtMs int64  `json:"created_at"`
	UpdatedAtMs int64  `json:"updated_at"`
}

// PersistentState is the durable JSON state persisted on the Fly Volume.
type PersistentState struct {
	Operations         map[string]*Operation     `json:"operations"`
	MachineVolumes     map[string]*MachineVolume `json:"machine_volumes"`
	BaselineMachineID  string                    `json:"baseline_machine_id"`
	SustainedHighTicks int                       `json:"sustained_high_ticks"`
	SustainedLowTicks  int                       `json:"sustained_low_ticks"`
	LastScaleOutAtMs   int64                     `json:"last_scale_out_at_ms"`
	LastScaleInAtMs    int64                     `json:"last_scale_in_at_ms"`
}

// Store persists PersistentState atomically to a dedicated Fly Volume directory.
type Store struct {
	mu       sync.Mutex
	dir      string
	filePath string
	state    PersistentState
}

// NewStore opens or initializes the durable state store in dir.
func NewStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("state dir is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create state dir %s: %w", dir, err)
	}
	s := &Store{
		dir:      dir,
		filePath: filepath.Join(dir, stateFileName),
		state: PersistentState{
			Operations:     make(map[string]*Operation),
			MachineVolumes: make(map[string]*MachineVolume),
		},
	}
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) loadLocked() error {
	b, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read state file %s: %w", s.filePath, err)
	}
	if len(b) == 0 {
		return nil
	}
	var st PersistentState
	if err := json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("unmarshal state file %s: %w", s.filePath, err)
	}
	if st.Operations == nil {
		st.Operations = make(map[string]*Operation)
	}
	if st.MachineVolumes == nil {
		st.MachineVolumes = make(map[string]*MachineVolume)
	}
	s.state = st
	return nil
}

func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(&s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	tmp, err := os.CreateTemp(s.dir, "autoscaler_state.*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, s.filePath); err != nil {
		return fmt.Errorf("rename state file: %w", err)
	}
	cleanup = false

	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Snapshot returns a copy of the current persistent state for status inspection.
func (s *Store) Snapshot() PersistentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := PersistentState{
		Operations:         make(map[string]*Operation, len(s.state.Operations)),
		MachineVolumes:     make(map[string]*MachineVolume, len(s.state.MachineVolumes)),
		BaselineMachineID:  s.state.BaselineMachineID,
		SustainedHighTicks: s.state.SustainedHighTicks,
		SustainedLowTicks:  s.state.SustainedLowTicks,
		LastScaleOutAtMs:   s.state.LastScaleOutAtMs,
		LastScaleInAtMs:    s.state.LastScaleInAtMs,
	}
	for k, op := range s.state.Operations {
		cp := *op
		if op.Details != nil {
			cp.Details = make(map[string]any, len(op.Details))
			for dk, dv := range op.Details {
				cp.Details[dk] = dv
			}
		}
		out.Operations[k] = &cp
	}
	for k, mv := range s.state.MachineVolumes {
		cp := *mv
		out.MachineVolumes[k] = &cp
	}
	return out
}

// Update mutates the persistent state atomically and fsyncs it to disk.
func (s *Store) Update(fn func(st *PersistentState)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state)
	return s.saveLocked()
}

// PendingScaleOutOps returns all scale_out operations that are not yet terminal.
func (s *Store) PendingScaleOutOps() []*Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Operation
	for _, op := range s.state.Operations {
		if op.Kind != OpKindScaleOut {
			continue
		}
		switch op.State {
		case OpStateIntent, OpStateVolumeReady, OpStateMachineCreated, OpStateReadyVerified, OpStateUnresolved:
			cp := *op
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtMs != out[j].CreatedAtMs {
			return out[i].CreatedAtMs < out[j].CreatedAtMs
		}
		return out[i].OpID < out[j].OpID
	})
	return out
}

// PendingScaleInOps returns all scale_in operations that are not yet terminal.
func (s *Store) PendingScaleInOps() []*Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Operation
	for _, op := range s.state.Operations {
		if op.Kind != OpKindScaleIn {
			continue
		}
		switch op.State {
		case OpStateDraining, OpStateFlushed, OpStateStopped:
			cp := *op
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtMs != out[j].CreatedAtMs {
			return out[i].CreatedAtMs < out[j].CreatedAtMs
		}
		return out[i].OpID < out[j].OpID
	})
	return out
}
