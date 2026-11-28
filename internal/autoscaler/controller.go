package autoscaler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// TickResult describes the outcome of a single autoscaler control loop tick.
type TickResult struct {
	Action           string  `json:"action"`
	Mode             string  `json:"mode"`
	OpID             string  `json:"op_id,omitempty"`
	MachineID        string  `json:"machine_id,omitempty"`
	RetainedVolumeID string  `json:"retained_volume_id,omitempty"`
	ActiveMachines   int     `json:"active_machines"`
	BaselineID       string  `json:"baseline_id,omitempty"`
	ActiveConns      int64   `json:"active_conns"`
	MaxConns         int     `json:"max_conns"`
	UtilPct          float64 `json:"util_pct"`
	SpareConns       int     `json:"spare_conns"`
	HighTicks        int     `json:"high_ticks"`
	LowTicks         int     `json:"low_ticks"`
	Reason           string  `json:"reason,omitempty"`
}

// StatusSnapshot is returned by GET /status.
type StatusSnapshot struct {
	Mode              string                    `json:"mode"`
	AutoAddEnabled    bool                      `json:"auto_add_enabled"`
	AutoRemoveEnabled bool                      `json:"auto_remove_enabled"`
	BaselineMachineID string                    `json:"baseline_machine_id"`
	MaxReplicas       int                       `json:"max_replicas"`
	State             PersistentState           `json:"state"`
	LastTick          *TickResult               `json:"last_tick,omitempty"`
	LastReconcileAtMs int64                     `json:"last_reconcile_at_ms,omitempty"`
	LastError         string                    `json:"last_error,omitempty"`
	MachinesKnown     map[string]*MachineVolume `json:"machines_known,omitempty"`
}

// Controller implements the native Fly.io gateway autoscaler control loop.
type Controller struct {
	cfg    Config
	store  *Store
	fly    FlyClient
	prober GatewayProber

	sleepFn func(ctx context.Context, d time.Duration) error

	mu                sync.Mutex
	inTick            bool
	lastTick          *TickResult
	lastReconcileAtMs int64
	lastError         string
}

// NewController constructs a Controller.
func NewController(cfg Config, store *Store, fly FlyClient, prober GatewayProber) *Controller {
	return &Controller{
		cfg:    cfg,
		store:  store,
		fly:    fly,
		prober: prober,
		sleepFn: func(ctx context.Context, d time.Duration) error {
			if d <= 0 {
				return nil
			}
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}
}

func newOpID(prefix string, nowMs int64) string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%d_%s", prefix, nowMs, hex.EncodeToString(b[:]))
}

// ReconcileOnStartup reconciles persisted pending operations against Fly inventory on controller startup.
func (c *Controller) ReconcileOnStartup(ctx context.Context, nowMs int64) error {
	machines, err := c.fly.ListMachines(ctx)
	if err != nil {
		c.setLastError(err.Error())
		return err
	}
	volumes, err := c.fly.ListVolumes(ctx)
	if err != nil {
		c.setLastError(err.Error())
		return err
	}
	if _, err := c.reconcileInventory(machines, volumes, nowMs); err != nil {
		c.setLastError(err.Error())
		return err
	}
	c.mu.Lock()
	c.lastReconcileAtMs = nowMs
	c.lastError = ""
	c.mu.Unlock()
	return nil
}

func (c *Controller) setLastError(msg string) {
	c.mu.Lock()
	c.lastError = msg
	c.mu.Unlock()
}

// Status returns the current controller status and persisted state.
func (c *Controller) Status() StatusSnapshot {
	st := c.store.Snapshot()
	c.mu.Lock()
	var lastTick *TickResult
	if c.lastTick != nil {
		cp := *c.lastTick
		lastTick = &cp
	}
	lastReconcile := c.lastReconcileAtMs
	lastErr := c.lastError
	c.mu.Unlock()

	return StatusSnapshot{
		Mode:              c.cfg.Mode,
		AutoAddEnabled:    c.cfg.AutoAddEnabled,
		AutoRemoveEnabled: c.cfg.AutoRemoveEnabled,
		BaselineMachineID: c.effectiveBaselineIDLocked(nil, st.BaselineMachineID),
		MaxReplicas:       c.cfg.MaxReplicas,
		State:             st,
		LastTick:          lastTick,
		LastReconcileAtMs: lastReconcile,
		LastError:         lastErr,
	}
}

func (c *Controller) effectiveBaselineIDLocked(machines []Machine, stored string) string {
	if c.cfg.BaselineMachineID != "" {
		return c.cfg.BaselineMachineID
	}
	if stored != "" {
		if machines == nil {
			return stored
		}
		for _, m := range machines {
			if m.ID == stored {
				return stored
			}
		}
	}
	if len(machines) == 0 {
		return ""
	}
	var candidates []Machine
	for _, m := range machines {
		if m.Config.Metadata["air_role"] != "burst" {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		candidates = append(candidates, machines...)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CreatedAt != candidates[j].CreatedAt {
			return candidates[i].CreatedAt < candidates[j].CreatedAt
		}
		return candidates[i].ID < candidates[j].ID
	})
	return candidates[0].ID
}

func (c *Controller) identifyBaseline(machines []Machine, nowMs int64) string {
	st := c.store.Snapshot()
	if c.cfg.BaselineMachineID != "" {
		if st.BaselineMachineID != c.cfg.BaselineMachineID {
			_ = c.store.Update(func(s *PersistentState) {
				s.BaselineMachineID = c.cfg.BaselineMachineID
			})
		}
		return c.cfg.BaselineMachineID
	}
	id := c.effectiveBaselineIDLocked(machines, st.BaselineMachineID)
	if id != "" && id != st.BaselineMachineID {
		_ = c.store.Update(func(s *PersistentState) {
			s.BaselineMachineID = id
		})
	}
	_ = nowMs
	return id
}

func findMachineForOp(machines []Machine, op *Operation) *Machine {
	for i := range machines {
		m := &machines[i]
		if (op.OpID != "" && m.Config.Metadata["air_op_id"] == op.OpID) || (op.MachineID != "" && m.ID == op.MachineID) {
			return m
		}
	}
	return nil
}

func findVolumeForOp(volumes []Volume, op *Operation) *Volume {
	expectedName := "air_gw_burst_" + op.OpID
	for i := range volumes {
		v := &volumes[i]
		if (op.OpID != "" && v.Name == expectedName) || (op.VolumeID != "" && v.ID == op.VolumeID) {
			return v
		}
	}
	return nil
}

func (c *Controller) reconcileInventory(machines []Machine, volumes []Volume, nowMs int64) (string, error) {
	pendingOut := c.store.PendingScaleOutOps()
	pendingIn := c.store.PendingScaleInOps()
	baselineID := c.identifyBaseline(machines, nowMs)

	err := c.store.Update(func(s *PersistentState) {
		for _, p := range pendingOut {
			op := s.Operations[p.OpID]
			if op == nil {
				continue
			}
			mm := findMachineForOp(machines, op)
			mv := findVolumeForOp(volumes, op)
			if mm != nil && mv != nil {
				op.MachineID = mm.ID
				op.VolumeID = mv.ID
				op.UpdatedAtMs = nowMs
				if op.State == OpStateIntent || op.State == OpStateVolumeReady || op.State == OpStateUnresolved {
					op.State = OpStateMachineCreated
					op.LastError = ""
				}
			} else if mm != nil && mv == nil {
				op.MachineID = mm.ID
				if len(mm.Config.Mounts) > 0 {
					op.VolumeID = mm.Config.Mounts[0].Volume
				}
				op.UpdatedAtMs = nowMs
				if op.State == OpStateIntent || op.State == OpStateVolumeReady || op.State == OpStateUnresolved {
					op.State = OpStateMachineCreated
					op.LastError = ""
				}
			} else if mm == nil && mv != nil {
				op.VolumeID = mv.ID
				op.UpdatedAtMs = nowMs
				if op.CreateAttempted {
					op.State = OpStateUnresolved
					op.LastError = "reconcile found volume without machine after attempted create"
				} else if op.State == OpStateIntent {
					op.State = OpStateVolumeReady
				}
			} else if op.State == OpStateIntent {
				op.State = OpStateUnresolved
				op.UpdatedAtMs = nowMs
				op.LastError = "reconcile found intent without confirmed inventory"
			}
		}

		for _, p := range pendingIn {
			op := s.Operations[p.OpID]
			if op == nil {
				continue
			}
			var mm *Machine
			for i := range machines {
				if machines[i].ID == op.MachineID {
					mm = &machines[i]
					break
				}
			}
			switch op.State {
			case OpStateDraining:
				if mm == nil {
					op.State = OpStateDeleted
					op.UpdatedAtMs = nowMs
				} else if mm.State == "stopped" {
					op.State = OpStateStopped
					op.UpdatedAtMs = nowMs
				} else {
					op.State = OpStateDeferred
					op.LastError = "controller restarted while draining; deferred for safe retry"
					op.UpdatedAtMs = nowMs
				}
			case OpStateFlushed:
				if mm == nil {
					op.State = OpStateDeleted
					op.UpdatedAtMs = nowMs
				} else if mm.State == "stopped" {
					op.State = OpStateStopped
					op.UpdatedAtMs = nowMs
				}
			case OpStateStopped:
				if mm == nil {
					op.State = OpStateDeleted
					op.UpdatedAtMs = nowMs
				}
			}
		}

		for _, m := range machines {
			volID := ""
			if len(m.Config.Mounts) > 0 {
				volID = m.Config.Mounts[0].Volume
			}
			role := m.Config.Metadata["air_role"]
			if m.ID == baselineID {
				role = "baseline"
			} else if role == "" {
				role = "burst"
			}
			vmSize := fmt.Sprintf("%s-%d", m.Config.Guest.CPUKind, m.Config.Guest.CPUs)
			existing := s.MachineVolumes[m.ID]
			createdAt := nowMs
			if existing != nil && existing.CreatedAtMs > 0 {
				createdAt = existing.CreatedAtMs
			}
			s.MachineVolumes[m.ID] = &MachineVolume{
				MachineID:   m.ID,
				VolumeID:    volID,
				Role:        role,
				Region:      m.Region,
				SizeGB:      c.cfg.BurstVolumeSizeGB,
				VMSize:      vmSize,
				State:       m.State,
				CreatedAtMs: createdAt,
				UpdatedAtMs: nowMs,
			}
		}
	})
	return baselineID, err
}

type aggregatedMetrics struct {
	ActiveConns int64
	MaxConns    int
	OK          bool
	Err         string
}

func (c *Controller) collectGatewayMetrics(ctx context.Context, activeMachines []Machine) aggregatedMetrics {
	if len(activeMachines) == 0 {
		return aggregatedMetrics{OK: false, Err: "no active gateway machines"}
	}
	var totalActive int64
	var totalMax int
	for _, m := range activeMachines {
		gm, err := c.prober.FetchMetrics(ctx, m)
		if err != nil {
			return aggregatedMetrics{OK: false, Err: fmt.Sprintf("metrics fetch failed for %s: %v", m.ID, err)}
		}
		if gm.Limits.MaxClientConns <= 0 {
			return aggregatedMetrics{OK: false, Err: fmt.Sprintf("invalid max_client_conns=%d on %s", gm.Limits.MaxClientConns, m.ID)}
		}
		totalActive += gm.Global.ActiveConnections
		totalMax += gm.Limits.MaxClientConns
	}
	if totalMax <= 0 {
		return aggregatedMetrics{OK: false, Err: "aggregate max_client_conns <= 0"}
	}
	return aggregatedMetrics{
		ActiveConns: totalActive,
		MaxConns:    totalMax,
		OK:          true,
	}
}

// HandleTick runs one control loop evaluation and (when in enforce mode) executes scaling actions.
func (c *Controller) HandleTick(ctx context.Context, nowMs int64) (TickResult, error) {
	c.mu.Lock()
	if c.inTick {
		c.mu.Unlock()
		return TickResult{Action: "skipped_concurrent_tick", Mode: c.cfg.Mode}, nil
	}
	c.inTick = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.inTick = false
		c.mu.Unlock()
	}()

	machines, err := c.fly.ListMachines(ctx)
	if err != nil {
		c.setLastError(err.Error())
		return TickResult{Action: "error_fly_inventory", Mode: c.cfg.Mode, Reason: err.Error()}, err
	}
	volumes, err := c.fly.ListVolumes(ctx)
	if err != nil {
		c.setLastError(err.Error())
		return TickResult{Action: "error_fly_inventory", Mode: c.cfg.Mode, Reason: err.Error()}, err
	}

	baselineID, err := c.reconcileInventory(machines, volumes, nowMs)
	if err != nil {
		c.setLastError(err.Error())
		return TickResult{Action: "error_reconcile", Mode: c.cfg.Mode, Reason: err.Error()}, err
	}
	c.mu.Lock()
	c.lastReconcileAtMs = nowMs
	c.lastError = ""
	c.mu.Unlock()

	var activeMachines []Machine
	var burstMachines []Machine
	for _, m := range machines {
		if m.State == "started" || m.State == "starting" {
			activeMachines = append(activeMachines, m)
			if m.ID != baselineID {
				burstMachines = append(burstMachines, m)
			}
		}
	}

	metrics := c.collectGatewayMetrics(ctx, activeMachines)
	if !metrics.OK {
		st := c.store.Snapshot()
		res := TickResult{
			Action:         "paused_missing_metrics",
			Mode:           c.cfg.Mode,
			ActiveMachines: len(activeMachines),
			BaselineID:     baselineID,
			HighTicks:      st.SustainedHighTicks,
			LowTicks:       st.SustainedLowTicks,
			Reason:         metrics.Err,
		}
		c.recordTickResult(res)
		return res, nil
	}

	utilPct := (float64(metrics.ActiveConns) / float64(metrics.MaxConns)) * 100.0
	spareConns := max(0, metrics.MaxConns-int(metrics.ActiveConns))
	isHighLoad := utilPct >= c.cfg.HighConnUtilPct || spareConns < c.cfg.MinSpareConns
	isLowLoad := utilPct <= c.cfg.LowConnUtilPct && spareConns >= c.cfg.MinSpareConns*2

	var highTicks, lowTicks int
	var lastOutMs, lastInMs int64
	if err := c.store.Update(func(s *PersistentState) {
		if isHighLoad {
			s.SustainedHighTicks++
		} else {
			s.SustainedHighTicks = 0
		}
		if isLowLoad {
			s.SustainedLowTicks++
		} else {
			s.SustainedLowTicks = 0
		}
		highTicks = s.SustainedHighTicks
		lowTicks = s.SustainedLowTicks
		lastOutMs = s.LastScaleOutAtMs
		lastInMs = s.LastScaleInAtMs
	}); err != nil {
		return TickResult{Action: "error_state_persist", Mode: c.cfg.Mode, Reason: err.Error()}, err
	}

	baseRes := TickResult{
		Mode:           c.cfg.Mode,
		ActiveMachines: len(activeMachines),
		BaselineID:     baselineID,
		ActiveConns:    metrics.ActiveConns,
		MaxConns:       metrics.MaxConns,
		UtilPct:        utilPct,
		SpareConns:     spareConns,
		HighTicks:      highTicks,
		LowTicks:       lowTicks,
	}

	// 1. Check any pending or unresolved scale_out operations.
	pendingOut := c.store.PendingScaleOutOps()
	if len(pendingOut) > 0 {
		op := pendingOut[0]
		if c.cfg.Mode != ModeEnforce || op.State == OpStateUnresolved {
			res := baseRes
			res.Action = "blocked_unresolved_op"
			res.OpID = op.OpID
			res.Reason = op.LastError
			c.recordTickResult(res)
			return res, nil
		}
		res, err := c.continueScaleOut(ctx, op, machines, volumes, baseRes, nowMs)
		c.recordTickResult(res)
		return res, err
	}

	// 2. Evaluate Scale-Out (auto-add).
	scaleOutCooldownOK := lastOutMs == 0 || (nowMs-lastOutMs) >= c.cfg.ScaleOutCooldown.Milliseconds()
	if c.cfg.AutoAddEnabled && highTicks >= c.cfg.SustainedHighTicks && len(activeMachines) < c.cfg.MaxReplicas && scaleOutCooldownOK {
		if c.cfg.Mode != ModeEnforce {
			res := baseRes
			res.Action = "decision_scale_out"
			c.recordTickResult(res)
			return res, nil
		}
		res, err := c.executeScaleOut(ctx, machines, volumes, baselineID, baseRes, nowMs)
		c.recordTickResult(res)
		return res, err
	}

	// 3. Evaluate Scale-In (auto-remove, disabled by default).
	scaleInCooldownOK := lastInMs == 0 || (nowMs-lastInMs) >= c.cfg.ScaleInCooldown.Milliseconds()
	if c.cfg.AutoRemoveEnabled && len(burstMachines) > 0 && lowTicks >= c.cfg.SustainedLowTicks && scaleInCooldownOK {
		if c.cfg.BaselineMachineID == "" {
			res := baseRes
			res.Action = "defer_scale_in_no_baseline_configured"
			c.recordTickResult(res)
			return res, nil
		}
		var baseline *Machine
		for i := range activeMachines {
			if activeMachines[i].ID == c.cfg.BaselineMachineID {
				baseline = &activeMachines[i]
				break
			}
		}
		if baseline == nil {
			res := baseRes
			res.Action = "defer_scale_in_no_healthy_baseline"
			c.recordTickResult(res)
			return res, nil
		}
		targetBurst := burstMachines[0]
		if c.cfg.Mode != ModeEnforce {
			res := baseRes
			res.Action = "decision_scale_in"
			res.MachineID = targetBurst.ID
			c.recordTickResult(res)
			return res, nil
		}
		res, err := c.executeScaleIn(ctx, targetBurst, *baseline, baseRes, nowMs)
		c.recordTickResult(res)
		return res, err
	}

	res := baseRes
	res.Action = "noop"
	c.recordTickResult(res)
	return res, nil
}

func (c *Controller) recordTickResult(res TickResult) {
	c.mu.Lock()
	cp := res
	c.lastTick = &cp
	c.mu.Unlock()
}

func (c *Controller) executeScaleOut(ctx context.Context, machines []Machine, volumes []Volume, baselineID string, baseRes TickResult, nowMs int64) (TickResult, error) {
	opID := newOpID("out", nowMs)
	if err := c.store.Update(func(s *PersistentState) {
		s.Operations[opID] = &Operation{
			OpID:        opID,
			Kind:        OpKindScaleOut,
			State:       OpStateIntent,
			CreatedAtMs: nowMs,
			UpdatedAtMs: nowMs,
			Details: map[string]any{
				"region":    c.cfg.Region,
				"cpus":      c.cfg.BurstCPUs,
				"memory_mb": c.cfg.BurstMemoryMB,
			},
		}
	}); err != nil {
		return baseRes, err
	}

	var vol *Volume
	for i := range volumes {
		v := &volumes[i]
		if v.Region == c.cfg.Region && v.AttachedMachineID == "" && strings.HasPrefix(v.Name, "air_gw_burst_") {
			vol = v
			break
		}
	}
	if vol == nil {
		createdVol, err := c.fly.CreateVolume(ctx, CreateVolumeRequest{
			Name:      "air_gw_burst_" + opID,
			Region:    c.cfg.Region,
			SizeGB:    c.cfg.BurstVolumeSizeGB,
			Encrypted: true,
		})
		if err != nil {
			// Check inventory before marking unresolved in case create succeeded before timeout.
			if freshVols, listErr := c.fly.ListVolumes(ctx); listErr == nil {
				vol = findVolumeForOp(freshVols, &Operation{OpID: opID})
			}
			if vol == nil {
				_ = c.store.Update(func(s *PersistentState) {
					if op := s.Operations[opID]; op != nil {
						op.State = OpStateUnresolved
						op.LastError = err.Error()
						op.UpdatedAtMs = nowMs
					}
				})
				res := baseRes
				res.Action = "scale_out_unresolved"
				res.OpID = opID
				res.Reason = err.Error()
				return res, err
			}
		} else {
			vol = createdVol
		}
	}

	if vol == nil || vol.ID == "" {
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateUnresolved
				op.LastError = "volume creation returned empty id"
				op.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_out_unresolved"
		res.OpID = opID
		return res, nil
	}

	if err := c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateVolumeReady
			op.VolumeID = vol.ID
			op.CreateAttempted = true
			op.UpdatedAtMs = nowMs
		}
	}); err != nil {
		return baseRes, err
	}

	var baseline *Machine
	for i := range machines {
		if machines[i].ID == baselineID {
			baseline = &machines[i]
			break
		}
	}
	req := c.buildBurstMachineRequest(opID, vol.ID, baseline)
	created, err := c.fly.CreateMachine(ctx, req)
	if err != nil {
		// Ambiguous create / timeout: check existing Fly inventory before attempting another create.
		if freshMachines, listErr := c.fly.ListMachines(ctx); listErr == nil {
			if existing := findMachineForOp(freshMachines, &Operation{OpID: opID}); existing != nil {
				created = existing
				err = nil
			}
		}
		if err != nil {
			_ = c.store.Update(func(s *PersistentState) {
				if op := s.Operations[opID]; op != nil {
					op.State = OpStateUnresolved
					op.LastError = err.Error()
					op.UpdatedAtMs = nowMs
				}
			})
			res := baseRes
			res.Action = "scale_out_unresolved"
			res.OpID = opID
			res.Reason = err.Error()
			return res, err
		}
	}

	if created == nil || created.ID == "" {
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateUnresolved
				op.LastError = "machine creation returned empty id"
				op.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_out_unresolved"
		res.OpID = opID
		return res, nil
	}

	_ = c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateMachineCreated
			op.MachineID = created.ID
			op.VolumeID = vol.ID
			op.UpdatedAtMs = nowMs
		}
	})

	return c.verifyAndAdmitBurst(ctx, opID, *created, *vol, baseRes, nowMs)
}

func (c *Controller) buildBurstMachineRequest(opID, volumeID string, baseline *Machine) CreateMachineRequest {
	image := "registry.fly.io/" + c.cfg.AppName + ":latest"
	env := make(map[string]string)
	var services []ServiceConfig
	var files []FileConfig

	if baseline != nil {
		if baseline.Config.Image != "" {
			image = baseline.Config.Image
		}
		for k, v := range baseline.Config.Env {
			env[k] = v
		}
		for _, svc := range baseline.Config.Services {
			cp := svc
			cp.AutoStop = "off"
			f := false
			cp.AutoStart = &f
			services = append(services, cp)
		}
		files = append(files, baseline.Config.Files...)
	}

	env["AIR_BILLING_ENABLED"] = "true"
	env["AIR_METERING_LOG_DIR"] = "/var/lib/air-gateway/metering"
	env["AIR_GATEWAY_CORDONED"] = "true"
	env["AIR_GATEWAY_DISCOVERY_HOST"] = c.cfg.AppName + ".internal"
	env["AIR_GATEWAY_PEER_ADMIN_PORT"] = "9093"
	env["AIR_GATEWAY_PRIVATE_HTTP_ADDR"] = fmt.Sprintf("[::]:%d", c.cfg.GatewayPrivateHTTPPort)

	return CreateMachineRequest{
		Region:     c.cfg.Region,
		SkipLaunch: false,
		Config: MachineConfig{
			Image: image,
			Env:   env,
			Guest: GuestConfig{
				CPUKind:  c.cfg.BurstCPUKind,
				CPUs:     c.cfg.BurstCPUs,
				MemoryMB: c.cfg.BurstMemoryMB,
			},
			Mounts:   []MountConfig{{Volume: volumeID, Path: "/var/lib/air-gateway"}},
			Services: services,
			Files:    files,
			Metadata: map[string]string{
				"air_role":  "burst",
				"air_op_id": opID,
			},
			SkipServiceRegistration: true,
		},
	}
}

func (c *Controller) continueScaleOut(ctx context.Context, op *Operation, machines []Machine, volumes []Volume, baseRes TickResult, nowMs int64) (TickResult, error) {
	mm := findMachineForOp(machines, op)
	mv := findVolumeForOp(volumes, op)
	if mm == nil || mv == nil {
		_ = c.store.Update(func(s *PersistentState) {
			if cur := s.Operations[op.OpID]; cur != nil {
				cur.State = OpStateUnresolved
				cur.LastError = "missing machine or volume during resume"
				cur.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_out_unresolved"
		res.OpID = op.OpID
		return res, nil
	}
	return c.verifyAndAdmitBurst(ctx, op.OpID, *mm, *mv, baseRes, nowMs)
}

func (c *Controller) verifyAndAdmitBurst(ctx context.Context, opID string, machine Machine, vol Volume, baseRes TickResult, nowMs int64) (TickResult, error) {
	subsystemsOK := false
	for attempt := 0; attempt < c.cfg.ReadinessMaxAttempts; attempt++ {
		rep, err := c.prober.CheckReadiness(ctx, machine)
		if err == nil && rep != nil {
			checks := rep.Checks
			if checks["tls"].OK && checks["discovery"].OK && checks["backends"].OK && checks["metering"].OK {
				subsystemsOK = true
				break
			}
		}
		if attempt+1 < c.cfg.ReadinessMaxAttempts {
			if err := c.sleepFn(ctx, c.cfg.ReadinessRetryInterval); err != nil {
				return baseRes, err
			}
		}
	}

	meteringOK := false
	if subsystemsOK {
		ms, err := c.prober.CheckMeteringStatus(ctx, machine)
		if err == nil && ms != nil && ms.Writable && ms.SinkHealthy && !ms.Blocked {
			meteringOK = true
		}
	}

	tlsPGOK := false
	if subsystemsOK && meteringOK {
		if err := c.prober.VerifyPostgresTLS(ctx, machine); err == nil {
			tlsPGOK = true
		}
	}

	if !subsystemsOK || !meteringOK || !tlsPGOK {
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateFailed
				op.LastError = fmt.Sprintf("burst verification failed (subsystems=%v metering=%v tls_pg=%v)", subsystemsOK, meteringOK, tlsPGOK)
				op.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_out_failed_readiness"
		res.OpID = opID
		res.MachineID = machine.ID
		return res, nil
	}

	_ = c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateReadyVerified
			op.UpdatedAtMs = nowMs
		}
	})

	if err := c.prober.Uncordon(ctx, machine); err != nil {
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateFailed
				op.LastError = "uncordon failed: " + err.Error()
				op.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_out_failed_uncordon"
		res.OpID = opID
		res.MachineID = machine.ID
		return res, err
	}

	updatedCfg := machine.Config
	if updatedCfg.Env == nil {
		updatedCfg.Env = make(map[string]string)
	} else {
		envCopy := make(map[string]string, len(updatedCfg.Env))
		for k, v := range updatedCfg.Env {
			envCopy[k] = v
		}
		updatedCfg.Env = envCopy
	}
	updatedCfg.Env["AIR_GATEWAY_CORDONED"] = "false"
	updatedCfg.SkipServiceRegistration = false

	if _, err := c.fly.UpdateMachine(ctx, machine.ID, updatedCfg); err != nil {
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateFailed
				op.LastError = "fly update skip_service_registration=false failed: " + err.Error()
				op.UpdatedAtMs = nowMs
			}
		})
		return baseRes, err
	}

	_ = c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateAdmitted
			op.UpdatedAtMs = nowMs
		}
		s.LastScaleOutAtMs = nowMs
		s.SustainedHighTicks = 0
		s.MachineVolumes[machine.ID] = &MachineVolume{
			MachineID:   machine.ID,
			VolumeID:    vol.ID,
			Role:        "burst",
			Region:      machine.Region,
			SizeGB:      vol.SizeGB,
			VMSize:      fmt.Sprintf("%s-%d", c.cfg.BurstCPUKind, c.cfg.BurstCPUs),
			State:       "admitted",
			CreatedAtMs: nowMs,
			UpdatedAtMs: nowMs,
		}
	})

	res := baseRes
	res.Action = "scale_out_admitted"
	res.OpID = opID
	res.MachineID = machine.ID
	res.RetainedVolumeID = vol.ID
	return res, nil
}

func (c *Controller) executeScaleIn(ctx context.Context, burst Machine, baseline Machine, baseRes TickResult, nowMs int64) (TickResult, error) {
	// Explicit baseline protection: never remove the configured baseline Machine.
	if c.cfg.BaselineMachineID == "" || burst.ID == c.cfg.BaselineMachineID || burst.ID == baseline.ID {
		res := baseRes
		res.Action = "refused_baseline_removal"
		res.MachineID = burst.ID
		return res, nil
	}

	baseReady, err := c.prober.CheckReadiness(ctx, baseline)
	if err != nil || baseReady == nil || !baseReady.Ready {
		res := baseRes
		res.Action = "defer_scale_in_baseline_unhealthy"
		return res, nil
	}

	opID := newOpID("in", nowMs)
	volID := ""
	if len(burst.Config.Mounts) > 0 {
		volID = burst.Config.Mounts[0].Volume
	}
	if err := c.store.Update(func(s *PersistentState) {
		s.Operations[opID] = &Operation{
			OpID:        opID,
			Kind:        OpKindScaleIn,
			State:       OpStateDraining,
			MachineID:   burst.ID,
			VolumeID:    volID,
			CreatedAtMs: nowMs,
			UpdatedAtMs: nowMs,
		}
	}); err != nil {
		return baseRes, err
	}

	// 1. Cordon via Fly skip_service_registration=true + Gateway /internal/v1/cordon.
	cordonedCfg := burst.Config
	cordonedCfg.SkipServiceRegistration = true
	if _, err := c.fly.UpdateMachine(ctx, burst.ID, cordonedCfg); err != nil {
		return baseRes, err
	}
	if err := c.prober.Cordon(ctx, burst); err != nil {
		_ = c.restoreBurstAdmission(ctx, burst)
		return baseRes, err
	}

	// 2. Drain sessions & flush metering, requiring fresh op_id-bound acknowledgement.
	ack, drainErr := c.prober.Drain(ctx, burst, opID, c.cfg.DrainTimeout)
	ackValid := drainErr == nil && ack != nil &&
		ack.OpID == opID &&
		ack.Drained &&
		ack.MeteringConfirmed &&
		ack.PendingBatches == 0 &&
		ack.QuarantinedCount == 0 &&
		ack.LegacyPendingCount == 0 &&
		!ack.Blocked

	if !ackValid {
		c.restoreBurstAdmission(ctx, burst)
		errMsg := "drain or metering confirmation failed"
		if ack != nil && ack.Error != "" {
			errMsg = ack.Error
		} else if drainErr != nil {
			errMsg = drainErr.Error()
		}
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateDeferred
				op.LastError = errMsg
				op.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_in_deferred"
		res.OpID = opID
		res.MachineID = burst.ID
		res.Reason = errMsg
		return res, nil
	}

	_ = c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateFlushed
			op.UpdatedAtMs = nowMs
		}
	})

	// 3. Stop Machine and confirm it is stopped before deletion.
	if err := c.fly.StopMachine(ctx, burst.ID); err != nil {
		return baseRes, err
	}
	var inspected *Machine
	for attempt := 0; attempt < 10; attempt++ {
		m, err := c.fly.GetMachine(ctx, burst.ID)
		if err == nil && m != nil {
			inspected = m
			if m.State == "stopped" {
				break
			}
		}
		if attempt+1 < 10 {
			retryDelay := min(c.cfg.ReadinessRetryInterval, time.Second)
			if err := c.sleepFn(ctx, retryDelay); err != nil {
				return baseRes, err
			}
		}
	}
	if inspected == nil || inspected.State != "stopped" {
		stateStr := "unknown"
		if inspected != nil {
			stateStr = inspected.State
		}
		_ = c.store.Update(func(s *PersistentState) {
			if op := s.Operations[opID]; op != nil {
				op.State = OpStateDeferred
				op.LastError = fmt.Sprintf("machine state %s != stopped", stateStr)
				op.UpdatedAtMs = nowMs
			}
		})
		res := baseRes
		res.Action = "scale_in_deferred_not_stopped"
		res.OpID = opID
		res.MachineID = burst.ID
		return res, nil
	}

	_ = c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateStopped
			op.UpdatedAtMs = nowMs
		}
	})

	// 4. Delete ONLY the stopped burst Machine and RETAIN its volume.
	if err := c.fly.DeleteMachine(ctx, burst.ID); err != nil {
		return baseRes, err
	}

	_ = c.store.Update(func(s *PersistentState) {
		if op := s.Operations[opID]; op != nil {
			op.State = OpStateDeleted
			op.UpdatedAtMs = nowMs
		}
		s.LastScaleInAtMs = nowMs
		s.SustainedLowTicks = 0
		if volID != "" {
			s.MachineVolumes[burst.ID] = &MachineVolume{
				MachineID:   burst.ID,
				VolumeID:    volID,
				Role:        "burst",
				Region:      burst.Region,
				SizeGB:      c.cfg.BurstVolumeSizeGB,
				VMSize:      fmt.Sprintf("%s-%d", c.cfg.BurstCPUKind, c.cfg.BurstCPUs),
				State:       "retained_detached",
				CreatedAtMs: nowMs,
				UpdatedAtMs: nowMs,
			}
		}
	})

	res := baseRes
	res.Action = "scale_in_deleted_machine_retained_volume"
	res.OpID = opID
	res.MachineID = burst.ID
	res.RetainedVolumeID = volID
	return res, nil
}

func (c *Controller) restoreBurstAdmission(ctx context.Context, burst Machine) error {
	if err := c.prober.Uncordon(ctx, burst); err != nil {
		return err
	}
	restoredCfg := burst.Config
	restoredCfg.SkipServiceRegistration = false
	_, err := c.fly.UpdateMachine(ctx, burst.ID, restoredCfg)
	return err
}
