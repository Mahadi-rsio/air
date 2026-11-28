package autoscaler

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockFly struct {
	mu                sync.Mutex
	machines          map[string]Machine
	volumes           map[string]Volume
	createMachineErr  error
	createMachineLeak bool // simulates timeout after Fly actually created the machine
	createCalls       int
	volumeCalls       int
	stopCalls         int
	deleteCalls       int
	updateCalls       int
	stayStarting      bool
	nextSeq           int
}

func newMockFly(machines []Machine, volumes []Volume) *mockFly {
	mf := &mockFly{
		machines: make(map[string]Machine),
		volumes:  make(map[string]Volume),
	}
	for _, m := range machines {
		mf.machines[m.ID] = m
	}
	for _, v := range volumes {
		mf.volumes[v.ID] = v
	}
	return mf
}

func (f *mockFly) ListMachines(_ context.Context) ([]Machine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Machine, 0, len(f.machines))
	for _, m := range f.machines {
		out = append(out, m)
	}
	return out, nil
}

func (f *mockFly) GetMachine(_ context.Context, id string) (*Machine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.machines[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return &m, nil
}

func (f *mockFly) CreateMachine(_ context.Context, req CreateMachineRequest) (*Machine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.createMachineErr != nil && !f.createMachineLeak {
		return nil, f.createMachineErr
	}
	f.nextSeq++
	id := "m_burst_" + strconv.Itoa(f.nextSeq)
	m := Machine{
		ID:        id,
		Region:    req.Region,
		State:     "started",
		PrivateIP: "fdaa:0:1::" + strconv.Itoa(10+f.nextSeq),
		CreatedAt: "2026-11-28T00:00:00Z",
		Config:    req.Config,
	}
	if len(req.Config.Mounts) > 0 {
		volID := req.Config.Mounts[0].Volume
		if v, ok := f.volumes[volID]; ok {
			v.AttachedMachineID = id
			f.volumes[volID] = v
		}
	}
	f.machines[id] = m
	if f.createMachineLeak && f.createMachineErr != nil {
		return nil, f.createMachineErr
	}
	return &m, nil
}

func (f *mockFly) UpdateMachine(_ context.Context, id string, cfg MachineConfig) (*Machine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls++
	m, ok := f.machines[id]
	if !ok {
		return nil, errors.New("not found")
	}
	m.Config = cfg
	f.machines[id] = m
	return &m, nil
}

func (f *mockFly) StopMachine(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopCalls++
	m, ok := f.machines[id]
	if !ok {
		return errors.New("not found")
	}
	if !f.stayStarting {
		m.State = "stopped"
	}
	f.machines[id] = m
	return nil
}

func (f *mockFly) DeleteMachine(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	m, ok := f.machines[id]
	if !ok {
		return errors.New("not found")
	}
	if len(m.Config.Mounts) > 0 {
		volID := m.Config.Mounts[0].Volume
		if v, ok := f.volumes[volID]; ok {
			v.AttachedMachineID = ""
			f.volumes[volID] = v
		}
	}
	delete(f.machines, id)
	return nil
}

func (f *mockFly) ListVolumes(_ context.Context) ([]Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Volume, 0, len(f.volumes))
	for _, v := range f.volumes {
		out = append(out, v)
	}
	return out, nil
}

func (f *mockFly) CreateVolume(_ context.Context, req CreateVolumeRequest) (*Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeCalls++
	f.nextSeq++
	id := "vol_burst_" + strconv.Itoa(f.nextSeq)
	v := Volume{
		ID:     id,
		Name:   req.Name,
		Region: req.Region,
		SizeGB: req.SizeGB,
	}
	f.volumes[id] = v
	return &v, nil
}

type mockProber struct {
	mu              sync.Mutex
	activeConns     int64
	maxConns        int
	metricsErr      error
	readinessByID   map[string]*ReadinessReport
	meteringOK      bool
	tlsErr          error
	drainResultFn   func(m Machine, opID string) (*DrainResult, error)
	cordonCalls     int
	uncordonCalls   int
	drainCalls      int
	tlsVerifyCalls  int
	metricsCalls    int
}

func newMockProber(active int64, maxConns int) *mockProber {
	return &mockProber{
		activeConns:   active,
		maxConns:      maxConns,
		readinessByID: make(map[string]*ReadinessReport),
		meteringOK:    true,
	}
}

func (p *mockProber) FetchMetrics(_ context.Context, _ Machine) (*GatewayMetrics, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metricsCalls++
	if p.metricsErr != nil {
		return nil, p.metricsErr
	}
	gm := &GatewayMetrics{}
	gm.Limits.MaxClientConns = p.maxConns
	gm.Global.ActiveConnections = p.activeConns
	return gm, nil
}

func (p *mockProber) CheckReadiness(_ context.Context, m Machine) (*ReadinessReport, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if rep, ok := p.readinessByID[m.ID]; ok {
		return rep, nil
	}
	return &ReadinessReport{
		Ready:  true,
		Status: "ready",
		Checks: map[string]ReadinessCheck{
			"tls":       {OK: true},
			"discovery": {OK: true},
			"backends":  {OK: true},
			"metering":  {OK: true},
		},
	}, nil
}

func (p *mockProber) CheckMeteringStatus(_ context.Context, m Machine) (*MeteringStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MeteringStatus{
		GatewayID:      m.ID,
		BillingEnabled: true,
		Writable:       p.meteringOK,
		SinkConfigured: true,
		SinkHealthy:    p.meteringOK,
		Blocked:        !p.meteringOK,
	}, nil
}

func (p *mockProber) VerifyPostgresTLS(_ context.Context, _ Machine) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tlsVerifyCalls++
	return p.tlsErr
}

func (p *mockProber) Cordon(_ context.Context, _ Machine) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cordonCalls++
	return nil
}

func (p *mockProber) Uncordon(_ context.Context, _ Machine) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uncordonCalls++
	return nil
}

func (p *mockProber) Drain(_ context.Context, m Machine, opID string, _ time.Duration) (*DrainResult, error) {
	p.mu.Lock()
	p.drainCalls++
	fn := p.drainResultFn
	p.mu.Unlock()
	if fn != nil {
		return fn(m, opID)
	}
	return &DrainResult{
		OpID:              opID,
		GatewayID:         m.ID,
		Drained:           true,
		MeteringConfirmed: true,
	}, nil
}

func baselineFixture() (Machine, Volume) {
	return Machine{
			ID:        "m_baseline",
			Region:    "sin",
			State:     "started",
			PrivateIP: "fdaa:0:1::1",
			CreatedAt: "2026-11-01T00:00:00Z",
			Config: MachineConfig{
				Image:  "registry.fly.io/air-pg-gateway:latest",
				Guest:  GuestConfig{CPUKind: "shared", CPUs: 2, MemoryMB: 512},
				Env:    map[string]string{"AIR_REQUIRE_TLS": "true"},
				Mounts: []MountConfig{{Volume: "vol_base", Path: "/var/lib/air-gateway"}},
				Services: []ServiceConfig{{
					Protocol:     "tcp",
					InternalPort: 5432,
				}},
			},
		}, Volume{
			ID:                "vol_base",
			Name:              "air_gateway_metering",
			Region:            "sin",
			SizeGB:            1,
			AttachedMachineID: "m_baseline",
		}
}

func testConfig(t *testing.T) Config {
	t.Helper()
	c := DefaultConfig()
	c.StateDir = t.TempDir()
	c.BaselineMachineID = "m_baseline"
	c.ReadinessRetryInterval = 0
	c.ScaleOutCooldown = 60 * time.Second
	c.ScaleInCooldown = 60 * time.Second
	return c
}

func TestDecisionOnly_NeverMutatesFlyAndPausesOnMissingMetrics(t *testing.T) {
	baseM, baseV := baselineFixture()
	fly := newMockFly([]Machine{baseM}, []Volume{baseV})
	prober := newMockProber(900, 1024)
	cfg := testConfig(t) // defaults: ModeDecisionOnly, AutoRemoveEnabled=false
	cfg.SustainedHighTicks = 3

	store, err := NewStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctrl := NewController(cfg, store, fly, prober)

	if cfg.Mode != ModeDecisionOnly || cfg.AutoRemoveEnabled {
		t.Fatalf("expected safe defaults decision_only and auto_remove=false, got mode=%s auto_remove=%v", cfg.Mode, cfg.AutoRemoveEnabled)
	}

	// Missing metrics must pause scaling decisions without incrementing sustained counters.
	prober.metricsErr = errors.New("6pn timeout")
	res, err := ctrl.HandleTick(context.Background(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Action != "paused_missing_metrics" {
		t.Fatalf("expected paused_missing_metrics, got %s", res.Action)
	}
	if store.Snapshot().SustainedHighTicks != 0 {
		t.Fatalf("expected sustained_high_ticks=0 when metrics missing")
	}

	// Restore metrics and trigger sustained high load -> decision_only must not mutate Fly.
	prober.metricsErr = nil
	for i := 0; i < 4; i++ {
		res, err = ctrl.HandleTick(context.Background(), int64(10000+i*60000))
		if err != nil {
			t.Fatal(err)
		}
	}
	if res.Action != "decision_scale_out" || res.Mode != ModeDecisionOnly {
		t.Fatalf("expected decision_scale_out in decision_only mode, got %+v", res)
	}
	if fly.createCalls != 0 || fly.volumeCalls != 0 || fly.updateCalls != 0 || fly.deleteCalls != 0 {
		t.Fatalf("expected zero Fly mutations in decision_only mode")
	}
}

func TestScaleOut_VerifyReadinessMeteringAndTLSBeforeAdmission(t *testing.T) {
	baseM, baseV := baselineFixture()
	fly := newMockFly([]Machine{baseM}, []Volume{baseV})
	prober := newMockProber(900, 1024)
	cfg := testConfig(t)
	cfg.Mode = ModeEnforce
	cfg.SustainedHighTicks = 2

	store, err := NewStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctrl := NewController(cfg, store, fly, prober)

	if _, err := ctrl.HandleTick(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	res, err := ctrl.HandleTick(context.Background(), 61000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Action != "scale_out_admitted" {
		t.Fatalf("expected scale_out_admitted, got %+v", res)
	}
	if prober.tlsVerifyCalls != 1 || prober.uncordonCalls != 1 {
		t.Fatalf("expected 1 TLS verify and 1 uncordon, got tls=%d uncordon=%d", prober.tlsVerifyCalls, prober.uncordonCalls)
	}

	machines, _ := fly.ListMachines(context.Background())
	if len(machines) != 2 {
		t.Fatalf("expected 2 machines, got %d", len(machines))
	}
	var burst Machine
	for _, m := range machines {
		if m.ID != "m_baseline" {
			burst = m
		}
	}
	if burst.Config.SkipServiceRegistration {
		t.Fatalf("expected admitted burst to have skip_service_registration=false")
	}
	if burst.Config.Env["AIR_GATEWAY_CORDONED"] != "false" {
		t.Fatalf("expected AIR_GATEWAY_CORDONED=false after admission")
	}
	if len(burst.Config.Mounts) != 1 || burst.Config.Mounts[0].Path != "/var/lib/air-gateway" {
		t.Fatalf("expected metering volume mount on burst, got %+v", burst.Config.Mounts)
	}
}

func TestRestartRecovery_ResumesPendingScaleOutWithoutDuplicateCreate(t *testing.T) {
	baseM, baseV := baselineFixture()
	fly := newMockFly([]Machine{baseM}, []Volume{baseV})
	prober := newMockProber(900, 1024)
	cfg := testConfig(t)
	cfg.Mode = ModeEnforce
	cfg.SustainedHighTicks = 1

	// Simulate an existing burst machine & volume already created before controller crashed.
	opID := "out_5000_abc123"
	vol, _ := fly.CreateVolume(context.Background(), CreateVolumeRequest{
		Name:   "air_gw_burst_" + opID,
		Region: "sin",
		SizeGB: 1,
	})
	burstM, _ := fly.CreateMachine(context.Background(), CreateMachineRequest{
		Region: "sin",
		Config: MachineConfig{
			Image:                   baseM.Config.Image,
			Guest:                   GuestConfig{CPUKind: "shared", CPUs: 4, MemoryMB: 1024},
			Mounts:                  []MountConfig{{Volume: vol.ID, Path: "/var/lib/air-gateway"}},
			Metadata:                map[string]string{"air_role": "burst", "air_op_id": opID},
			SkipServiceRegistration: true,
		},
	})
	fly.createCalls = 0
	fly.volumeCalls = 0

	store1, err := NewStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store1.Update(func(s *PersistentState) {
		s.Operations[opID] = &Operation{
			OpID:        opID,
			Kind:        OpKindScaleOut,
			State:       OpStateIntent,
			CreatedAtMs: 5000,
			UpdatedAtMs: 5000,
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate controller restart with a new Store & Controller reading the same volume.
	store2, err := NewStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctrl2 := NewController(cfg, store2, fly, prober)
	if err := ctrl2.ReconcileOnStartup(context.Background(), 10000); err != nil {
		t.Fatal(err)
	}

	snap := store2.Snapshot()
	if snap.Operations[opID].State != OpStateMachineCreated || snap.Operations[opID].MachineID != burstM.ID {
		t.Fatalf("expected startup reconcile to recover machine_created for %s, got %+v", burstM.ID, snap.Operations[opID])
	}

	res, err := ctrl2.HandleTick(context.Background(), 20000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Action != "scale_out_admitted" || res.MachineID != burstM.ID {
		t.Fatalf("expected resumed scale_out_admitted for %s, got %+v", burstM.ID, res)
	}
	if fly.createCalls != 0 || fly.volumeCalls != 0 {
		t.Fatalf("expected zero duplicate creates after restart recovery, got create=%d vol=%d", fly.createCalls, fly.volumeCalls)
	}
}

func TestAmbiguousCreateTimeout_ChecksInventoryBeforeRetry(t *testing.T) {
	// Case A: Fly actually created the machine before timing out -> controller adopts it, no duplicate create.
	t.Run("timeout_after_fly_created_adopts_existing", func(t *testing.T) {
		baseM, baseV := baselineFixture()
		fly := newMockFly([]Machine{baseM}, []Volume{baseV})
		fly.createMachineErr = context.DeadlineExceeded
		fly.createMachineLeak = true
		prober := newMockProber(900, 1024)
		cfg := testConfig(t)
		cfg.Mode = ModeEnforce
		cfg.SustainedHighTicks = 1

		store, err := NewStore(cfg.StateDir)
		if err != nil {
			t.Fatal(err)
		}
		ctrl := NewController(cfg, store, fly, prober)
		res, err := ctrl.HandleTick(context.Background(), 10000)
		if err != nil {
			t.Fatalf("expected ambiguous timeout to adopt existing machine and succeed, got err=%v", err)
		}
		if res.Action != "scale_out_admitted" {
			t.Fatalf("expected scale_out_admitted, got %+v", res)
		}
		if fly.createCalls != 1 {
			t.Fatalf("expected exactly 1 CreateMachine call, got %d", fly.createCalls)
		}
	})

	// Case B: Fly did NOT create the machine -> marks unresolved and blocks duplicate creates on subsequent ticks.
	t.Run("timeout_without_machine_marks_unresolved_and_blocks_duplicates", func(t *testing.T) {
		baseM, baseV := baselineFixture()
		fly := newMockFly([]Machine{baseM}, []Volume{baseV})
		fly.createMachineErr = context.DeadlineExceeded
		fly.createMachineLeak = false
		prober := newMockProber(900, 1024)
		cfg := testConfig(t)
		cfg.Mode = ModeEnforce
		cfg.SustainedHighTicks = 1

		store, err := NewStore(cfg.StateDir)
		if err != nil {
			t.Fatal(err)
		}
		ctrl := NewController(cfg, store, fly, prober)
		res, _ := ctrl.HandleTick(context.Background(), 10000)
		if res.Action != "scale_out_unresolved" {
			t.Fatalf("expected scale_out_unresolved, got %+v", res)
		}

		// Subsequent tick must stay blocked without creating another volume or machine.
		fly.createMachineErr = nil
		res2, err := ctrl.HandleTick(context.Background(), 70000)
		if err != nil {
			t.Fatal(err)
		}
		if res2.Action != "blocked_unresolved_op" {
			t.Fatalf("expected blocked_unresolved_op, got %+v", res2)
		}
		if fly.volumeCalls != 1 || fly.createCalls != 1 {
			t.Fatalf("expected no duplicate volume/machine creates, got vol=%d create=%d", fly.volumeCalls, fly.createCalls)
		}
	})
}

func TestBaselineProtection_AndReplicaLimitsAndCooldowns(t *testing.T) {
	baseM, baseV := baselineFixture()
	fly := newMockFly([]Machine{baseM}, []Volume{baseV})
	prober := newMockProber(10, 1024) // very low load
	cfg := testConfig(t)
	cfg.Mode = ModeEnforce
	cfg.AutoRemoveEnabled = true
	cfg.SustainedLowTicks = 1

	store, err := NewStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctrl := NewController(cfg, store, fly, prober)

	// Even with low load and auto_remove=true, baseline alone must never be removed.
	res, err := ctrl.HandleTick(context.Background(), 10000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Action != "noop" {
		t.Fatalf("expected noop when only baseline exists, got %+v", res)
	}
	if fly.stopCalls != 0 || fly.deleteCalls != 0 {
		t.Fatalf("baseline machine must never be stopped or deleted")
	}

	// With MaxReplicas=1, high load must not scale out.
	prober.activeConns = 950
	cfg.MaxReplicas = 1
	cfg.SustainedHighTicks = 1
	ctrl2 := NewController(cfg, store, fly, prober)
	res2, err := ctrl2.HandleTick(context.Background(), 70000)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Action != "noop" || fly.createCalls != 0 {
		t.Fatalf("expected MaxReplicas=1 to block scale-out, got %+v", res2)
	}
}

func TestMeteringSafeScaleIn_DeferOnUnconfirmedAndRetainVolumeOnSuccess(t *testing.T) {
	baseM, baseV := baselineFixture()
	burstM := Machine{
		ID:        "m_burst_1",
		Region:    "sin",
		State:     "started",
		PrivateIP: "fdaa:0:1::11",
		CreatedAt: "2026-11-02T00:00:00Z",
		Config: MachineConfig{
			Guest:    GuestConfig{CPUKind: "shared", CPUs: 4, MemoryMB: 1024},
			Mounts:   []MountConfig{{Volume: "vol_burst_1", Path: "/var/lib/air-gateway"}},
			Metadata: map[string]string{"air_role": "burst"},
		},
	}
	burstV := Volume{
		ID:                "vol_burst_1",
		Name:              "air_gw_burst_1",
		Region:            "sin",
		SizeGB:            1,
		AttachedMachineID: "m_burst_1",
	}

	// 1. Unconfirmed metering or wrong op_id defers scale-in and restores admission.
	t.Run("unconfirmed_metering_defers_and_restores_admission", func(t *testing.T) {
		fly := newMockFly([]Machine{baseM, burstM}, []Volume{baseV, burstV})
		prober := newMockProber(20, 1024)
		prober.drainResultFn = func(_ Machine, _ string) (*DrainResult, error) {
			return &DrainResult{
				OpID:              "stale_wrong_op",
				Drained:           true,
				MeteringConfirmed: false,
				PendingBatches:    2,
			}, nil
		}
		cfg := testConfig(t)
		cfg.Mode = ModeEnforce
		cfg.AutoRemoveEnabled = true
		cfg.SustainedLowTicks = 1

		store, _ := NewStore(cfg.StateDir)
		ctrl := NewController(cfg, store, fly, prober)
		res, err := ctrl.HandleTick(context.Background(), 100000)
		if err != nil {
			t.Fatal(err)
		}
		if res.Action != "scale_in_deferred" {
			t.Fatalf("expected scale_in_deferred, got %+v", res)
		}
		if fly.stopCalls != 0 || fly.deleteCalls != 0 {
			t.Fatalf("must not stop or delete machine when metering ack is unconfirmed")
		}
		m, _ := fly.GetMachine(context.Background(), "m_burst_1")
		if m.Config.SkipServiceRegistration {
			t.Fatalf("expected skip_service_registration=false restored after deferred drain")
		}
	})

	// 2. Unhealthy baseline defers scale-in.
	t.Run("unhealthy_baseline_defers_scale_in", func(t *testing.T) {
		fly := newMockFly([]Machine{baseM, burstM}, []Volume{baseV, burstV})
		prober := newMockProber(20, 1024)
		prober.readinessByID["m_baseline"] = &ReadinessReport{Ready: false, Status: "not_ready"}
		cfg := testConfig(t)
		cfg.Mode = ModeEnforce
		cfg.AutoRemoveEnabled = true
		cfg.SustainedLowTicks = 1

		store, _ := NewStore(cfg.StateDir)
		ctrl := NewController(cfg, store, fly, prober)
		res, err := ctrl.HandleTick(context.Background(), 100000)
		if err != nil {
			t.Fatal(err)
		}
		if res.Action != "defer_scale_in_baseline_unhealthy" {
			t.Fatalf("expected defer_scale_in_baseline_unhealthy, got %+v", res)
		}
		if prober.cordonCalls != 0 || fly.deleteCalls != 0 {
			t.Fatalf("must not cordon or delete burst when baseline is unhealthy")
		}
	})

	// 3. Confirmed op-bound drain stops, confirms stopped, deletes ONLY burst Machine, retains volume.
	t.Run("confirmed_drain_deletes_burst_only_and_retains_volume", func(t *testing.T) {
		fly := newMockFly([]Machine{baseM, burstM}, []Volume{baseV, burstV})
		prober := newMockProber(20, 1024)
		cfg := testConfig(t)
		cfg.Mode = ModeEnforce
		cfg.AutoRemoveEnabled = true
		cfg.SustainedLowTicks = 2

		store, _ := NewStore(cfg.StateDir)
		ctrl := NewController(cfg, store, fly, prober)
		if _, err := ctrl.HandleTick(context.Background(), 100000); err != nil {
			t.Fatal(err)
		}
		res, err := ctrl.HandleTick(context.Background(), 160000)
		if err != nil {
			t.Fatal(err)
		}
		if res.Action != "scale_in_deleted_machine_retained_volume" || res.RetainedVolumeID != "vol_burst_1" {
			t.Fatalf("expected scale_in_deleted_machine_retained_volume, got %+v", res)
		}
		machines, _ := fly.ListMachines(context.Background())
		if len(machines) != 1 || machines[0].ID != "m_baseline" {
			t.Fatalf("expected only baseline machine remaining, got %+v", machines)
		}
		vols, _ := fly.ListVolumes(context.Background())
		if len(vols) != 2 {
			t.Fatalf("expected retained burst volume, got %d volumes", len(vols))
		}
		for _, v := range vols {
			if v.ID == "vol_burst_1" && v.AttachedMachineID != "" {
				t.Fatalf("expected retained burst volume to be detached, got %+v", v)
			}
		}
	})
}

func TestNetGatewayProber_VerifyPostgresTLS_CertificateAndHostnameValidation(t *testing.T) {
	// Generate a private CA and a leaf certificate for air-pg-gateway.fly.dev.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "AIR Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "air-pg-gateway.fly.dev"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"air-pg-gateway.fly.dev"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caTmpl, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	tlsCert := tls.Certificate{
		Certificate: [][]byte{leafDER},
		PrivateKey:  leafKey,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				var hdr [8]byte
				if _, err := io.ReadFull(conn, hdr[:]); err != nil {
					return
				}
				if binary.BigEndian.Uint32(hdr[4:8]) != pgSSLRequestCode {
					return
				}
				_, _ = conn.Write([]byte{'S'})
				tc := tls.Server(conn, &tls.Config{
					MinVersion:   tls.VersionTLS12,
					Certificates: []tls.Certificate{tlsCert},
				})
				_ = tc.Handshake()
			}(c)
		}
	}()

	m := Machine{ID: "m_tls", PrivateIP: "127.0.0.1"}

	// Valid CA + matching hostname succeeds.
	validProber := NewNetGatewayProber("", 8082, port, "air-pg-gateway.fly.dev", caPool)
	if err := validProber.VerifyPostgresTLS(context.Background(), m); err != nil {
		t.Fatalf("expected valid PG TLS probe to succeed, got %v", err)
	}

	// Wrong hostname fails validation.
	wrongHostProber := NewNetGatewayProber("", 8082, port, "wrong-host.fly.dev", caPool)
	if err := wrongHostProber.VerifyPostgresTLS(context.Background(), m); err == nil {
		t.Fatalf("expected hostname mismatch to fail PG TLS probe")
	}

	// Untrusted CA fails validation.
	emptyPool := x509.NewCertPool()
	untrustedProber := NewNetGatewayProber("", 8082, port, "air-pg-gateway.fly.dev", emptyPool)
	if err := untrustedProber.VerifyPostgresTLS(context.Background(), m); err == nil {
		t.Fatalf("expected untrusted CA to fail PG TLS probe")
	}
}

func TestHTTPServer_AuthAndStatus(t *testing.T) {
	baseM, baseV := baselineFixture()
	fly := newMockFly([]Machine{baseM}, []Volume{baseV})
	prober := newMockProber(100, 1024)
	cfg := testConfig(t)
	cfg.InternalToken = "secret-token"
	store, _ := NewStore(cfg.StateDir)
	ctrl := NewController(cfg, store, fly, prober)
	srv := NewHTTPServer("127.0.0.1:0", cfg.InternalToken, ctrl)

	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recHealth := httptest.NewRecorder()
	srv.serveHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK || !strings.Contains(recHealth.Body.String(), `"decision_only"`) {
		t.Fatalf("expected 200 decision_only on /healthz, got %d %s", recHealth.Code, recHealth.Body.String())
	}

	reqUnauth := httptest.NewRequest(http.MethodGet, "/status", nil)
	recUnauth := httptest.NewRecorder()
	srv.serveHTTP(recUnauth, reqUnauth)
	if recUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on unauthenticated /status, got %d", recUnauth.Code)
	}

	reqAuth := httptest.NewRequest(http.MethodGet, "/status", nil)
	reqAuth.Header.Set("X-Air-Internal-Token", "secret-token")
	recAuth := httptest.NewRecorder()
	srv.serveHTTP(recAuth, reqAuth)
	if recAuth.Code != http.StatusOK {
		t.Fatalf("expected 200 on authenticated /status, got %d", recAuth.Code)
	}
	var snap StatusSnapshot
	if err := json.Unmarshal(recAuth.Body.Bytes(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Mode != ModeDecisionOnly {
		t.Fatalf("expected decision_only in status, got %s", snap.Mode)
	}
}
