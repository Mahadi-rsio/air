package cell

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCollectTenantTelemetryCgroupV2(t *testing.T) {
	volDir := t.TempDir()
	cgRoot := t.TempDir()

	ag, err := NewAgent(volDir, "", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()
	ag.SetCgroupRoot(cgRoot)

	tid := "t_tel_01"
	cgDir := filepath.Join(cgRoot, "air_tenant_"+tid)
	if err := os.MkdirAll(cgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"memory.current":  "419430400\n",
		"memory.high":     "536870912\n",
		"memory.max":      "1073741824\n",
		"memory.events":   "low 0\nhigh 3\nmax 1\noom 0\noom_kill 0\n",
		"cpu.stat":        "usage_usec 123456\nthrottled_usec 789\n",
		"memory.pressure": "some avg10=12.50 avg60=5.00 avg300=1.00 total=100\nfull avg10=1.25 avg60=0.50 avg300=0.10 total=20\n",
		"cpu.pressure":    "some avg10=8.00 avg60=2.00 avg300=0.50 total=50\n",
		"io.pressure":     "some avg10=4.50 avg60=1.00 avg300=0.20 total=30\nfull avg10=2.00 avg60=0.50 avg300=0.10 total=10\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(cgDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	snap := ag.CollectTenantTelemetry(tid)
	if snap.Source != "cgroup_v2" {
		t.Fatalf("expected source cgroup_v2, got %q", snap.Source)
	}
	if snap.MemoryCurrentBytes != 419430400 || snap.MemoryHighBytes != 536870912 || snap.MemoryMaxBytes != 1073741824 {
		t.Fatalf("unexpected memory bytes: %+v", snap)
	}
	if snap.MemoryHighEvents != 3 || snap.MemoryMaxEvents != 1 || snap.MemoryOOMEvents != 0 {
		t.Fatalf("unexpected memory events: %+v", snap)
	}
	if snap.MemoryPSISomeAvg10 != 12.50 || snap.MemoryPSIFullAvg10 != 1.25 {
		t.Fatalf("unexpected memory PSI: %+v", snap)
	}
	if snap.CPUPSISomeAvg10 != 8.00 || snap.IOPSIMaxAvg10 != 4.50 {
		t.Fatalf("unexpected cpu/io PSI: %+v", snap)
	}
	if snap.CPUUsageUsec != 123456 || snap.CPUThrottledUsec != 789 {
		t.Fatalf("unexpected cpu.stat: %+v", snap)
	}
}

func TestCollectTenantTelemetryMissingDegradesGracefully(t *testing.T) {
	volDir := t.TempDir()
	cgRoot := t.TempDir()

	ag, err := NewAgent(volDir, "", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()
	ag.SetCgroupRoot(cgRoot)

	snap := ag.CollectTenantTelemetry("nonexistent_tenant")
	if snap.Timestamp.IsZero() {
		t.Fatal("expected non-zero timestamp")
	}
	if snap.MemoryCurrentBytes != 0 {
		t.Fatalf("expected 0 memory current, got %d", snap.MemoryCurrentBytes)
	}
}

func TestTelemetryPublisherPushOnce(t *testing.T) {
	volDir := t.TempDir()
	cgRoot := t.TempDir()

	ag, err := NewAgent(volDir, "", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()
	ag.SetCgroupRoot(cgRoot)

	tid := "t_pub_01"
	cgDir := filepath.Join(cgRoot, "air_tenant_"+tid)
	if err := os.MkdirAll(cgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(cgDir, "memory.current"), []byte("104857600\n"), 0o644)
	_ = os.WriteFile(filepath.Join(cgDir, "memory.high"), []byte("450000000\n"), 0o644)
	_ = os.WriteFile(filepath.Join(cgDir, "memory.max"), []byte("536870912\n"), 0o644)

	var gotToken string
	var gotSnap ResourceSnapshot
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/telemetry" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotToken = r.Header.Get("X-Air-Internal-Token")
		_ = json.NewDecoder(r.Body).Decode(&gotSnap)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pub := NewTelemetryPublisher(ag, srv.URL, "secret-tok", tid, 50*time.Millisecond)
	snap, err := pub.PushOnce(context.Background())
	if err != nil {
		t.Fatalf("PushOnce: %v", err)
	}
	if gotToken != "secret-tok" {
		t.Fatalf("expected token secret-tok, got %q", gotToken)
	}
	if gotSnap.MemoryCurrentBytes != 104857600 || snap.MemoryMaxBytes != 536870912 {
		t.Fatalf("unexpected pushed snapshot: %+v", gotSnap)
	}
}
