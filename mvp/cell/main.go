package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	startTime = time.Now()
	volDir    = envOr("VOL_DIR", "/vol")
	pgBin     = envOr("PG_BIN", "/usr/lib/postgresql/17/bin")
	r2Bucket  = envOr("R2_BUCKET", "air-mvp-test")
	validID   = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)
	portMu    sync.Mutex
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	if err := os.MkdirAll(filepath.Join(volDir, "tenants"), 0o755); err != nil {
		log.Fatalf("mkdir tenants: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(volDir, "tmp"), 0o777); err != nil {
		log.Fatalf("mkdir tmp: %v", err)
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", volDir).Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/envprobe", handleEnvProbe)
	mux.HandleFunc("/tenant/create", handleTenantCreate)
	mux.HandleFunc("/tenant/query", handleTenantQuery)
	mux.HandleFunc("/tenant/sleep", handleTenantSleep)
	mux.HandleFunc("/tenant/backup", handleTenantBackup)
	mux.HandleFunc("/tenant/restore", handleTenantRestore)
	mux.HandleFunc("/metrics", handleMetrics)

	srv := &http.Server{
		Addr:              ":" + envOr("PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("air-mvp-cell listening on %s (vol=%s)", srv.Addr, volDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-stop
	log.Println("SIGTERM received: draining HTTP and stopping active tenant postgres clusters...")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	stopAllTenants()
	log.Println("clean shutdown complete")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	memPct, memTotalMB, memAvailMB := readMemStats()
	diskPct, diskTotalGB, diskFreeGB := readDiskStats(volDir)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"machine_id":     os.Getenv("FLY_MACHINE_ID"),
		"region":         os.Getenv("FLY_REGION"),
		"uptime_s":       int(time.Since(startTime).Seconds()),
		"ram_total_mb":   memTotalMB,
		"ram_avail_mb":   memAvailMB,
		"ram_used_pct":   memPct,
		"disk_total_gb":  diskTotalGB,
		"disk_free_gb":   diskFreeGB,
		"disk_used_pct":  diskPct,
		"active_tenants": countActiveTenants(),
	})
}

func handleEnvProbe(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	out["kernel"] = runCmdOut("uname", "-a")
	out["swaps"] = runCmdOut("cat", "/proc/swaps")
	out["vol_fstype"] = runCmdOut("stat", "-f", "-c", "%T", volDir)
	out["vol_mount"] = runCmdOut("sh", "-c", fmt.Sprintf("grep ' %s ' /proc/mounts || true", volDir))
	out["cgroups_v2_controllers"] = runCmdOut("cat", "/sys/fs/cgroup/cgroup.controllers")
	out["dev_shm"] = runCmdOut("df", "-h", "/dev/shm")
	out["dev_loop"] = runCmdOut("sh", "-c", "ls -la /dev/loop* 2>&1 | head -n 5")
	out["dev_watchdog"] = runCmdOut("sh", "-c", "ls -la /dev/watchdog* 2>&1 || true")
	out["unshare_ns_test"] = runCmdOut("sh", "-c", "unshare --mount --pid --fork --net echo NS_OK 2>&1")
	out["fsync_4kb_100x"] = benchFsync(filepath.Join(volDir, "tmp", "fsync_probe.bin"), 100)
	writeJSON(w, http.StatusOK, out)
}

func benchFsync(path string, n int) map[string]any {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(path)
	}()
	buf := make([]byte, 4096)
	durs := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if _, err := f.WriteAt(buf, 0); err != nil {
			return map[string]any{"error": err.Error()}
		}
		if err := f.Sync(); err != nil {
			return map[string]any{"error": err.Error()}
		}
		durs = append(durs, float64(time.Since(t0).Microseconds())/1000.0)
	}
	sort.Float64s(durs)
	return map[string]any{
		"samples": n,
		"p50_ms":  durs[n/2],
		"p95_ms":  durs[(n*95)/100],
		"max_ms":  durs[n-1],
	}
}

func tenantPaths(id string) (base, data, sock, portFile string) {
	base = filepath.Join(volDir, "tenants", id)
	data = filepath.Join(base, "data")
	sock = filepath.Join(base, "sock")
	portFile = filepath.Join(base, "port")
	return
}

func allocTenantPort(id string) (int, error) {
	portMu.Lock()
	defer portMu.Unlock()
	_, _, _, portFile := tenantPaths(id)
	if b, err := os.ReadFile(portFile); err == nil {
		if p, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
			return p, nil
		}
	}
	// Deterministic hash port in 15432..16431 range, avoiding collisions
	h := 0
	for _, c := range id {
		h = (h*31 + int(c)) % 1000
	}
	p := 15432 + h
	_ = os.WriteFile(portFile, []byte(strconv.Itoa(p)), 0o644)
	return p, nil
}

func runAsPostgres(cmd string) (string, error) {
	c := exec.Command("su", "-s", "/bin/sh", "postgres", "-c", cmd)
	c.Env = append(os.Environ(), "TMPDIR="+filepath.Join(volDir, "tmp"))
	var out bytes.Buffer
	c.Stdout = &out
	c.Stderr = &out
	err := c.Run()
	return strings.TrimSpace(out.String()), err
}

func isTenantRunning(dataDir string) bool {
	_, err := runAsPostgres(fmt.Sprintf("%s/pg_ctl -D %s status", pgBin, dataDir))
	return err == nil
}

func startTenantPG(id string) (int64, int, error) {
	_, data, sock, _ := tenantPaths(id)
	port, err := allocTenantPort(id)
	if err != nil {
		return 0, 0, err
	}
	if isTenantRunning(data) {
		return 0, port, nil
	}
	t0 := time.Now()
	_ = os.MkdirAll(sock, 0o755)
	_ = exec.Command("chown", "-R", "postgres:postgres", filepath.Dir(data)).Run()
	cmd := fmt.Sprintf("%s/pg_ctl -D %s -l %s/postgres.log -w -t 30 start", pgBin, data, filepath.Dir(data))
	out, err := runAsPostgres(cmd)
	if err != nil {
		return 0, port, fmt.Errorf("pg_ctl start failed: %w (%s)", err, out)
	}
	return time.Since(t0).Milliseconds(), port, nil
}

func handleTenantCreate(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if !validID.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id (alphanumeric/_/- max 32)"})
		return
	}
	base, data, sock, _ := tenantPaths(id)
	if _, err := os.Stat(filepath.Join(data, "PG_VERSION")); err == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant already exists"})
		return
	}
	if err := os.MkdirAll(sock, 0o755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	port, _ := allocTenantPort(id)
	_ = exec.Command("chown", "-R", "postgres:postgres", base).Run()

	t0 := time.Now()
	initCmd := fmt.Sprintf("%s/initdb -D %s -U postgres --auth=trust --no-sync", pgBin, data)
	if out, err := runAsPostgres(initCmd); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error(), "out": out})
		return
	}
	conf := fmt.Sprintf(`
listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 10
shared_buffers = '8MB'
work_mem = '2MB'
maintenance_work_mem = '16MB'
max_parallel_workers_per_gather = 0
summarize_wal = on
wal_level = replica
max_wal_senders = 4
logging_collector = off
`, port, sock)
	if err := os.WriteFile(filepath.Join(data, "postgresql.conf"), []byte(conf), 0o600); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", base).Run()
	initMs := time.Since(t0).Milliseconds()

	wakeMs, _, err := startTenantPG(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	res, err := runAsPostgres(fmt.Sprintf("%s/psql -h %s -p %d -U postgres -d postgres -t -A -c \"SELECT version(), pg_current_wal_lsn();\"", pgBin, sock, port))
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": id,
		"port":      port,
		"initdb_ms": initMs,
		"start_ms":  wakeMs,
		"probe":     res,
		"probe_err": errStr(err),
	})
}

func handleTenantQuery(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	sql := r.URL.Query().Get("sql")
	if sql == "" {
		sql = "SELECT now(), current_database();"
	}
	if !validID.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	_, data, sock, _ := tenantPaths(id)
	if _, err := os.Stat(filepath.Join(data, "PG_VERSION")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}
	wakeMs, port, err := startTenantPG(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	t0 := time.Now()
	cmd := fmt.Sprintf("%s/psql -h %s -p %d -U postgres -d postgres -t -A -c %q", pgBin, sock, port, sql)
	out, qErr := runAsPostgres(cmd)
	queryMs := time.Since(t0).Milliseconds()
	code := http.StatusOK
	if qErr != nil {
		code = http.StatusBadRequest
	}
	writeJSON(w, code, map[string]any{
		"tenant_id": id,
		"wake_ms":   wakeMs,
		"query_ms":  queryMs,
		"result":    out,
		"error":     errStr(qErr),
	})
}

func handleTenantSleep(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if !validID.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	_, data, _, _ := tenantPaths(id)
	t0 := time.Now()
	out, err := runAsPostgres(fmt.Sprintf("%s/pg_ctl -D %s -m smart -w -t 25 stop", pgBin, data))
	sleepMs := time.Since(t0).Milliseconds()
	ctrlOut, _ := runAsPostgres(fmt.Sprintf("%s/pg_controldata %s | grep 'Database cluster state'", pgBin, data))
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id":     id,
		"sleep_ms":      sleepMs,
		"pg_ctl_out":    out,
		"cluster_state": ctrlOut,
		"error":         errStr(err),
	})
}

func getR2Client(ctx context.Context) (*s3.Client, error) {
	ak := strings.Trim(os.Getenv("ACCESS_KEY"), "\"' ")
	sk := strings.Trim(os.Getenv("SECRET_KEY"), "\"' ")
	ep := strings.Trim(os.Getenv("ENDPOINT"), "\"' ")
	if ak == "" || sk == "" || ep == "" {
		return nil, errors.New("missing ACCESS_KEY, SECRET_KEY, or ENDPOINT env vars")
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ep)
		o.UsePathStyle = true
	}), nil
}

func handleTenantBackup(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if !validID.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	wakeMs, port, err := startTenantPG(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	base, _, sock, _ := tenantPaths(id)
	backupDir := filepath.Join(volDir, "tmp", "bkp_"+id)
	_ = os.RemoveAll(backupDir)

	t0 := time.Now()
	cmd := fmt.Sprintf("%s/pg_basebackup -h %s -p %d -U postgres -D %s --checkpoint=fast", pgBin, sock, port, backupDir)
	if out, err := runAsPostgres(cmd); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error(), "out": out})
		return
	}
	backupMs := time.Since(t0).Milliseconds()

	verifyOut, verifyErr := runAsPostgres(fmt.Sprintf("%s/pg_verifybackup %s", pgBin, backupDir))
	tarPath := filepath.Join(base, "latest_backup.tar.gz")
	if out, err := exec.Command("tar", "-czf", tarPath, "-C", backupDir, ".").CombinedOutput(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error(), "out": string(out)})
		return
	}
	_ = os.RemoveAll(backupDir)

	dataBytes, err := os.ReadFile(tarPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s3c, err := getR2Client(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	key := fmt.Sprintf("mvp/%s/%d.tar.gz", id, time.Now().Unix())
	tUp := time.Now()
	_, err = s3c.PutObject(r.Context(), &s3.PutObjectInput{
		Bucket:      aws.String(r2Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(dataBytes),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "r2 put failed: " + err.Error()})
		return
	}
	uploadMs := time.Since(tUp).Milliseconds()

	// Verify R2 conditional PutObject fence (EXP-D4): second identical key MUST fail with 412!
	_, dupErr := s3c.PutObject(r.Context(), &s3.PutObjectInput{
		Bucket:      aws.String(r2Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader([]byte("overwrite-attempt")),
		IfNoneMatch: aws.String("*"),
	})

	_ = os.WriteFile(filepath.Join(base, "last_r2_key"), []byte(key), 0o644)

	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id":            id,
		"wake_ms":              wakeMs,
		"pg_basebackup_ms":     backupMs,
		"pg_verifybackup":      verifyOut,
		"pg_verifybackup_err":  errStr(verifyErr),
		"archive_bytes":        len(dataBytes),
		"r2_bucket":            r2Bucket,
		"r2_key":               key,
		"r2_upload_ms":         uploadMs,
		"if_none_match_fenced": dupErr != nil,
		"if_none_match_err":    errStr(dupErr),
	})
}

func handleTenantRestore(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if !validID.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	base, _, _, _ := tenantPaths(id)
	keyBytes, err := os.ReadFile(filepath.Join(base, "last_r2_key"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no backup recorded yet; call /tenant/backup first"})
		return
	}
	key := strings.TrimSpace(string(keyBytes))
	s3c, err := getR2Client(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	t0 := time.Now()
	obj, err := s3c.GetObject(r.Context(), &s3.GetObjectInput{
		Bucket: aws.String(r2Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer obj.Body.Close()
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	dlMs := time.Since(t0).Milliseconds()

	restoreDir := filepath.Join(volDir, "tmp", "restore_"+id)
	_ = os.RemoveAll(restoreDir)
	_ = os.MkdirAll(restoreDir, 0o700)
	tarTmp := filepath.Join(volDir, "tmp", "restore_"+id+".tar.gz")
	_ = os.WriteFile(tarTmp, body, 0o600)
	if out, err := exec.Command("tar", "-xzf", tarTmp, "-C", restoreDir).CombinedOutput(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error(), "out": string(out)})
		return
	}
	_ = os.Remove(tarTmp)
	_ = exec.Command("chown", "-R", "postgres:postgres", restoreDir).Run()
	_ = os.Chmod(restoreDir, 0o700)

	verifyOut, verifyErr := runAsPostgres(fmt.Sprintf("%s/pg_verifybackup %s", pgBin, restoreDir))
	_ = os.RemoveAll(restoreDir)

	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id":           id,
		"r2_key":              key,
		"r2_download_ms":      dlMs,
		"bytes":               len(body),
		"pg_verifybackup":     verifyOut,
		"pg_verifybackup_err": errStr(verifyErr),
	})
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	memPct, _, _ := readMemStats()
	diskPct, _, _ := readDiskStats(volDir)
	active := countActiveTenants()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP air_cell_ram_used_pct Percent of Cell RAM used\n# TYPE air_cell_ram_used_pct gauge\nair_cell_ram_used_pct %.2f\n", memPct)
	fmt.Fprintf(w, "# HELP air_cell_disk_used_pct Percent of /vol disk used\n# TYPE air_cell_disk_used_pct gauge\nair_cell_disk_used_pct %.2f\n", diskPct)
	fmt.Fprintf(w, "# HELP air_cell_active_tenants Number of running PG clusters on Cell\n# TYPE air_cell_active_tenants gauge\nair_cell_active_tenants %d\n", active)
}

func countActiveTenants() int {
	entries, err := os.ReadDir(filepath.Join(volDir, "tenants"))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			pidFile := filepath.Join(volDir, "tenants", e.Name(), "data", "postmaster.pid")
			if _, err := os.Stat(pidFile); err == nil {
				n++
			}
		}
	}
	return n
}

func stopAllTenants() {
	entries, err := os.ReadDir(filepath.Join(volDir, "tenants"))
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			data := filepath.Join(volDir, "tenants", e.Name(), "data")
			if isTenantRunning(data) {
				_, _ = runAsPostgres(fmt.Sprintf("%s/pg_ctl -D %s -m smart -w -t 20 stop", pgBin, data))
			}
		}
	}
}

func runCmdOut(name string, args ...string) string {
	out, err := exec.Command(name, args...).CombinedOutput()
	s := strings.TrimSpace(string(out))
	if err != nil && s == "" {
		return err.Error()
	}
	return s
}

func readMemStats() (pct float64, totalMB int64, availMB int64) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, 0
	}
	var totalKB, availKB int64
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			totalKB = v
		case "MemAvailable:":
			availKB = v
		}
	}
	if totalKB > 0 {
		pct = float64(totalKB-availKB) * 100.0 / float64(totalKB)
	}
	return pct, totalKB / 1024, availKB / 1024
}

func readDiskStats(path string) (pct float64, totalGB float64, freeGB float64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, 0
	}
	total := float64(st.Blocks) * float64(st.Bsize)
	free := float64(st.Bavail) * float64(st.Bsize)
	if total > 0 {
		pct = (total - free) * 100.0 / total
	}
	return pct, total / (1024 * 1024 * 1024), free / (1024 * 1024 * 1024)
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
