package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	startTime = time.Now()
	cellURL   = strings.TrimRight(envOr("CELL_URL", "http://air-mvp-cell.flycast"), "/")
	cellApp   = envOr("CELL_APP", "air-mvp-cell")
	httpCli   = &http.Client{Timeout: 45 * time.Second}
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"role":       "24x7-gateway",
			"status":     "ok",
			"machine_id": os.Getenv("FLY_MACHINE_ID"),
			"region":     os.Getenv("FLY_REGION"),
			"uptime_s":   int(time.Since(startTime).Seconds()),
			"cell_url":   cellURL,
		})
	})

	// Proxies /cell/<path> -> <CELL_URL>/<path> over Flycast so Fly Proxy auto-resumes/wakes the Cell!
	mux.HandleFunc("/cell/", func(w http.ResponseWriter, r *http.Request) {
		targetPath := strings.TrimPrefix(r.URL.Path, "/cell")
		if r.URL.RawQuery != "" {
			targetPath += "?" + r.URL.RawQuery
		}
		target := cellURL + targetPath
		req, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		t0 := time.Now()
		resp, err := httpCli.Do(req)
		rttMs := time.Since(t0).Milliseconds()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error":      err.Error(),
				"target":     target,
				"e2e_rtt_ms": rttMs,
			})
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.Header().Set("X-Gateway-E2E-Ms", fmt.Sprintf("%d", rttMs))
		w.Header().Set("X-Gateway-Machine", os.Getenv("FLY_MACHINE_ID"))
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	})

	// Checks Fly Machines API for air-mvp-cell state (started/suspended/stopped) without waking it!
	mux.HandleFunc("/cell-state", func(w http.ResponseWriter, r *http.Request) {
		tok := os.Getenv("FLY_API_TOKEN")
		if tok == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "FLY_API_TOKEN not set on gateway"})
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet,
			fmt.Sprintf("https://api.machines.dev/v1/apps/%s/machines", cellApp), nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := httpCli.Do(req)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	})

	addr := ":" + envOr("PORT", "8080")
	log.Printf("air-mvp-gateway (24/7 256MB) listening on %s -> %s", addr, cellURL)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
