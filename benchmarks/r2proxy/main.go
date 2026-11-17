package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type FaultConfig struct {
	Mode          string `json:"mode"`           // "", "unavailable", "timeout", "5xx", "drop_layer_put", "drop_wal_put", "corrupt_layer_get"
	TimeoutMs     int    `json:"timeout_ms"`     // delay for timeout mode
	ErrorRate     float64 `json:"error_rate"`    // 0..1 for transient 5xx
	DropAfterN    int64  `json:"drop_after_n"`   // optional counter
}

type OpStats struct {
	Count      int64   `json:"count"`
	BytesIn    int64   `json:"bytes_in"`
	BytesOut   int64   `json:"bytes_out"`
	Errors     int64   `json:"errors"`
	TotalLatMs float64 `json:"total_lat_ms"`
	MaxLatMs   float64 `json:"max_lat_ms"`
}

type ProxyStats struct {
	mu          sync.Mutex
	ByOp        map[string]*OpStats `json:"by_op"`         // GET, PUT, HEAD, LIST, DELETE
	ByCategory  map[string]*OpStats `json:"by_category"`   // metadata, layer, wal, skeleton, other
	RecentErrs  []string            `json:"recent_errs"`
}

func newProxyStats() *ProxyStats {
	return &ProxyStats{
		ByOp:       make(map[string]*OpStats),
		ByCategory: make(map[string]*OpStats),
	}
}

func (s *ProxyStats) record(op, cat string, inBytes, outBytes int64, latMs float64, isErr bool, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range []struct {
		tbl map[string]*OpStats
		k   string
	}{{s.ByOp, op}, {s.ByCategory, cat}} {
		st := m.tbl[m.k]
		if st == nil {
			st = &OpStats{}
			m.tbl[m.k] = st
		}
		st.Count++
		st.BytesIn += inBytes
		st.BytesOut += outBytes
		st.TotalLatMs += latMs
		if latMs > st.MaxLatMs {
			st.MaxLatMs = latMs
		}
		if isErr {
			st.Errors++
		}
	}
	if isErr && errMsg != "" {
		if len(s.RecentErrs) >= 20 {
			s.RecentErrs = s.RecentErrs[1:]
		}
		s.RecentErrs = append(s.RecentErrs, errMsg)
	}
}

func classifyOp(r *http.Request) (op, cat string) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("list-type") || r.URL.Query().Has("prefix") {
			op = "LIST"
		} else {
			op = "GET"
		}
	case http.MethodPut:
		op = "PUT"
	case http.MethodHead:
		op = "HEAD"
	case http.MethodDelete:
		op = "DELETE"
	default:
		op = r.Method
	}
	p := r.URL.Path
	switch {
	case strings.Contains(p, "/layers/"):
		cat = "layer"
	case strings.Contains(p, "/wal/"):
		cat = "wal"
	case strings.Contains(p, "/skeleton/"):
		cat = "skeleton"
	case strings.Contains(p, "manifest") || op == "LIST" || op == "HEAD":
		cat = "metadata"
	default:
		cat = "other"
	}
	return op, cat
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:19000", "Listen address")
	adminAddr := flag.String("admin", "127.0.0.1:19001", "Admin HTTP address (/stats, /reset, /fault)")
	targetEndpoint := flag.String("endpoint", os.Getenv("ENDPOINT"), "Upstream R2 endpoint URL")
	accessKey := flag.String("access-key", os.Getenv("ACCESS_KEY"), "R2 access key")
	secretKey := flag.String("secret-key", os.Getenv("SECRET_KEY"), "R2 secret key")
	flag.Parse()

	if *targetEndpoint == "" || *accessKey == "" || *secretKey == "" {
		log.Fatal("endpoint, access-key, and secret-key are required")
	}
	targetURL, err := url.Parse(*targetEndpoint)
	if err != nil {
		log.Fatalf("invalid endpoint: %v", err)
	}

	signer := v4.NewSigner()
	creds := aws.Credentials{AccessKeyID: *accessKey, SecretAccessKey: *secretKey}
	client := &http.Client{Timeout: 60 * time.Second}

	var statsMu sync.RWMutex
	stats := newProxyStats()
	var faultMu sync.RWMutex
	fault := FaultConfig{}
	var reqCounter atomic.Int64

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		statsMu.RLock()
		st := stats
		statsMu.RUnlock()
		st.mu.Lock()
		defer st.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	adminMux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		statsMu.Lock()
		stats = newProxyStats()
		statsMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	adminMux.HandleFunc("/fault", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var fc FaultConfig
			if err := json.NewDecoder(r.Body).Decode(&fc); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			faultMu.Lock()
			fault = fc
			faultMu.Unlock()
		}
		faultMu.RLock()
		fc := fault
		faultMu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fc)
	})
	go func() {
		log.Printf("r2proxy admin listening on %s", *adminAddr)
		log.Fatal(http.ListenAndServe(*adminAddr, adminMux))
	}()

	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		op, cat := classifyOp(r)
		n := reqCounter.Add(1)

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		inLen := int64(len(bodyBytes))

		faultMu.RLock()
		fc := fault
		faultMu.RUnlock()

		// Fault injection checks
		switch fc.Mode {
		case "unavailable":
			statsMu.RLock()
			stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, fmt.Sprintf("%s %s -> injected 503 unavailable", op, r.URL.Path))
			statsMu.RUnlock()
			http.Error(w, "Service Unavailable (injected)", http.StatusServiceUnavailable)
			return
		case "timeout":
			ms := fc.TimeoutMs
			if ms <= 0 {
				ms = 10000
			}
			time.Sleep(time.Duration(ms) * time.Millisecond)
			statsMu.RLock()
			stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, fmt.Sprintf("%s %s -> injected timeout %dms", op, r.URL.Path, ms))
			statsMu.RUnlock()
			http.Error(w, "Gateway Timeout (injected)", http.StatusGatewayTimeout)
			return
		case "5xx":
			if n%2 == 1 {
				statsMu.RLock()
				stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, fmt.Sprintf("%s %s -> injected transient 500", op, r.URL.Path))
				statsMu.RUnlock()
				http.Error(w, "Internal Server Error (injected)", http.StatusInternalServerError)
				return
			}
		case "drop_layer_put":
			if op == "PUT" && cat == "layer" {
				statsMu.RLock()
				stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, fmt.Sprintf("PUT %s -> injected layer drop", r.URL.Path))
				statsMu.RUnlock()
				http.Error(w, "Injected layer upload failure", http.StatusBadGateway)
				return
			}
		case "drop_wal_put":
			if op == "PUT" && cat == "wal" {
				statsMu.RLock()
				stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, fmt.Sprintf("PUT %s -> injected wal drop", r.URL.Path))
				statsMu.RUnlock()
				http.Error(w, "Injected WAL upload failure", http.StatusBadGateway)
				return
			}
		}

		upURL := *r.URL
		upURL.Scheme = targetURL.Scheme
		upURL.Host = targetURL.Host
		upReq, err := http.NewRequestWithContext(r.Context(), r.Method, upURL.String(), bytes.NewReader(bodyBytes))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for k, vv := range r.Header {
			lk := strings.ToLower(k)
			if lk == "authorization" || lk == "host" || lk == "x-amz-date" || lk == "x-amz-content-sha256" || lk == "amz-sdk-invocation-id" || lk == "amz-sdk-request" {
				continue
			}
			for _, v := range vv {
				upReq.Header.Add(k, v)
			}
		}
		sum := sha256.Sum256(bodyBytes)
		payloadHash := hex.EncodeToString(sum[:])
		upReq.Header.Set("X-Amz-Content-Sha256", payloadHash)
		if err := signer.SignHTTP(context.Background(), creds, upReq, payloadHash, "s3", "auto", time.Now().UTC()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp, err := client.Do(upReq)
		if err != nil {
			statsMu.RLock()
			stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, err.Error())
			statsMu.RUnlock()
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			statsMu.RLock()
			stats.record(op, cat, inLen, 0, time.Since(start).Seconds()*1000, true, err.Error())
			statsMu.RUnlock()
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		if fc.Mode == "corrupt_layer_get" && op == "GET" && cat == "layer" && len(respBody) > 32 {
			respBody[len(respBody)/2] ^= 0xFF
		}

		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)

		latMs := time.Since(start).Seconds() * 1000
		isErr := resp.StatusCode >= 500
		statsMu.RLock()
		stats.record(op, cat, inLen, int64(len(respBody)), latMs, isErr, fmt.Sprintf("HTTP %d on %s %s", resp.StatusCode, op, r.URL.Path))
		statsMu.RUnlock()
	})

	log.Printf("r2proxy S3 listening on %s -> %s", *listenAddr, *targetEndpoint)
	log.Fatal(http.ListenAndServe(*listenAddr, proxyHandler))
}
