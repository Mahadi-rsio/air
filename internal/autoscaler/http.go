package autoscaler

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// HTTPServer exposes /healthz, /status, and /tick for the autoscaler controller.
type HTTPServer struct {
	addr          string
	internalToken string
	ctrl          *Controller
	ln            net.Listener
	srv           *http.Server
}

// NewHTTPServer constructs an HTTPServer.
func NewHTTPServer(addr, internalToken string, ctrl *Controller) *HTTPServer {
	return &HTTPServer{
		addr:          addr,
		internalToken: internalToken,
		ctrl:          ctrl,
	}
}

// Start binds and serves HTTP endpoints in the background.
func (s *HTTPServer) Start() error {
	if s.addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.srv = &http.Server{
		Handler:           http.HandlerFunc(s.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      65 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		_ = s.srv.Serve(ln)
	}()
	return nil
}

// Addr returns the bound listener address.
func (s *HTTPServer) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPServer) Stop() {
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(ctx)
		s.srv = nil
	}
}

func (s *HTTPServer) isAuthorized(r *http.Request) bool {
	if s.internalToken == "" {
		return true
	}
	tok := strings.TrimSpace(r.Header.Get("X-Air-Internal-Token"))
	if tok != "" && subtle.ConstantTimeCompare([]byte(tok), []byte(s.internalToken)) == 1 {
		return true
	}
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		bearer := strings.TrimSpace(auth[7:])
		if subtle.ConstantTimeCompare([]byte(bearer), []byte(s.internalToken)) == 1 {
			return true
		}
	}
	return false
}

func writeJSONResp(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "close")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *HTTPServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"status": "ok",
			"mode":   s.ctrl.cfg.Mode,
		})
		return
	}
	if !s.isAuthorized(r) {
		writeJSONResp(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/status":
		writeJSONResp(w, http.StatusOK, s.ctrl.Status())
	case r.Method == http.MethodPost && r.URL.Path == "/tick":
		var body struct {
			NowMs int64 `json:"nowMs"`
		}
		if raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<16)); err == nil && len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		nowMs := body.NowMs
		if nowMs <= 0 {
			nowMs = time.Now().UnixMilli()
		}
		res, err := s.ctrl.HandleTick(r.Context(), nowMs)
		if err != nil {
			writeJSONResp(w, http.StatusInternalServerError, map[string]any{
				"error":  err.Error(),
				"result": res,
			})
			return
		}
		writeJSONResp(w, http.StatusOK, res)
	default:
		writeJSONResp(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}
