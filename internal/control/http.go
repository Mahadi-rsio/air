package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

const (
	accountIDContextKey = "air_account_id"
	maxJSONBodyBytes    = 16 * 1024
)

type createDatabaseReq struct {
	TenantID  string `json:"tenant_id"`
	AccountID string `json:"account_id,omitempty"`
}

type triggerBackupReq struct {
	Type string `json:"type"`
}

type restoreDatabaseReq struct {
	TargetLSN   string `json:"target_lsn"`
	VerifyQuery string `json:"verify_query"`
}

// HTTPHandler returns an http.Handler backed by Gin exposing the AIR Control Plane REST API:
//   - GET    /healthz
//   - POST   /v1/databases            (Header: Authorization: Bearer <token>, Idempotency-Key; Body: {"tenant_id": "..."})
//   - GET    /v1/databases/:id        (Header: Authorization: Bearer <token>)
//   - DELETE /v1/databases/:id        (Header: Authorization: Bearer <token>)
//   - DELETE /v1/api-keys/:id         (Header: Authorization: Bearer <token>)
//   - POST   /v1/databases/:id/backups (Header: Authorization: Bearer <token>)
//   - POST   /v1/databases/:id/restore (Header: Authorization: Bearer <token>)
func (cp *ControlPlane) HTTPHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		if err := cp.db.Ping(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/v1", cp.authMiddleware())
	{
		v1.POST("/databases", func(c *gin.Context) {
			idemKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
			if idemKey == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
				return
			}
			var req createDatabaseReq
			if !decodeJSONBody(c, &req, false) {
				return
			}
			resp, err := cp.ProvisionDatabase(c.Request.Context(), c.GetString(accountIDContextKey), idemKey, req.TenantID)
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusCreated, resp)
		})

		v1.GET("/databases/:id", func(c *gin.Context) {
			info, err := cp.GetDatabase(c.Request.Context(), c.GetString(accountIDContextKey), c.Param("id"))
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, info)
		})

		v1.DELETE("/databases/:id", func(c *gin.Context) {
			id := c.Param("id")
			if err := cp.DeleteDatabase(c.Request.Context(), c.GetString(accountIDContextKey), id); err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"status": "purged", "tenant_id": id})
		})

		v1.DELETE("/api-keys/:id", func(c *gin.Context) {
			id := c.Param("id")
			if err := cp.RevokeAPIKey(c.Request.Context(), c.GetString(accountIDContextKey), id); err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"status": "revoked", "key_id": id})
		})

		v1.POST("/databases/:id/backups", func(c *gin.Context) {
			var req triggerBackupReq
			if !decodeJSONBody(c, &req, true) {
				return
			}
			resp, err := cp.TriggerBackup(c.Request.Context(), c.GetString(accountIDContextKey), c.Param("id"), req.Type)
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusCreated, resp)
		})

		v1.POST("/databases/:id/restore", func(c *gin.Context) {
			var req restoreDatabaseReq
			if !decodeJSONBody(c, &req, true) {
				return
			}
			resp, err := cp.RestoreDatabase(c.Request.Context(), c.GetString(accountIDContextKey), c.Param("id"), req.TargetLSN, req.VerifyQuery)
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, resp)
		})
	}

	cp.registerInternalRoutes(r)
	return r
}

// RunnerHTTPHandler exposes only /healthz and /internal/v1/* so Air DB acts solely
// as a database runner controlled by air-gateway.
func (cp *ControlPlane) RunnerHTTPHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		if err := cp.db.Ping(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	cp.registerInternalRoutes(r)
	return r
}

func (cp *ControlPlane) registerInternalRoutes(r *gin.Engine) {
	internal := r.Group("/internal/v1", cp.internalAuthMiddleware())
	{
		internal.POST("/databases", func(c *gin.Context) {
			var req createDatabaseReq
			if !decodeJSONBody(c, &req, false) {
				return
			}
			accountID := strings.TrimSpace(req.AccountID)
			if accountID == "" {
				accountID = "acc_gateway"
			}
			idemKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
			if idemKey == "" {
				idemKey = "gw-" + req.TenantID
			}
			resp, err := cp.ProvisionDatabase(c.Request.Context(), accountID, idemKey, req.TenantID)
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusCreated, resp)
		})

		internal.GET("/databases", func(c *gin.Context) {
			list, err := cp.ListDatabases(c.Request.Context(), strings.TrimSpace(c.Query("account_id")))
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"databases": list})
		})

		internal.GET("/databases/:id", func(c *gin.Context) {
			info, err := cp.GetDatabase(c.Request.Context(), "", c.Param("id"))
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, info)
		})

		internal.DELETE("/databases/:id", func(c *gin.Context) {
			id := c.Param("id")
			if err := cp.DeleteDatabase(c.Request.Context(), "", id); err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"status": "purged", "tenant_id": id})
		})

		internal.POST("/databases/:id/suspend", func(c *gin.Context) {
			id := c.Param("id")
			if err := cp.SuspendTenant(c.Request.Context(), id, 3*time.Second, nil); err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"status": "SUSPENDED", "tenant_id": id})
		})

		internal.POST("/databases/:id/start", func(c *gin.Context) {
			id := c.Param("id")
			port, sockDir, appPass, err := cp.WakeTenant(c.Request.Context(), id)
			if err != nil {
				writeControlError(c, err)
				return
			}
			cp.TouchTenantActivity(id)
			c.JSON(http.StatusOK, gin.H{
				"tenant_id":    id,
				"state":        "ACTIVE",
				"host":         cp.backendHost,
				"port":         port,
				"sock_dir":     sockDir,
				"app_password": appPass,
			})
		})

		internal.POST("/databases/:id/lock", func(c *gin.Context) {
			id := c.Param("id")
			if err := cp.LockDatabase(c.Request.Context(), id, 3*time.Second, nil); err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"status": "LOCKED", "tenant_id": id})
		})

		internal.POST("/databases/:id/unlock", func(c *gin.Context) {
			id := c.Param("id")
			if err := cp.UnlockDatabase(c.Request.Context(), id); err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, gin.H{"status": "SUSPENDED", "tenant_id": id})
		})

		internal.GET("/tenants/:id/route", func(c *gin.Context) {
			id := c.Param("id")
			state, port, sockDir, appPass, err := cp.GetTenantRouteAndCredentials(c.Request.Context(), id)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					c.JSON(http.StatusNotFound, gin.H{"error": "tenant not found"})
					return
				}
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"tenant_id":    id,
				"state":        state,
				"host":         cp.backendHost,
				"port":         port,
				"sock_dir":     sockDir,
				"app_password": appPass,
			})
		})

		internal.POST("/tenants/:id/wake", func(c *gin.Context) {
			id := c.Param("id")
			port, sockDir, appPass, err := cp.WakeTenant(c.Request.Context(), id)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					c.JSON(http.StatusNotFound, gin.H{"error": "tenant not found"})
					return
				}
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
				return
			}
			cp.TouchTenantActivity(id)
			c.JSON(http.StatusOK, gin.H{
				"tenant_id":    id,
				"state":        "ACTIVE",
				"host":         cp.backendHost,
				"port":         port,
				"sock_dir":     sockDir,
				"app_password": appPass,
			})
		})

		internal.POST("/tenants/:id/activity", func(c *gin.Context) {
			id := c.Param("id")
			if err := tenant.ValidateID(id); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			cp.TouchTenantActivity(id)
			c.Status(http.StatusNoContent)
		})
	}
}

func (cp *ControlPlane) internalAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if cp.internalToken == "" {
			c.Next()
			return
		}
		tok := strings.TrimSpace(c.GetHeader("X-Air-Internal-Token"))
		if tok == "" {
			authHeader := strings.TrimSpace(c.GetHeader("Authorization"))
			if len(authHeader) > 7 && strings.EqualFold(authHeader[:7], "Bearer ") {
				tok = strings.TrimSpace(authHeader[7:])
			}
		}
		if tok != cp.internalToken {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": ErrUnauthorized.Error()})
			return
		}
		c.Next()
	}
}

// NewHTTPPoolDrainFn returns a cooperative pool drain callback that calls air-gateway's admin HTTP API.
func NewHTTPPoolDrainFn(gatewayAdminURL, internalToken string) func(tenantID string, timeout time.Duration) error {
	base := strings.TrimRight(strings.TrimSpace(gatewayAdminURL), "/")
	if base == "" {
		return nil
	}
	client := &http.Client{Timeout: 5 * time.Second}
	return func(tenantID string, timeout time.Duration) error {
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout+2*time.Second)
		defer cancel()
		endpoint := fmt.Sprintf("%s/internal/v1/tenants/%s/drain?timeout=%s", base, url.PathEscape(tenantID), url.QueryEscape(timeout.String()))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return err
		}
		if internalToken != "" {
			req.Header.Set("X-Air-Internal-Token", internalToken)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		if resp.StatusCode == http.StatusConflict {
			return fmt.Errorf("%w: gateway vetoed drain", cell.ErrDrainVetoed)
		}
		return fmt.Errorf("gateway drain failed with status %d", resp.StatusCode)
	}
}

func (cp *ControlPlane) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := strings.TrimSpace(c.GetHeader("Authorization"))
		if len(authHeader) < 8 || !strings.EqualFold(authHeader[:7], "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": ErrUnauthorized.Error()})
			return
		}
		rawToken := strings.TrimSpace(authHeader[7:])
		accountID, _, err := cp.AuthenticateAPIKey(c.Request.Context(), rawToken)
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			} else {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "authentication check failed"})
			}
			return
		}
		if hdrAcct := strings.TrimSpace(c.GetHeader("X-Account-ID")); hdrAcct != "" && hdrAcct != accountID {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": ErrForbidden.Error()})
			return
		}
		c.Set(accountIDContextKey, accountID)
		c.Next()
	}
}

func decodeJSONBody(c *gin.Context, dst any, allowEOF bool) bool {
	err := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, maxJSONBodyBytes)).Decode(dst)
	if err != nil && (!allowEOF || !errors.Is(err, io.EOF)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return false
	}
	return true
}

func writeControlError(c *gin.Context, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrForbidden):
		status = http.StatusForbidden
	case errors.Is(err, ErrIdempotencyConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrBackupNotConfigured):
		status = http.StatusServiceUnavailable
	}
	c.JSON(status, gin.H{"error": err.Error()})
}
