package control

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	accountIDContextKey = "air_account_id"
	maxJSONBodyBytes    = 16 * 1024
)

type createDatabaseReq struct {
	TenantID string `json:"tenant_id"`
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

	return r
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
