package tenant

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var validTenantIDRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`)

// ValidateID enforces a strict, non-traversable tenant identifier.
func ValidateID(tenantID string) error {
	if !validTenantIDRe.MatchString(tenantID) {
		return fmt.Errorf("invalid tenant_id %q: must match ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$", tenantID)
	}
	if strings.Contains(tenantID, "..") || strings.EqualFold(tenantID, "postgres") {
		return fmt.Errorf("invalid tenant_id %q: reserved or traversal sequence", tenantID)
	}
	return nil
}

// ValidateAccountID enforces a strict account identifier for multi-tenant API scoping.
func ValidateAccountID(accountID string) error {
	if !validTenantIDRe.MatchString(accountID) {
		return fmt.Errorf("invalid account_id %q: must match ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$", accountID)
	}
	return nil
}

// SafeSubpath resolves filepath.Join(rootDir, subdir, tenantID), verifies canonical containment,
// and rejects symlinks at root/subdir/target so no tenant filesystem operation can escape rootDir.
func SafeSubpath(rootDir, subdir, tenantID string) (string, error) {
	if err := ValidateID(tenantID); err != nil {
		return "", err
	}
	baseAbs, err := filepath.Abs(rootDir)
	if err != nil {
		return "", err
	}
	baseReal, err := filepath.EvalSymlinks(baseAbs)
	if err != nil {
		baseReal = baseAbs
	}

	subAbs := filepath.Join(baseReal, subdir)
	if info, err := os.Lstat(subAbs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("symlink rejected at subdir %s", subAbs)
	}
	subReal, err := filepath.EvalSymlinks(subAbs)
	if err != nil {
		subReal = subAbs
	}
	if subReal != baseReal && !strings.HasPrefix(subReal, baseReal+string(filepath.Separator)) {
		return "", fmt.Errorf("subdir %s escapes rootDir %s", subReal, baseReal)
	}

	target := filepath.Join(subReal, tenantID)
	if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("symlink rejected at target %s", target)
	}
	rel, err := filepath.Rel(baseReal, target)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal rejected for tenant %q", tenantID)
	}
	return target, nil
}
