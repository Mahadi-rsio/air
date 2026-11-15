package backup

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	DefaultMaxTarFileBytes  int64 = 512 * 1024 * 1024  // 512 MiB per file
	DefaultMaxTarTotalBytes int64 = 2048 * 1024 * 1024 // 2 GiB total uncompressed
)

var (
	ErrUnsafeTarEntry       = errors.New("backup: unsafe tar entry rejected (symlink/hardlink/device/fifo/traversal)")
	ErrTarSizeLimitExceeded = errors.New("backup: tar uncompressed byte limit exceeded")
)

type TarExtractLimits struct {
	MaxFileBytes  int64
	MaxTotalBytes int64
}

func (l TarExtractLimits) withDefaults() TarExtractLimits {
	if l.MaxFileBytes <= 0 {
		l.MaxFileBytes = DefaultMaxTarFileBytes
	}
	if l.MaxTotalBytes <= 0 {
		l.MaxTotalBytes = DefaultMaxTarTotalBytes
	}
	return l
}

// CompressBytesZstd compresses raw bytes using zstd.
func CompressBytesZstd(raw []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, make([]byte, 0, len(raw)/2)), nil
}

// DecompressBytesZstd decompresses zstd-compressed bytes with a strict uncompressed byte cap.
func DecompressBytesZstd(compressed []byte, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxTarFileBytes
	}
	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	out, err := io.ReadAll(io.LimitReader(dec, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > maxBytes {
		return nil, fmt.Errorf("%w: decompressed stream exceeds %d bytes", ErrTarSizeLimitExceeded, maxBytes)
	}
	return out, nil
}

// CompressDirToTarZst packs srcDir into a zstd-compressed tar archive in memory.
func CompressDirToTarZst(srcDir string) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	tw := tar.NewWriter(zw)

	err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: refusing to archive symlink %q", ErrUnsafeTarEntry, rel)
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if info.IsDir() && !strings.HasSuffix(hdr.Name, "/") {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(tw, f)
			_ = f.Close()
			if copyErr != nil {
				return copyErr
			}
		}
		return nil
	})
	if err != nil {
		_ = tw.Close()
		_ = zw.Close()
		return nil, err
	}
	if err := tw.Close(); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ExtractTarZstSafe streams a .tar.zst reader into destDir using archive/tar with strict
// entry validation, bounded io.LimitReader caps, and 0700/0600 permissions.
func ExtractTarZstSafe(r io.Reader, destDir string, limits TarExtractLimits) error {
	limits = limits.withDefaults()

	destAbs, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(destAbs, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(destAbs, 0o700); err != nil {
		return err
	}
	destReal, err := filepath.EvalSymlinks(destAbs)
	if err != nil {
		return err
	}

	zr, err := zstd.NewReader(r)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}
	defer zr.Close()

	// Bound the uncompressed tar stream itself so crafted headers/padding cannot bypass limits.
	// Allow tar header/padding overhead on top of MaxTotalBytes, while still enforcing strict payload totals.
	streamCap := limits.MaxTotalBytes + (limits.MaxTotalBytes / 4) + 1024*1024
	if streamCap < limits.MaxTotalBytes {
		streamCap = limits.MaxTotalBytes
	}
	boundedStream := io.LimitReader(zr, streamCap+1)
	tr := tar.NewReader(boundedStream)

	var totalPayload int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar next: %w", err)
		}

		// Reject unsafe entry types immediately.
		switch hdr.Typeflag {
		case tar.TypeDir, tar.TypeReg, tar.TypeRegA:
		case tar.TypeSymlink, tar.TypeLink, tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			return fmt.Errorf("%w: entry %q has forbidden type %q", ErrUnsafeTarEntry, hdr.Name, hdr.Typeflag)
		default:
			return fmt.Errorf("%w: entry %q has unsupported type %q", ErrUnsafeTarEntry, hdr.Name, hdr.Typeflag)
		}

		target, isRoot, err := validateTarTargetPath(destReal, hdr.Name)
		if err != nil {
			return err
		}

		if hdr.Typeflag == tar.TypeDir {
			if !isRoot {
				if err := os.MkdirAll(target, 0o700); err != nil {
					return err
				}
				if err := os.Chmod(target, 0o700); err != nil {
					return err
				}
			}
			continue
		}

		if hdr.Size < 0 || hdr.Size > limits.MaxFileBytes {
			return fmt.Errorf("%w: file %q size %d exceeds per-file limit %d", ErrTarSizeLimitExceeded, hdr.Name, hdr.Size, limits.MaxFileBytes)
		}
		if totalPayload+hdr.Size > limits.MaxTotalBytes {
			return fmt.Errorf("%w: total uncompressed bytes exceed limit %d", ErrTarSizeLimitExceeded, limits.MaxTotalBytes)
		}

		parentDir := filepath.Dir(target)
		if err := os.MkdirAll(parentDir, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(parentDir, 0o700); err != nil {
			return err
		}

		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(f, io.LimitReader(tr, limits.MaxFileBytes+1))
		chmodErr := f.Chmod(0o600)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if chmodErr != nil {
			return chmodErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n > limits.MaxFileBytes {
			return fmt.Errorf("%w: file %q actual bytes %d exceed per-file limit %d", ErrTarSizeLimitExceeded, hdr.Name, n, limits.MaxFileBytes)
		}
		totalPayload += n
		if totalPayload > limits.MaxTotalBytes {
			return fmt.Errorf("%w: total uncompressed bytes %d exceed limit %d", ErrTarSizeLimitExceeded, totalPayload, limits.MaxTotalBytes)
		}
	}
	return nil
}

func validateTarTargetPath(destReal, rawName string) (target string, isRoot bool, err error) {
	if rawName == "" || strings.ContainsRune(rawName, '\x00') {
		return "", false, fmt.Errorf("%w: invalid empty or null-byte path %q", ErrUnsafeTarEntry, rawName)
	}
	if strings.HasPrefix(rawName, "/") || filepath.IsAbs(rawName) {
		return "", false, fmt.Errorf("%w: absolute path rejected: %q", ErrUnsafeTarEntry, rawName)
	}
	slashClean := filepath.ToSlash(rawName)
	for _, seg := range strings.Split(slashClean, "/") {
		if seg == ".." {
			return "", false, fmt.Errorf("%w: path traversal rejected: %q", ErrUnsafeTarEntry, rawName)
		}
	}

	clean := filepath.Clean(filepath.FromSlash(slashClean))
	if clean == "." {
		return destReal, true, nil
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false, fmt.Errorf("%w: path traversal rejected: %q", ErrUnsafeTarEntry, rawName)
	}

	target = filepath.Join(destReal, clean)
	rel, err := filepath.Rel(destReal, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false, fmt.Errorf("%w: target %q escapes destDir %q", ErrUnsafeTarEntry, target, destReal)
	}
	return target, false, nil
}
