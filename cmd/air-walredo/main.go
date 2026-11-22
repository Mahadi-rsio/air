package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

const (
	prSetNoNewPrivs = 38
	maxWALRecordLen = 16 * 1024 * 1024
	maxRedoRecords  = 65536
)

func applySandbox() {
	_ = os.Chdir(os.TempDir())
	// Prevent privilege escalation.
	_, _, _ = syscall.Syscall6(syscall.SYS_PRCTL, prSetNoNewPrivs, 1, 0, 0, 0, 0)
	// Disallow creating/writing files.
	_ = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 0, Max: 0})
	// Cap open file descriptors (stdin, stdout, stderr + runtime).
	_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: 64, Max: 64})
	// Cap virtual address space to 512 MiB.
	_ = syscall.Setrlimit(syscall.RLIMIT_AS, &syscall.Rlimit{Cur: 512 * 1024 * 1024, Max: 512 * 1024 * 1024})
}

func main() {
	applySandbox()

	br := bufio.NewReaderSize(os.Stdin, 64*1024)
	bw := bufio.NewWriterSize(os.Stdout, 16*1024)

	var keyBuf [24]byte
	var hdr [1]byte
	var cntBuf [4]byte

	for {
		if _, err := io.ReadFull(br, keyBuf[:]); err != nil {
			if err == io.EOF {
				return
			}
			fmt.Fprintf(os.Stderr, "air-walredo: read key: %v\n", err)
			os.Exit(1)
		}
		key := pageserver.DecodeKey(keyBuf[:])

		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			fmt.Fprintf(os.Stderr, "air-walredo: read hasBase: %v\n", err)
			os.Exit(1)
		}
		var basePage []byte
		if hdr[0] != 0 {
			basePage = make([]byte, pageserver.PageSize)
			if _, err := io.ReadFull(br, basePage); err != nil {
				fmt.Fprintf(os.Stderr, "air-walredo: read basePage: %v\n", err)
				os.Exit(1)
			}
		}

		if _, err := io.ReadFull(br, cntBuf[:]); err != nil {
			fmt.Fprintf(os.Stderr, "air-walredo: read count: %v\n", err)
			os.Exit(1)
		}
		count := binary.LittleEndian.Uint32(cntBuf[:])
		if count > maxRedoRecords {
			fmt.Fprintf(os.Stderr, "air-walredo: record count %d exceeds limit\n", count)
			os.Exit(2)
		}

		records := make([][]byte, count)
		for i := uint32(0); i < count; i++ {
			if _, err := io.ReadFull(br, cntBuf[:]); err != nil {
				fmt.Fprintf(os.Stderr, "air-walredo: read record len: %v\n", err)
				os.Exit(1)
			}
			rlen := binary.LittleEndian.Uint32(cntBuf[:])
			if rlen > maxWALRecordLen {
				fmt.Fprintf(os.Stderr, "air-walredo: record len %d exceeds limit\n", rlen)
				os.Exit(2)
			}
			rec := make([]byte, rlen)
			if _, err := io.ReadFull(br, rec); err != nil {
				fmt.Fprintf(os.Stderr, "air-walredo: read record payload: %v\n", err)
				os.Exit(1)
			}
			records[i] = rec
		}

		page, ok := pageserver.ApplyWALRecordsInGo(key, basePage, records)
		if !ok || len(page) != pageserver.PageSize {
			fmt.Fprintf(os.Stderr, "air-walredo: unsupported or corrupt WAL record chain for %s (count=%d)\n", key, count)
			os.Exit(2)
		}
		if _, err := bw.Write(page); err != nil {
			fmt.Fprintf(os.Stderr, "air-walredo: write page: %v\n", err)
			os.Exit(1)
		}
		if err := bw.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "air-walredo: flush page: %v\n", err)
			os.Exit(1)
		}
	}
}
