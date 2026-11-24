package gateway

import (
	"bytes"
	"net"
	"sync"
	"testing"
)

func TestPhase28_RelayReadBackendBatch_FallbackBeforeStreamAndNoRetryAfterStream(t *testing.T) {
	// 1. Pre-stream ErrorResponse ('E') -> zero client bytes written, safe PG fallback.
	t.Run("PreStreamErrorAllowsFallback", func(t *testing.T) {
		readSrv, readCli := net.Pipe()
		clientSrv, clientCli := net.Pipe()
		defer readSrv.Close()
		defer readCli.Close()
		defer clientSrv.Close()
		defer clientCli.Close()

		var clientBytes bytes.Buffer
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4096)
			for {
				n, err := clientCli.Read(buf)
				if n > 0 {
					clientBytes.Write(buf[:n])
				}
				if err != nil {
					return
				}
			}
		}()

		go func() {
			_, _ = readSrv.Write(BuildErrorResponse("ERROR", "42000", "unsupported syntax"))
			_, _ = readSrv.Write(BuildReadyForQuery('I'))
			_ = readSrv.Close()
		}()

		_, _, hadErr, streamStarted, clientOK := relayReadBackendBatch(readCli, clientSrv, 4096)
		_ = clientSrv.Close()
		wg.Wait()

		if !clientOK || !hadErr || streamStarted {
			t.Fatalf("expected clientOK=true hadErr=true streamStarted=false, got clientOK=%v hadErr=%v streamStarted=%v",
				clientOK, hadErr, streamStarted)
		}
		if clientBytes.Len() != 0 {
			t.Fatalf("expected 0 bytes written to client on pre-stream error, got %d", clientBytes.Len())
		}
	})

	// 2. Mid-stream ErrorResponse after fallbackCap exceeded -> error forwarded, NO PG retry.
	t.Run("MidStreamErrorNeverRetriesPG", func(t *testing.T) {
		readSrv, readCli := net.Pipe()
		clientSrv, clientCli := net.Pipe()
		defer readSrv.Close()
		defer readCli.Close()
		defer clientSrv.Close()
		defer clientCli.Close()

		var clientTypes []byte
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tp, _, err := ReadPGPacket(clientCli)
				if err != nil {
					return
				}
				clientTypes = append(clientTypes, tp)
			}
		}()

		go func() {
			// Exceed fallbackCap=128 with DataRows, then emit an error mid-stream.
			row := BuildPGMessage('D', []byte{0, 1, 0, 0, 0, 40, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '!', '?', '.', ','})
			for i := 0; i < 6; i++ {
				_, _ = readSrv.Write(row)
			}
			_, _ = readSrv.Write(BuildErrorResponse("ERROR", "53200", "memory budget exceeded mid-stream"))
			_, _ = readSrv.Write(BuildReadyForQuery('I'))
			_ = readSrv.Close()
		}()

		rstats, _, hadErr, streamStarted, clientOK := relayReadBackendBatch(readCli, clientSrv, 128)
		_ = clientSrv.Close()
		wg.Wait()

		if !clientOK || !hadErr || !streamStarted {
			t.Fatalf("expected clientOK=true hadErr=true streamStarted=true, got clientOK=%v hadErr=%v streamStarted=%v",
				clientOK, hadErr, streamStarted)
		}
		if rstats.rowsReturned != 6 {
			t.Fatalf("expected 6 rows counted, got %d", rstats.rowsReturned)
		}
		if len(clientTypes) == 0 || clientTypes[len(clientTypes)-1] != 'Z' {
			t.Fatalf("expected forwarded 'E' and 'Z' to client, got packet types %q", string(clientTypes))
		}
		if !bytes.Contains(clientTypes, []byte{'E'}) {
			t.Fatalf("expected forwarded 'E' packet in %q", string(clientTypes))
		}
	})
}
