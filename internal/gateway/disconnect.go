package gateway

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"syscall"
	"time"
)

type netConnUnwrapper interface {
	NetConn() net.Conn
}

func extractRawConn(c net.Conn) syscall.RawConn {
	for c != nil {
		if sc, ok := c.(syscall.Conn); ok {
			if rc, err := sc.SyscallConn(); err == nil {
				return rc
			}
		}
		if u, ok := c.(netConnUnwrapper); ok {
			c = u.NetConn()
			continue
		}
		break
	}
	return nil
}

// isSocketDisconnected checks whether the peer has closed or reset the TCP socket
// using MSG_PEEK|MSG_DONTWAIT so zero buffered/pipelined bytes are consumed.
func isSocketDisconnected(rc syscall.RawConn) bool {
	if rc == nil {
		return false
	}
	var disconnected bool
	err := rc.Control(func(fd uintptr) {
		var buf [1]byte
		n, _, rerr := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		if rerr == nil {
			if n == 0 {
				disconnected = true
			}
			return
		}
		if rerr != syscall.EAGAIN && rerr != syscall.EWOULDBLOCK && rerr != syscall.EINTR {
			disconnected = true
		}
	})
	if err != nil {
		return true
	}
	return disconnected
}

// watchClientDisconnect monitors client for peer disconnect while a backend query
// is in flight. Fast queries (<25ms) exit before the first poll tick. Calling the
// returned stop func waits for the watcher goroutine to exit (zero leaks).
func watchClientDisconnect(ctx context.Context, client net.Conn, onDisconnect func()) func() {
	rc := extractRawConn(client)
	if rc == nil || onDisconnect == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if isSocketDisconnected(rc) {
					onDisconnect()
					return
				}
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(stopCh)
			<-doneCh
		})
	}
}

func sendBackendCancel(host, sockDir string, port int, pid, secret uint32, readTLS ...*tls.Config) {
	if port <= 0 || pid == 0 {
		return
	}
	bconn, err := dialBackendSocket(host, sockDir, port)
	if err != nil {
		return
	}
	if len(readTLS) != 0 {
		bconn, err = dialReadEngineTLS(context.Background(), bconn, host, readTLS[0])
		if err != nil {
			return
		}
	}
	defer bconn.Close()
	_ = bconn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	_, _ = bconn.Write(BuildCancelRequest(pid, secret))
}
