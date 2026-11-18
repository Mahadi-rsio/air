#pragma once

#include <cstddef>
#include <cstdint>
#include <openssl/ssl.h>
#include <string>
#include <sys/types.h>
#include <vector>

namespace air::gw {

/**
 * RAII socket stream supporting both plaintext TCP/Unix sockets and OpenSSL TLS.
 * Non-copyable, move-enabled; closes the underlying SSL session and fd on destruction.
 */
class Stream {
public:
    explicit Stream(int fd = -1) : fd_(fd) {}
    ~Stream() { close(); }

    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;

    Stream(Stream&& other) noexcept : fd_(other.fd_), ssl_(other.ssl_) {
        other.fd_ = -1;
        other.ssl_ = nullptr;
    }

    Stream& operator=(Stream&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = other.fd_;
            ssl_ = other.ssl_;
            other.fd_ = -1;
            other.ssl_ = nullptr;
        }
        return *this;
    }

    int fd() const { return fd_; }
    bool valid() const { return fd_ >= 0; }
    bool is_tls() const { return ssl_ != nullptr; }

    /// Attach an already-accepted OpenSSL SSL* session to this stream.
    void set_ssl(SSL* ssl) { ssl_ = ssl; }

    /// Configure SO_RCVTIMEO / SO_SNDTIMEO socket timeouts in milliseconds (negative = unchanged).
    void set_timeouts(int recv_ms, int send_ms);

    /// Read exactly `len` bytes into `dst`, retrying on EINTR. Returns false on EOF/error.
    bool read_exact(uint8_t* dst, size_t len);

    /// Write all `len` bytes from `src`, retrying on EINTR. Returns false on error.
    bool write_all(const uint8_t* src, size_t len);
    bool write_all(const std::vector<uint8_t>& buf) {
        return write_all(buf.data(), buf.size());
    }

    /// Read up to `max_len` bytes into `dst`. Returns >0 on data, 0 on EOF, <0 on error.
    ssize_t read_some(uint8_t* dst, size_t max_len);

    /// Gracefully shut down TLS (if active) and close the underlying file descriptor.
    void close();

private:
    int fd_ = -1;
    SSL* ssl_ = nullptr;
};

/// Disable Nagle's algorithm (TCP_NODELAY) to avoid 40ms delayed-ACK stalls on PG wire round-trips.
void set_tcp_nodelay(int fd);

/// Bind and listen on a `host:port` TCP address (SO_REUSEADDR, backlog 128). Returns fd or -1.
int listen_tcp_addr(const std::string& addr_str);

/// Connect to a PostgreSQL backend via Unix domain socket (when local and sock_dir is set) or TCP.
int dial_backend_socket(const std::string& host, const std::string& sock_dir, int port);

} // namespace air::gw
