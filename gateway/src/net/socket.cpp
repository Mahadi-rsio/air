#include "net/socket.hpp"

#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

namespace air::gw {

void Stream::set_timeouts(int recv_ms, int send_ms) {
    if (fd_ < 0) return;
    struct timeval tv{};
    if (recv_ms >= 0) {
        tv.tv_sec = recv_ms / 1000;
        tv.tv_usec = (recv_ms % 1000) * 1000;
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    if (send_ms >= 0) {
        tv.tv_sec = send_ms / 1000;
        tv.tv_usec = (send_ms % 1000) * 1000;
        setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }
}

bool Stream::read_exact(uint8_t* dst, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = 0;
        if (ssl_) {
            n = SSL_read(ssl_, dst + off, static_cast<int>(len - off));
            if (n <= 0) return false;
        } else {
            n = ::recv(fd_, dst + off, len - off, 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool Stream::write_all(const uint8_t* src, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = 0;
        if (ssl_) {
            n = SSL_write(ssl_, src + off, static_cast<int>(len - off));
            if (n <= 0) return false;
        } else {
            n = ::send(fd_, src + off, len - off, MSG_NOSIGNAL);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

ssize_t Stream::read_some(uint8_t* dst, size_t max_len) {
    if (max_len == 0) return 0;
    while (true) {
        ssize_t n = 0;
        if (ssl_) {
            n = SSL_read(ssl_, dst, static_cast<int>(max_len));
            if (n <= 0) return -1;
            return n;
        }
        n = ::recv(fd_, dst, max_len, 0);
        if (n < 0 && errno == EINTR) continue;
        return n;
    }
}

void Stream::close() {
    if (ssl_) {
        SSL_shutdown(ssl_);
        SSL_free(ssl_);
        ssl_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void set_tcp_nodelay(int fd) {
    if (fd < 0) return;
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

int listen_tcp_addr(const std::string& addr_str) {
    auto colon = addr_str.rfind(':');
    if (colon == std::string::npos) return -1;
    std::string host = addr_str.substr(0, colon);
    std::string port = addr_str.substr(colon + 1);
    if (host.empty()) host = "0.0.0.0";

    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    struct addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0 || !res) {
        return -1;
    }
    int fd = -1;
    for (auto* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        if (::bind(fd, p->ai_addr, p->ai_addrlen) == 0 && ::listen(fd, 128) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

int dial_backend_socket(const std::string& host, const std::string& sock_dir, int port) {
    const bool is_local = host.empty() || host == "127.0.0.1" || host == "localhost";
    if (!sock_dir.empty() && is_local) {
        std::string sock_path = sock_dir + "/.s.PGSQL." + std::to_string(port);
        if (sock_path.size() < sizeof(sockaddr_un::sun_path)) {
            int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
            if (fd >= 0) {
                struct timeval tv{5, 0};
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
                sockaddr_un addr{};
                addr.sun_family = AF_UNIX;
                std::strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);
                if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
                    return fd;
                }
                ::close(fd);
            }
        }
    }

    std::string target_host = host.empty() ? "127.0.0.1" : host;
    std::string port_str = std::to_string(port);
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (::getaddrinfo(target_host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        return -1;
    }
    int fd = -1;
    for (auto* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        struct timeval tv{5, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            set_tcp_nodelay(fd);
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

} // namespace air::gw
