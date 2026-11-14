// ============================================================================
// Regression tests: Coordinator::shutdown() stops the coordinator after a
// Shutdown frame, and returns while a client connection is still open. (Their
// own executable: on code where shutdown waits for every client to disconnect
// it never returns, and no test timeout is used.)
// ============================================================================

#include "numafabric/numafabric.hpp"
#include "test_util.hpp"

#include <winsock2.h>
#include <windows.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>

using namespace numafabric;

namespace {

std::unique_ptr<coord::Coordinator> make_coordinator() {
    backend::SyntheticConfig cfg;
    cfg.node_count = 2;
    cfg.processors_per_node = 8;
    cfg.group_capacity = 64;
    auto c = std::make_unique<coord::Coordinator>(backend::make_synthetic_backend(cfg));
    c->runtime().discover();
    return c;
}

SOCKET connect_loopback(std::uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    if (s != INVALID_SOCKET && ::connect(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) {
        ::closesocket(s);
        s = INVALID_SOCKET;
    }
    return s;
}

bool send_frame(SOCKET s, protocol::MsgType type) {
    const auto f = protocol::encode_frame(static_cast<std::uint16_t>(type), {});
    return ::send(s, reinterpret_cast<const char*>(f.data()), static_cast<int>(f.size()), 0) ==
           static_cast<int>(f.size());
}

// Reads until one complete frame arrives, or the connection ends (nullopt).
std::optional<protocol::Frame> recv_frame(SOCKET s) {
    protocol::FrameStreamDecoder dec;
    std::optional<protocol::Frame> fr;
    char buf[256];
    while (!fr) {
        const int n = ::recv(s, buf, static_cast<int>(sizeof(buf)), 0);
        if (n <= 0) break;
        fr = dec.push(reinterpret_cast<const std::uint8_t*>(buf), static_cast<std::size_t>(n));
    }
    return fr;
}

// After a Shutdown frame (which only clears running()), shutdown() must still stop the
// coordinator: close its listener, end its connections and join its threads. Otherwise the
// coordinator keeps serving, and destroying it terminates the process on joinable threads.
void test_shutdown_after_shutdown_frame() {
    auto c = make_coordinator();
    const auto port = c->bind(0);
    SOCKET s = connect_loopback(port);
    CHECK(s != INVALID_SOCKET);
    CHECK(send_frame(s, protocol::MsgType::Shutdown));
    const auto ack = recv_frame(s);
    CHECK(ack && ack->type == static_cast<std::uint16_t>(protocol::MsgType::Shutdown));
    CHECK(!c->running());
    c->shutdown();
    // The coordinator no longer serves this connection: a Ping gets no Pong.
    (void)send_frame(s, protocol::MsgType::Ping);
    const auto after = recv_frame(s);
    CHECK_MSG(!after, "coordinator still serving after shutdown()");
    ::closesocket(s);
    std::fflush(stdout);
    c.reset();
}

// shutdown() returns while a client connection is still open (it must not wait for every
// client to disconnect).
void test_shutdown_with_connected_client() {
    auto c = make_coordinator();
    const auto port = c->bind(0);
    SOCKET s = connect_loopback(port);
    CHECK(s != INVALID_SOCKET);
    // A Ping answered on this connection proves a connection thread is serving it.
    CHECK(send_frame(s, protocol::MsgType::Ping));
    const auto pong = recv_frame(s);
    CHECK(pong && pong->type == static_cast<std::uint16_t>(protocol::MsgType::Pong));
    c->shutdown();
    CHECK(!c->running());
    ::closesocket(s);
}

void run_all() {
    // A failure that ends in std::terminate must exit, not wait on an error-reporting dialog.
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
    WSADATA wsa{};
    WSAStartup(MAKEWORD(2, 2), &wsa);
    RUN_TEST(test_shutdown_after_shutdown_frame);
    RUN_TEST(test_shutdown_with_connected_client);
}

} // namespace

TEST_MAIN()
