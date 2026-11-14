// ============================================================================
// Regression tests: coordinator epoch and ownership fencing, binding and
// release races, reservation accounting and identity across recovery,
// recovered worker authority, persisted duplicate ids.
// ============================================================================

#include "numafabric/numafabric.hpp"
#include "test_util.hpp"

#include <winsock2.h>
#include <windows.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace numafabric;

namespace {

backend::SyntheticConfig two_nodes() {
    backend::SyntheticConfig c;
    c.node_count = 2;
    c.processors_per_node = 8;
    c.group_capacity = 64;
    return c;
}

std::string temp_name(const char* tag) {
    static int n = 0;
    return "nf_regression_" + std::to_string(GetCurrentProcessId()) + "_" + std::to_string(++n) + "_" + tag + ".state";
}

// A synthetic backend whose allocations are backed by real buffers (kept alive until the
// backend is destroyed, so a double free is counted, never performed), with hooks.
class HookBackend final : public backend::Backend {
public:
    explicit HookBackend(std::unique_ptr<backend::Backend> inner) : inner_(std::move(inner)) {}
    ~HookBackend() override { for (auto& [p, n] : frees_) { (void)n; std::free(p); } }

    std::function<void()> on_allocate;       // runs inside allocate() once, then cleared
    std::function<void()> on_apply_affinity; // runs inside apply_thread_affinity() once, then cleared

    int frees_of(void* p) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = frees_.find(p);
        return it == frees_.end() ? 0 : it->second;
    }

    backend::BackendKind kind() const noexcept override { return inner_->kind(); }
    std::string kind_name() const override { return inner_->kind_name(); }
    std::string describe() const override { return inner_->describe(); }
    topo::HostTopology discover_host() override { return inner_->discover_host(); }
    backend::AllocationResult allocate(const backend::AllocationSpec& spec) override {
        auto r = inner_->allocate(spec);
        void* buf = std::malloc(static_cast<std::size_t>(r.granted_bytes.value() ? r.granted_bytes.value() : 1));
        {
            std::lock_guard<std::mutex> lk(mu_);
            frees_[buf] = 0;
        }
        r.ptr = buf;
        r.ptr_valid = true;
        r.kind = AllocationKind::Host;
        if (on_allocate) { auto f = std::move(on_allocate); on_allocate = nullptr; f(); }
        return r;
    }
    void free(const backend::AllocationResult& alloc) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = frees_.find(alloc.ptr);
        if (it != frees_.end()) ++it->second;
    }
    std::optional<NumaNodeId> probe_allocation_node(const backend::AllocationResult& alloc) override {
        return inner_->probe_allocation_node(alloc);
    }
    affinity::AffinitySet current_thread_affinity() override { return inner_->current_thread_affinity(); }
    bool apply_thread_affinity(const affinity::AffinitySet& set) override {
        const bool ok = inner_->apply_thread_affinity(set);
        if (on_apply_affinity) { auto f = std::move(on_apply_affinity); on_apply_affinity = nullptr; f(); }
        return ok;
    }
    std::vector<accel::AcceleratorInfo> enumerate_accelerators() override { return inner_->enumerate_accelerators(); }

private:
    std::unique_ptr<backend::Backend> inner_;
    std::mutex mu_;
    std::map<void*, int> frees_;
};

// ---- coordinator frames ------------------------------------------------------
struct Reply {
    std::uint16_t type = 0;
    std::vector<std::uint8_t> payload;
};

Reply call(coord::Coordinator& c, std::uint64_t conn, protocol::MsgType t, const protocol::WireWriter& w) {
    protocol::Frame f;
    f.type = static_cast<std::uint16_t>(t);
    f.payload = w.data();
    const auto bytes = c.handle_frame(conn, f);
    protocol::FrameStreamDecoder dec;
    Reply r;
    if (auto out = dec.push(bytes.data(), bytes.size())) { r.type = out->type; r.payload = out->payload; }
    return r;
}

bool is_error(const Reply& r, AuthorityVerification code) {
    if (r.type != static_cast<std::uint16_t>(protocol::MsgType::Error) || r.payload.empty()) return false;
    return r.payload[0] == static_cast<std::uint8_t>(code);
}

bool is_type(const Reply& r, protocol::MsgType t) { return r.type == static_cast<std::uint16_t>(t); }

WorkerBootId register_worker(coord::Coordinator& c, std::uint64_t conn, std::uint64_t id) {
    protocol::WireWriter w;
    w.u64(id);
    w.u64(1000 + id);
    const auto r = call(c, conn, protocol::MsgType::Register, w);
    protocol::WireReader rd(r.payload);
    return WorkerBootId::from(rd.u64());
}

Reply reserve(coord::Coordinator& c, std::uint64_t conn, std::uint64_t id, WorkerBootId boot) {
    protocol::WireWriter w;
    w.u64(id); w.u64(boot.value()); w.u64(1); w.u64(1 << 20); w.u32(1); w.u64(0); w.u8(0);
    return call(c, conn, protocol::MsgType::ReserveRequest, w);
}

ReservationId reservation_of(const Reply& r) {
    protocol::WireReader rd(r.payload);
    return ReservationId::from(rd.u64());
}

Reply release(coord::Coordinator& c, std::uint64_t conn, std::uint64_t id, WorkerBootId boot, ReservationId res) {
    protocol::WireWriter w;
    w.u64(id); w.u64(boot.value()); w.u64(res.value());
    return call(c, conn, protocol::MsgType::ReleaseRequest, w);
}

Reply bind(coord::Coordinator& c, std::uint64_t conn, std::uint64_t id, WorkerBootId boot) {
    protocol::WireWriter w;
    w.u64(id); w.u64(boot.value()); w.u64(1); w.u8(0);
    return call(c, conn, protocol::MsgType::BindRequest, w);
}

Reply observe(coord::Coordinator& c, std::uint64_t conn, std::uint64_t id, WorkerBootId boot) {
    protocol::WireWriter w;
    w.u64(id); w.u64(boot.value()); w.u64(1); w.u8(0); w.u8(0);
    return call(c, conn, protocol::MsgType::ObserveReport, w);
}

std::unique_ptr<coord::Coordinator> make_coordinator() {
    auto c = std::make_unique<coord::Coordinator>(backend::make_synthetic_backend(two_nodes()));
    c->runtime().discover();
    return c;
}

// ---- tests -------------------------------------------------------------------

// Every mutable operation of a worker registered under an earlier coordinator epoch is
// fenced, not only placement requests; re-registering under the new epoch restores authority.
void test_epoch_fences_all_mutations() {
    auto c = make_coordinator();
    const auto conn = c->open_controller();
    const auto boot = register_worker(*c, conn, 10);
    const auto held = reserve(*c, conn, 10, boot);
    CHECK(is_type(held, protocol::MsgType::ReserveReply));
    c->advance_epoch();
    CHECK(is_error(reserve(*c, conn, 10, boot), AuthorityVerification::StaleEpoch));
    CHECK(is_error(release(*c, conn, 10, boot, reservation_of(held)), AuthorityVerification::StaleEpoch));
    CHECK(is_error(bind(*c, conn, 10, boot), AuthorityVerification::StaleEpoch));
    CHECK(is_error(observe(*c, conn, 10, boot), AuthorityVerification::StaleEpoch));
    const auto fresh = register_worker(*c, conn, 10);
    CHECK(fresh != boot);
    CHECK(is_type(reserve(*c, conn, 10, fresh), protocol::MsgType::ReserveReply));
    CHECK(is_type(release(*c, conn, 10, fresh, reservation_of(held)), protocol::MsgType::ReleaseReply));
    c->close_controller(conn);
}

// A worker cannot release another worker's reservation.
void test_release_requires_owner() {
    auto c = make_coordinator();
    const auto conn = c->open_controller();
    const auto b10 = register_worker(*c, conn, 10);
    const auto b20 = register_worker(*c, conn, 20);
    const auto held = reserve(*c, conn, 10, b10);
    CHECK(is_type(held, protocol::MsgType::ReserveReply));
    const auto foreign = release(*c, conn, 20, b20, reservation_of(held));
    CHECK(is_type(foreign, protocol::MsgType::Error));
    CHECK(c->runtime().reserved_bytes() == (1u << 20));
    CHECK(is_type(release(*c, conn, 10, b10, reservation_of(held)), protocol::MsgType::ReleaseReply));
    CHECK(c->runtime().reservations_clean());
    c->close_controller(conn);
}

// A bind authorised for an incarnation that is superseded while it runs never lands on the
// restarted incarnation.
void test_superseded_bind_not_applied_to_restarted_worker() {
    auto hb = std::make_unique<HookBackend>(backend::make_synthetic_backend(two_nodes()));
    auto* hook = hb.get();
    Runtime rt(std::move(hb));
    rt.discover();
    const auto w1 = rt.register_worker(WorkerId::from(7), ProcessId::from(70));
    WorkerBootId fresh = WorkerBootId::invalid();
    hook->on_apply_affinity = [&] { fresh = rt.restart_worker(WorkerId::from(7), ProcessId::from(71)).boot_id; };
    try { (void)rt.bind_worker(WorkerId::from(7), w1.boot_id, rt.topology().nodes[0].id, true); } catch (const std::exception&) {}
    const auto* now = rt.worker(WorkerId::from(7));
    CHECK(now != nullptr);
    if (now) {
        CHECK(now->boot_id == fresh);
        CHECK(!now->bound);
        CHECK(now->binding_generation == BindingGeneration::initial());
    }
}

// Releasing a region while it is being migrated is refused: otherwise the migration frees the
// original buffer a second time and resurrects the released region.
void test_release_during_migration_refused() {
    auto hb = std::make_unique<HookBackend>(backend::make_synthetic_backend(two_nodes()));
    auto* hook = hb.get();
    Runtime rt(std::move(hb));
    rt.discover();
    const auto w = rt.register_worker(WorkerId::from(3), ProcessId::from(30));
    const auto region = rt.allocate_on_node(Bytes::from(64 * 1024), rt.topology().nodes[0].id);
    void* original = region.backend_result.ptr;
    std::atomic<bool> release_done{false};
    std::atomic<bool> release_ok{false};
    std::thread releaser;
    hook->on_allocate = [&] {
        releaser = std::thread([&] {
            try { rt.release(region.id); release_ok = true; } catch (const std::exception&) {}
            release_done = true;
        });
        // Wait until the release either took effect or was refused.
        while (!release_done.load()) {
            const auto* r = rt.memory_manager().get(region.id);
            if (r && r->state == LifecycleState::Released) break;
            std::this_thread::yield();
        }
    };
    bool committed = false;
    try {
        committed = rt.migrate(region.id, rt.topology().nodes[1].id, WorkerId::from(3),
                               Runtime::Auth{rt.epoch(), w.boot_id}, "regression");
    } catch (const std::exception& e) {
        CHECK_MSG(false, std::string("migration threw: ") + e.what());
    }
    if (releaser.joinable()) releaser.join();
    CHECK(hook->frees_of(original) <= 1);
    const auto* r = rt.memory_manager().get(region.id);
    CHECK(r != nullptr);
    if (r) CHECK(!(release_ok.load() && r->state != LifecycleState::Released));
    CHECK(!release_ok.load());
    CHECK(committed);
    if (r && r->state != LifecycleState::Released) rt.release(region.id);
    CHECK(rt.memory_accounting_clean());
}

// A refused reservation leaves the accounting exactly as it was.
void test_refused_reservation_leaves_accounting_clean() {
    reservation::ReservationStore store([](NumaNodeId) { return Capacity::from(1ull << 30); });
    reservation::ReserveRequest big;
    big.node = NumaNodeId::from(1);
    big.bytes = Bytes::from(2ull << 30);
    CHECK_THROWS(store.reserve(big));
    reservation::ReserveRequest accel;
    accel.node = NumaNodeId::from(1);
    accel.bytes = Bytes::from(65ull << 30);
    accel.accelerator = AcceleratorId::from(5);
    accel.accelerator_capacity = true;
    reservation::ReservationStore unbounded;
    CHECK_THROWS(unbounded.reserve(accel));
    CHECK(store.accounting_clean());
    CHECK(unbounded.accounting_clean());
}

// Reservation ids issued after recovery never collide with recovered reservations.
void test_recovered_reservation_ids_do_not_collide() {
    const auto path = temp_name("reservations");
    ReservationId first = ReservationId::invalid();
    {
        auto rt = Runtime::create_synthetic(two_nodes());
        rt->discover();
        reservation::ReserveRequest rr;
        rr.node = NumaNodeId::from(1);
        rr.bytes = Bytes::from(1 << 20);
        first = rt->reserve(rr).id;
        rt->persist(path);
    }
    auto rt = Runtime::create_synthetic(two_nodes());
    rt->discover();
    rt->recover(path);
    reservation::ReserveRequest rr;
    rr.node = NumaNodeId::from(1);
    rr.bytes = Bytes::from(1 << 20);
    const auto second = rt->reserve(rr).id;
    CHECK(second != first);
    CHECK(rt->reserved_bytes() == (2u << 20));
    rt->release_reservation(first);
    if (second != first) rt->release_reservation(second);
    CHECK(rt->reservations_clean());
    std::remove(path.c_str());
}

// A recovered worker is not live until it registers again: its persisted boot carries no
// authority in the recovered process.
void test_recovered_workers_not_live() {
    const auto path = temp_name("workers");
    WorkerBootId boot = WorkerBootId::invalid();
    {
        auto rt = Runtime::create_synthetic(two_nodes());
        rt->discover();
        boot = rt->register_worker(WorkerId::from(5), ProcessId::from(50)).boot_id;
        rt->persist(path);
    }
    auto c = make_coordinator();
    c->recover(path);
    CHECK(!c->runtime().workers_registry().validate_boot(WorkerId::from(5), boot));
    const auto conn = c->open_controller();
    CHECK(is_error(reserve(*c, conn, 5, boot), AuthorityVerification::StaleBoot));
    const auto fresh = register_worker(*c, conn, 5);
    CHECK(fresh != boot);
    CHECK(is_type(reserve(*c, conn, 5, fresh), protocol::MsgType::ReserveReply));
    c->close_controller(conn);
    std::remove(path.c_str());
}

// Persisted state with duplicate ids is rejected.
void test_persisted_duplicate_ids_rejected() {
    persist::PersistedState s;
    persist::PersistedWorker w;
    w.id = WorkerId::from(5);
    w.boot = WorkerBootId::from(1);
    s.workers.push_back(w);
    s.workers.push_back(w);
    CHECK_THROWS(persist::deserialize(persist::serialize(s)));
    persist::PersistedState r;
    persist::PersistedReservation pr;
    pr.id = ReservationId::from(9);
    pr.node = NumaNodeId::from(1);
    pr.bytes = 1;
    r.reservations.push_back(pr);
    r.reservations.push_back(pr);
    CHECK_THROWS(persist::deserialize(persist::serialize(r)));
}

void run_all() {
    WSADATA wsa{};
    WSAStartup(MAKEWORD(2, 2), &wsa);
    RUN_TEST(test_epoch_fences_all_mutations);
    RUN_TEST(test_release_requires_owner);
    RUN_TEST(test_superseded_bind_not_applied_to_restarted_worker);
    RUN_TEST(test_release_during_migration_refused);
    RUN_TEST(test_refused_reservation_leaves_accounting_clean);
    RUN_TEST(test_recovered_reservation_ids_do_not_collide);
    RUN_TEST(test_recovered_workers_not_live);
    RUN_TEST(test_persisted_duplicate_ids_rejected);
}

} // namespace

TEST_MAIN()
