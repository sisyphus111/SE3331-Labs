#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>
#include <condition_variable>
#include <vector>
#include <map>
#include <random>
#include <tuple>
#include <string>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    std::vector<LogEntry<Command>> logs;
    int log_base_index;
    int log_base_term;
    int commit_index;
    int last_applied;
    int voted_for;

    std::vector<int> next_index;
    std::vector<int> match_index;
    std::vector<bool> votes_received;
    int vote_granted_cnt;

    std::chrono::steady_clock::time_point last_election_reset;
    std::chrono::milliseconds election_timeout;
    std::chrono::milliseconds heartbeat_interval;

    std::mt19937 random_engine;

    std::vector<u8> snapshot_data;
    std::condition_variable apply_cv;

    std::string log_file_path;

    auto num_nodes() const -> int { return static_cast<int>(node_configs.size()); }
    auto majority() const -> int { return num_nodes() / 2 + 1; }
    auto get_last_log_index() const -> int { return log_base_index + static_cast<int>(logs.size()) - 1; }
    auto get_last_log_term() const -> int { return logs.empty() ? log_base_term : logs.back().term; }
    auto term_at(int index) const -> int;
    auto log_up_to_date(int index, int term) const -> bool;
    void reset_election_timer_locked();
    auto random_timeout_ms() -> int;
    void persist_locked();
    void truncate_prefix_locked(int index, int term);
    void step_down_locked(int new_term, int new_leader);
    void start_election_locked(std::vector<int> &targets, RequestVoteArgs &args);
    void become_leader_locked();
    AppendEntriesArgs<Command> build_append_entries_locked(int target, bool empty_only);
    InstallSnapshotArgs build_install_snapshot_locked();
    void update_commit_index_locked();
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

    thread_pool = std::make_unique<ThreadPool>(std::max(4u, std::thread::hardware_concurrency()));
    state = std::make_unique<StateMachine>();

    for (auto &config : node_configs) {
        if (config.node_id == my_id) {
            rpc_clients_map[config.node_id] = nullptr;
        } else {
            rpc_clients_map[config.node_id] = std::make_unique<RpcClient>(config.ip_address, config.port, true);
        }
    }

    std::filesystem::create_directories("/tmp/raft_log");
    log_file_path = (std::filesystem::path("/tmp/raft_log") / ("node_" + std::to_string(my_id) + ".bin")).string();
    auto bm = std::make_shared<BlockManager>(log_file_path, 128);
    log_storage = std::make_unique<RaftLog<Command>>(bm);

    logs.clear();
    logs.emplace_back(0, std::vector<u8>(), 0);
    log_base_index = 0;
    log_base_term = 0;
    commit_index = 0;
    last_applied = 0;
    voted_for = -1;
    vote_granted_cnt = 0;
    snapshot_data.clear();

    std::random_device rd;
    random_engine.seed(rd());
    heartbeat_interval = std::chrono::milliseconds(100);
    last_election_reset = std::chrono::steady_clock::now();
    election_timeout = std::chrono::milliseconds(random_timeout_ms());

    next_index.assign(num_nodes(), 1);
    match_index.assign(num_nodes(), 0);
    votes_received.assign(num_nodes(), false);

    if (log_storage) {
        typename RaftLog<Command>::PersistedState persisted;
        if (log_storage->load(persisted)) {
            current_term = persisted.current_term;
            voted_for = persisted.voted_for;
            log_base_index = persisted.last_included_index;
            log_base_term = persisted.last_included_term;
            logs.clear();
            logs.emplace_back(log_base_term, std::vector<u8>(), 0);
            for (auto &entry : persisted.entries) {
                logs.push_back(entry);
            }
            snapshot_data = persisted.snapshot;
            commit_index = log_base_index;
            last_applied = log_base_index;
            if (!snapshot_data.empty()) {
                state->apply_snapshot(snapshot_data);
            }
        }
    }

    if (match_index.size() > static_cast<size_t>(my_id)) {
        match_index[my_id] = get_last_log_index();
    }
    if (next_index.size() > static_cast<size_t>(my_id)) {
        next_index[my_id] = get_last_log_index() + 1;
    }

    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    bool expected = true;
    if (!stopped.compare_exchange_strong(expected, false)) {
        return 0;
    }

    {
        std::unique_lock<std::mutex> lock(mtx);
        role = RaftRole::Follower;
        leader_id = -1;
        reset_election_timer_locked();
    }

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    bool expected = false;
    if (!stopped.compare_exchange_strong(expected, true)) {
        return 0;
    }

    apply_cv.notify_all();

    if (background_election && background_election->joinable()) {
        background_election->join();
    }
    if (background_ping && background_ping->joinable()) {
        background_ping->join();
    }
    if (background_commit && background_commit->joinable()) {
        background_commit->join();
    }
    if (background_apply && background_apply->joinable()) {
        background_apply->join();
    }

    background_election.reset();
    background_ping.reset();
    background_commit.reset();
    background_apply.reset();

    std::unique_lock<std::mutex> lock(mtx);
    role = RaftRole::Follower;
    leader_id = -1;

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    std::unique_lock<std::mutex> lock(mtx);
    bool is_leader_now = (role == RaftRole::Leader);
    return std::make_tuple(is_leader_now, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    std::unique_lock<std::mutex> lock(mtx);
    if (stopped.load() || role != RaftRole::Leader) {
        return std::make_tuple(false, current_term, -1);
    }

    if (cmd_size < 0) {
        cmd_size = 0;
    }
    if (static_cast<int>(cmd_data.size()) < cmd_size) {
        cmd_data.resize(cmd_size, 0);
    } else if (static_cast<int>(cmd_data.size()) > cmd_size) {
        cmd_data.resize(cmd_size);
    }

    logs.emplace_back(current_term, cmd_data, cmd_size);
    int log_idx = get_last_log_index();
    if (match_index.size() > static_cast<size_t>(my_id)) {
        match_index[my_id] = log_idx;
    }
    if (next_index.size() > static_cast<size_t>(my_id)) {
        next_index[my_id] = log_idx + 1;
    }

    persist_locked();

    return std::make_tuple(true, current_term, log_idx);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    if (is_stopped()) {
        return false;
    }

    std::unique_lock<std::mutex> lock(mtx);
    if (last_applied <= log_base_index) {
        return true;
    }

    int snapshot_index = last_applied;
    int snapshot_term = term_at(snapshot_index);

    auto snapshot = state->snapshot();
    snapshot_data = snapshot;

    truncate_prefix_locked(snapshot_index, snapshot_term);
    commit_index = std::max(commit_index, snapshot_index);
    last_applied = std::max(last_applied, snapshot_index);

    persist_locked();

    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    std::unique_lock<std::mutex> lock(mtx);
    RequestVoteReply reply;
    reply.term = current_term;
    reply.vote_granted = false;

    if (args.term < current_term) {
        RAFT_LOG("reject RequestVote from %d: stale term %d < %d",
                 args.candidate_id, args.term, current_term);
        return reply;
    }

    if (args.term > current_term) {
        step_down_locked(args.term, -1);
    }

    reply.term = current_term;

    bool can_vote = (voted_for == -1 || voted_for == args.candidate_id);
    bool up_to_date = log_up_to_date(args.last_log_index, args.last_log_term);

    if (can_vote && up_to_date) {
        voted_for = args.candidate_id;
        reply.vote_granted = true;
        role = RaftRole::Follower;
        leader_id = -1;
        reset_election_timer_locked();
        persist_locked();

        RAFT_LOG("grant vote to %d in term %d, last_log=(%d,%d)",
                 args.candidate_id, current_term,
                 args.last_log_index, args.last_log_term);
    } else {
        RAFT_LOG("deny vote to %d in term %d, can_vote=%d up_to_date=%d "
                 "voted_for=%d my_last_log=(%d,%d) cand_last_log=(%d,%d)",
                 args.candidate_id, current_term,
                 can_vote ? 1 : 0, up_to_date ? 1 : 0,
                 voted_for,
                 get_last_log_index(), get_last_log_term(),
                 args.last_log_index, args.last_log_term);
    }

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        RAFT_LOG("step down due to higher term in RequestVoteReply from %d: %d > %d",
                 target, reply.term, current_term);
        step_down_locked(reply.term, -1);
        return;
    }

    if (role != RaftRole::Candidate) {
        return;
    }

    if (reply.term < current_term) {
        return;
    }

    if (!reply.vote_granted) {
        return;
    }

    if (target >= 0 && target < num_nodes()) {
        if (votes_received[target]) {
            return;
        }
        votes_received[target] = true;
    }

    vote_granted_cnt++;
    RAFT_LOG("receive vote from %d in term %d, votes=%d/%d",
             target, reply.term, vote_granted_cnt, majority());
    if (vote_granted_cnt >= majority()) {
        become_leader_locked();
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);

    std::unique_lock<std::mutex> lock(mtx);
    AppendEntriesReply reply;
    reply.term = current_term;
    reply.success = false;
    reply.match_index = get_last_log_index();
    reply.next_index = get_last_log_index() + 1;

    if (arg.term < current_term) {
        RAFT_LOG("reject AppendEntries from %d: stale term %d < %d",
                 arg.leader_id, arg.term, current_term);
        return reply;
    }

    if (arg.term > current_term) {
        step_down_locked(arg.term, arg.leader_id);
    }

    role = RaftRole::Follower;
    leader_id = arg.leader_id;
    reset_election_timer_locked();

    if (arg.prev_log_index < log_base_index) {
        reply.next_index = log_base_index + 1;
        RAFT_LOG("AppendEntries conflict: prev_log_index %d < log_base_index %d",
                 arg.prev_log_index, log_base_index);
        return reply;
    }

    if (arg.prev_log_index > get_last_log_index()) {
        reply.next_index = get_last_log_index() + 1;
        RAFT_LOG("AppendEntries conflict: prev_log_index %d > last_log_index %d",
                 arg.prev_log_index, get_last_log_index());
        return reply;
    }

    if (term_at(arg.prev_log_index) != arg.prev_log_term) {
        int conflict_term = term_at(arg.prev_log_index);
        int idx = arg.prev_log_index;
        while (idx > log_base_index && term_at(idx - 1) == conflict_term) {
            idx--;
        }
        reply.next_index = idx;
        RAFT_LOG("AppendEntries term mismatch at %d: expected %d, got %d, fallback next_index=%d",
                 arg.prev_log_index, arg.prev_log_term, conflict_term, reply.next_index);
        return reply;
    }

    int index = arg.prev_log_index;
    bool changed = false;
    for (auto &entry : arg.entries) {
        index++;
        if (index <= get_last_log_index()) {
            if (term_at(index) != entry.term) {
                int erase_pos = index - log_base_index;
                logs.erase(logs.begin() + erase_pos, logs.end());
                logs.push_back(entry);
                changed = true;
            }
        } else {
            logs.push_back(entry);
            changed = true;
        }
    }

    if (changed) {
        persist_locked();
    }

    if (arg.leader_commit > commit_index) {
        commit_index = std::min(arg.leader_commit, get_last_log_index());
        apply_cv.notify_all();
    }

    reply.term = current_term;
    reply.success = true;
    reply.match_index = arg.prev_log_index + static_cast<int>(arg.entries.size());
    if (reply.match_index < arg.prev_log_index) {
        reply.match_index = arg.prev_log_index;
    }
    reply.next_index = reply.match_index + 1;

    RAFT_LOG("AppendEntries from leader %d term %d: prev=(%d,%d) entries=%zu "
             "leader_commit=%d -> success=%d match_index=%d next_index=%d",
             arg.leader_id, arg.term, arg.prev_log_index, arg.prev_log_term,
             arg.entries.size(), arg.leader_commit,
             reply.success ? 1 : 0, reply.match_index, reply.next_index);
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        step_down_locked(reply.term, -1);
        return;
    }

    if (role != RaftRole::Leader) {
        return;
    }

    if (node_id < 0 || node_id >= num_nodes()) {
        return;
    }

    if (reply.success) {
        match_index[node_id] = std::max(match_index[node_id], reply.match_index);
        next_index[node_id] = reply.match_index + 1;
        update_commit_index_locked();
    } else {
        int fallback = reply.next_index;
        if (fallback <= log_base_index) {
            fallback = log_base_index + 1;
        }
        next_index[node_id] = fallback;
    }
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    InstallSnapshotReply reply;
    reply.term = current_term;
    reply.success = false;
    reply.applied_index = last_applied;

    std::vector<u8> snapshot = args.data;

    std::unique_lock<std::mutex> lock(mtx);
    if (args.term < current_term) {
        return reply;
    }

    if (args.term > current_term) {
        step_down_locked(args.term, args.leader_id);
    }

    role = RaftRole::Follower;
    leader_id = args.leader_id;
    reset_election_timer_locked();

    if (args.last_included_index <= log_base_index) {
        reply.success = true;
        reply.term = current_term;
        reply.applied_index = log_base_index;
        return reply;
    }

    snapshot_data = snapshot;
    truncate_prefix_locked(args.last_included_index, args.last_included_term);
    commit_index = std::max(commit_index, args.last_included_index);
    last_applied = std::max(last_applied, args.last_included_index);
    persist_locked();

    reply.success = true;
    reply.term = current_term;
    reply.applied_index = args.last_included_index;

    lock.unlock();
    state->apply_snapshot(snapshot);

    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        step_down_locked(reply.term, -1);
        return;
    }

    if (role != RaftRole::Leader) {
        return;
    }

    if (!reply.success) {
        return;
    }

    if (node_id < 0 || node_id >= num_nodes()) {
        return;
    }

    match_index[node_id] = std::max(match_index[node_id], reply.applied_index);
    next_index[node_id] = reply.applied_index + 1;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    while (true) {
        if (is_stopped()) {
            return;
        }

        std::vector<int> targets;
        RequestVoteArgs args{};
        bool should_start = false;

        {
            std::unique_lock<std::mutex> lock(mtx);
            auto now = std::chrono::steady_clock::now();
            if (role != RaftRole::Leader && now - last_election_reset >= election_timeout) {
                start_election_locked(targets, args);
                should_start = true;
            }
        }

        if (should_start) {
            for (auto target : targets) {
                send_request_vote(target, args);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) {
            return;
        }

        std::vector<std::pair<int, AppendEntriesArgs<Command>>> tasks;
        std::vector<std::pair<int, InstallSnapshotArgs>> snapshots;

        {
            std::unique_lock<std::mutex> lock(mtx);
            if (role == RaftRole::Leader) {
                int last_index = get_last_log_index();
                for (auto &config : node_configs) {
                    if (config.node_id == my_id) {
                        continue;
                    }

                    int follower_id = config.node_id;
                    if (follower_id >= num_nodes()) {
                        continue;
                    }

                    if (next_index[follower_id] <= log_base_index) {
                        if (log_base_index > 0 && !snapshot_data.empty()) {
                            snapshots.emplace_back(follower_id, build_install_snapshot_locked());
                        }
                    } else if (next_index[follower_id] <= last_index) {
                        tasks.emplace_back(follower_id, build_append_entries_locked(follower_id, false));
                    }
                }
            }
        }

        for (auto &job : snapshots) {
            send_install_snapshot(job.first, job.second);
        }

        for (auto &job : tasks) {
            send_append_entries(job.first, job.second);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        if (is_stopped()) {
            return;
        }

        std::vector<LogEntry<Command>> entries;
        {
            std::unique_lock<std::mutex> lock(mtx);
            apply_cv.wait_for(lock, std::chrono::milliseconds(50), [&]() {
                return stopped.load() || commit_index > last_applied;
            });

            if (stopped.load()) {
                return;
            }

            if (commit_index <= last_applied) {
                continue;
            }

            int start = last_applied + 1;
            int end = commit_index;
            for (int idx = start; idx <= end; ++idx) {
                if (idx <= log_base_index) {
                    continue;
                }
                int vec_idx = idx - log_base_index;
                if (vec_idx >= 0 && vec_idx < static_cast<int>(logs.size())) {
                    entries.push_back(logs[vec_idx]);
                }
            }
            last_applied = end;
        }

        for (auto &entry : entries) {
            if (entry.size == 0) {
                continue;
            }
            Command cmd;
            cmd.deserialize(entry.data, entry.size);
            state->apply_log(cmd);
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) {
            return;
        }

        std::vector<std::pair<int, AppendEntriesArgs<Command>>> heartbeats;
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (role == RaftRole::Leader) {
                for (auto &config : node_configs) {
                    if (config.node_id == my_id) {
                        continue;
                    }
                    heartbeats.emplace_back(config.node_id, build_append_entries_locked(config.node_id, true));
                }
            }
        }

        for (auto &hb : heartbeats) {
            send_append_entries(hb.first, hb.second);
        }

        std::this_thread::sleep_for(heartbeat_interval);
    }
}

/******************************************************************

                       Helper Functions

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::term_at(int index) const -> int
{
    if (index <= log_base_index) {
        return log_base_term;
    }

    int vec_idx = index - log_base_index;
    if (vec_idx >= 0 && vec_idx < static_cast<int>(logs.size())) {
        return logs[vec_idx].term;
    }

    return logs.empty() ? log_base_term : logs.back().term;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::log_up_to_date(int index, int term) const -> bool
{
    int my_term = get_last_log_term();
    if (term != my_term) {
        return term > my_term;
    }

    return index >= get_last_log_index();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::random_timeout_ms() -> int
{
    std::uniform_int_distribution<int> dist(300, 600);
    return dist(random_engine);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::reset_election_timer_locked()
{
    last_election_reset = std::chrono::steady_clock::now();
    election_timeout = std::chrono::milliseconds(random_timeout_ms());
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::persist_locked()
{
    if (!log_storage) {
        return;
    }

    typename RaftLog<Command>::PersistedState state;
    state.current_term = current_term;
    state.voted_for = voted_for;
    state.last_included_index = log_base_index;
    state.last_included_term = log_base_term;
    state.snapshot = snapshot_data;

    for (size_t i = 1; i < logs.size(); ++i) {
        state.entries.push_back(logs[i]);
    }

    log_storage->save(state);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::truncate_prefix_locked(int index, int term)
{
    if (logs.empty()) {
        logs.emplace_back(term, std::vector<u8>(), 0);
    }

    if (index <= log_base_index) {
        log_base_term = term;
        logs.front().term = term;
        logs.front().data.clear();
        logs.front().size = 0;
        return;
    }

    int remove = index - log_base_index;
    if (remove < 0) {
        remove = 0;
    }

    std::vector<LogEntry<Command>> new_logs;
    new_logs.emplace_back(term, std::vector<u8>(), 0);

    size_t start = std::min(static_cast<size_t>(remove + 1), logs.size());
    for (size_t i = start; i < logs.size(); ++i) {
        new_logs.push_back(logs[i]);
    }

    logs.swap(new_logs);
    log_base_index = index;
    log_base_term = term;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::step_down_locked(int new_term, int new_leader)
{
    if (new_term > current_term) {
        current_term = new_term;
        voted_for = -1;
    }
    role = RaftRole::Follower;
    leader_id = new_leader;
    vote_granted_cnt = 0;
    std::fill(votes_received.begin(), votes_received.end(), false);
    persist_locked();
    reset_election_timer_locked();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::start_election_locked(std::vector<int> &targets, RequestVoteArgs &args)
{
    current_term++;
    role = RaftRole::Candidate;
    leader_id = -1;
    voted_for = my_id;
    vote_granted_cnt = 1;
    std::fill(votes_received.begin(), votes_received.end(), false);
    if (my_id >= 0 && my_id < num_nodes()) {
        votes_received[my_id] = true;
    }

    args.term = current_term;
    args.candidate_id = my_id;
    args.last_log_index = get_last_log_index();
    args.last_log_term = term_at(args.last_log_index);

    RAFT_LOG("start election: term=%d, candidate=%d, last_log=(%d,%d)",
             current_term, my_id, args.last_log_index, args.last_log_term);

    persist_locked();
    reset_election_timer_locked();

    targets.clear();
    for (auto &config : node_configs) {
        if (config.node_id == my_id) {
            continue;
        }
        targets.push_back(config.node_id);
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_leader_locked()
{
    role = RaftRole::Leader;
    leader_id = my_id;

    RAFT_LOG("become leader at term %d, last_log_index=%d",
             current_term, get_last_log_index());

    int next = get_last_log_index() + 1;
    for (int i = 0; i < num_nodes(); ++i) {
        next_index[i] = next;
        match_index[i] = log_base_index;
    }
    if (my_id >= 0 && my_id < num_nodes()) {
        match_index[my_id] = get_last_log_index();
    }
}

template <typename StateMachine, typename Command>
AppendEntriesArgs<Command> RaftNode<StateMachine, Command>::build_append_entries_locked(int target, bool empty_only)
{
    AppendEntriesArgs<Command> args;
    args.term = current_term;
    args.leader_id = my_id;
    args.leader_commit = commit_index;

    int next = (target >= 0 && target < num_nodes()) ? next_index[target] : (get_last_log_index() + 1);
    if (next < log_base_index + 1) {
        next = log_base_index + 1;
    }
    int prev = next - 1;
    args.prev_log_index = prev;
    args.prev_log_term = term_at(prev);

    if (!empty_only) {
        int last = get_last_log_index();
        const int max_batch = 32;
        for (int idx = next; idx <= last && static_cast<int>(args.entries.size()) < max_batch; ++idx) {
            int vec_idx = idx - log_base_index;
            if (vec_idx >= 0 && vec_idx < static_cast<int>(logs.size())) {
                args.entries.push_back(logs[vec_idx]);
            }
        }
    }

    return args;
}

template <typename StateMachine, typename Command>
InstallSnapshotArgs RaftNode<StateMachine, Command>::build_install_snapshot_locked()
{
    InstallSnapshotArgs args;
    args.term = current_term;
    args.leader_id = my_id;
    args.last_included_index = log_base_index;
    args.last_included_term = log_base_term;
    if (!snapshot_data.empty()) {
        args.data = snapshot_data;
    } else {
        args.data = state->snapshot();
    }
    return args;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::update_commit_index_locked()
{
    int last = get_last_log_index();
    for (int idx = last; idx > commit_index; --idx) {
        if (term_at(idx) != current_term) {
            continue;
        }

        int cnt = 0;
        for (int i = 0; i < num_nodes(); ++i) {
            if (match_index[i] >= idx) {
                cnt++;
            }
        }

        if (cnt >= majority()) {
            commit_index = idx;
            apply_cv.notify_all();
            RAFT_LOG("update commit_index=%d (term=%d, agree=%d/%d)",
                     commit_index, term_at(commit_index), cnt, majority());
            break;
        }
    }
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}