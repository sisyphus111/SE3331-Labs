#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    int term = 0;
    int candidate_id = -1;
    int last_log_index = 0;
    int last_log_term = 0;
    
    MSGPACK_DEFINE(term, candidate_id, last_log_index, last_log_term)
};

struct RequestVoteReply {
    int term = 0;
    bool vote_granted = false;

    MSGPACK_DEFINE(term, vote_granted)
};

template <typename Command>
struct AppendEntriesArgs {
    int term = 0;
    int leader_id = -1;
    int prev_log_index = 0;
    int prev_log_term = 0;
    int leader_commit = 0;
    std::vector<LogEntry<Command>> entries;
};

struct RpcAppendEntriesArgs {
    struct RpcLogEntry {
        int term = 0;
        int size = 0;
        std::vector<u8> data;

        MSGPACK_DEFINE(term, size, data)
    };

    int term = 0;
    int leader_id = -1;
    int prev_log_index = 0;
    int prev_log_term = 0;
    int leader_commit = 0;
    std::vector<RpcLogEntry> entries;

    MSGPACK_DEFINE(term, leader_id, prev_log_index, prev_log_term, leader_commit, entries)
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    RpcAppendEntriesArgs rpc_arg;
    rpc_arg.term = arg.term;
    rpc_arg.leader_id = arg.leader_id;
    rpc_arg.prev_log_index = arg.prev_log_index;
    rpc_arg.prev_log_term = arg.prev_log_term;
    rpc_arg.leader_commit = arg.leader_commit;

    for (auto &entry : arg.entries) {
        typename RpcAppendEntriesArgs::RpcLogEntry rpc_entry;
        rpc_entry.term = entry.term;
        rpc_entry.size = entry.size;
        rpc_entry.data = entry.data;
        if (rpc_entry.size < static_cast<int>(rpc_entry.data.size())) {
            rpc_entry.data.resize(rpc_entry.size);
        } else if (rpc_entry.size > static_cast<int>(rpc_entry.data.size())) {
            rpc_entry.data.resize(rpc_entry.size, 0);
        }
        rpc_arg.entries.push_back(std::move(rpc_entry));
    }

    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    AppendEntriesArgs<Command> arg;
    arg.term = rpc_arg.term;
    arg.leader_id = rpc_arg.leader_id;
    arg.prev_log_index = rpc_arg.prev_log_index;
    arg.prev_log_term = rpc_arg.prev_log_term;
    arg.leader_commit = rpc_arg.leader_commit;

    for (auto &rpc_entry : rpc_arg.entries) {
        int sz = rpc_entry.size;
        std::vector<u8> data = rpc_entry.data;
        if (sz < static_cast<int>(data.size())) {
            data.resize(sz);
        } else if (sz > static_cast<int>(data.size())) {
            data.resize(sz, 0);
        }
        arg.entries.emplace_back(rpc_entry.term, std::move(data), sz);
    }

    return arg;
}

struct AppendEntriesReply {
    int term = 0;
    bool success = false;
    int match_index = 0;
    int next_index = 0;

    MSGPACK_DEFINE(term, success, match_index, next_index)
};

struct InstallSnapshotArgs {
    int term = 0;
    int leader_id = -1;
    int last_included_index = 0;
    int last_included_term = 0;
    std::vector<u8> data;

    MSGPACK_DEFINE(term, leader_id, last_included_index, last_included_term, data)
};

struct InstallSnapshotReply {
    int term = 0;
    bool success = false;
    int applied_index = 0;

    MSGPACK_DEFINE(term, success, applied_index)
};

} /* namespace chfs */