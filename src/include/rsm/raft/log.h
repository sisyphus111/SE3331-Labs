#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <utility>
#include <algorithm>

namespace chfs {

template <typename Command>
class LogEntry {
public:
    LogEntry(): term(0), size(0) {}

    LogEntry(int t, const Command &cmd): term(t)
    {
        size = cmd.size();
        data = cmd.serialize(size);
    }

    LogEntry(int t, const std::vector<u8> &raw, int sz): term(t), data(raw), size(sz) {}

    LogEntry(int t, std::vector<u8> &&raw, int sz): term(t), data(std::move(raw)), size(sz) {}

    auto has_command() const -> bool
    {
        return size > 0 && data.size() >= static_cast<size_t>(size);
    }

    auto to_command() const -> Command
    {
        Command cmd;
        if (has_command()) {
            cmd.deserialize(data, size);
        }
        return cmd;
    }

    int term;
    std::vector<u8> data;
    int size;
};


/** 
 * RaftLog uses a BlockManager to manage the data.
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog() = default;

    struct PersistedState {
        int current_term = 0;
        int voted_for = -1;
        int last_included_index = 0;
        int last_included_term = 0;
        std::vector<u8> snapshot;
        std::vector<LogEntry<Command>> entries;
    };

    auto save(const PersistedState &state) -> void;
    auto load(PersistedState &state) -> bool;

private:
    static constexpr int kMagic = 0x52414654;   /* 'RAFT' */
    static constexpr int kVersion = 1;

    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
    : bm_(std::move(bm))
{
}

template <typename Command>
auto RaftLog<Command>::save(const PersistedState &state) -> void
{
    if (bm_ == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(mtx);

    std::vector<u8> buffer;
    buffer.reserve(1024);

    auto append_int = [&buffer](int value) {
        u8 temp[sizeof(int)];
        std::memcpy(temp, &value, sizeof(int));
        buffer.insert(buffer.end(), temp, temp + sizeof(int));
    };

    append_int(kMagic);
    append_int(0); /* placeholder for total size */
    append_int(kVersion);
    append_int(state.current_term);
    append_int(state.voted_for);
    append_int(state.last_included_index);
    append_int(state.last_included_term);
    append_int(static_cast<int>(state.snapshot.size()));
    append_int(static_cast<int>(state.entries.size()));

    buffer.insert(buffer.end(), state.snapshot.begin(), state.snapshot.end());

    for (const auto &entry : state.entries) {
        append_int(entry.term);
        append_int(entry.size);
        if (entry.size > 0 && entry.data.size() >= static_cast<size_t>(entry.size)) {
            buffer.insert(buffer.end(), entry.data.begin(), entry.data.begin() + entry.size);
        }
    }

    int total_size = static_cast<int>(buffer.size());
    std::memcpy(buffer.data() + sizeof(int), &total_size, sizeof(int));

    const usize block_sz = bm_->block_size();
    if (block_sz == 0) {
        return;
    }

    usize blocks = (buffer.size() + block_sz - 1) / block_sz;
    if (blocks == 0) {
        blocks = 1;
    }

    std::vector<u8> block(block_sz, 0);
    size_t offset = 0;
    for (usize block_id = 0; block_id < blocks; block_id++) {
        std::fill(block.begin(), block.end(), 0);
        size_t copy_len = std::min(static_cast<size_t>(block_sz), buffer.size() - offset);
        std::memcpy(block.data(), buffer.data() + offset, copy_len);
        bm_->write_block(block_id, block.data());
        offset += copy_len;
    }

    if (offset % block_sz != 0 && blocks < bm_->total_blocks()) {
        bm_->zero_block(blocks);
    }

    bm_->flush();
}

template <typename Command>
auto RaftLog<Command>::load(PersistedState &state) -> bool
{
    if (bm_ == nullptr) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mtx);

    const usize block_sz = bm_->block_size();
    if (block_sz == 0) {
        return false;
    }

    std::vector<u8> block(block_sz, 0);
    if (bm_->read_block(0, block.data()).is_err()) {
        return false;
    }

    int magic = 0;
    std::memcpy(&magic, block.data(), sizeof(int));
    if (magic != kMagic) {
        return false;
    }

    int total_size = 0;
    std::memcpy(&total_size, block.data() + sizeof(int), sizeof(int));
    if (total_size <= 0 || total_size > static_cast<int>(bm_->total_storage_sz())) {
        return false;
    }

    std::vector<u8> buffer(total_size, 0);
    size_t copied = std::min(static_cast<size_t>(block_sz), buffer.size());
    std::memcpy(buffer.data(), block.data(), copied);

    size_t offset = copied;
    block_id_t block_id = 1;
    while (offset < buffer.size()) {
        if (bm_->read_block(block_id, block.data()).is_err()) {
            return false;
        }
        size_t chunk = std::min(static_cast<size_t>(block_sz), buffer.size() - offset);
        std::memcpy(buffer.data() + offset, block.data(), chunk);
        offset += chunk;
        block_id++;
    }

    size_t cursor = 0;
    auto consume_int = [&buffer, &cursor](int &value) -> bool {
        if (cursor + sizeof(int) > buffer.size()) {
            return false;
        }
        std::memcpy(&value, buffer.data() + cursor, sizeof(int));
        cursor += sizeof(int);
        return true;
    };

    int version = 0;
    int snapshot_size = 0;
    int log_count = 0;
    int stored_total_size = 0;

    if (!consume_int(magic) || !consume_int(stored_total_size) || !consume_int(version)
        || !consume_int(state.current_term) || !consume_int(state.voted_for)
        || !consume_int(state.last_included_index) || !consume_int(state.last_included_term)
        || !consume_int(snapshot_size) || !consume_int(log_count)) {
        return false;
    }

    if (magic != kMagic || version != kVersion || stored_total_size != total_size) {
        return false;
    }

    if (snapshot_size < 0 || log_count < 0) {
        return false;
    }

    if (cursor + static_cast<size_t>(snapshot_size) > buffer.size()) {
        return false;
    }

    state.snapshot.assign(buffer.begin() + cursor, buffer.begin() + cursor + snapshot_size);
    cursor += snapshot_size;

    state.entries.clear();
    state.entries.reserve(log_count);
    for (int i = 0; i < log_count; i++) {
        int entry_term = 0;
        int entry_size = 0;
        if (!consume_int(entry_term) || !consume_int(entry_size)) {
            return false;
        }
        if (entry_size < 0 || cursor + static_cast<size_t>(entry_size) > buffer.size()) {
            return false;
        }
        std::vector<u8> entry_data(entry_size);
        if (entry_size > 0) {
            std::memcpy(entry_data.data(), buffer.data() + cursor, entry_size);
        }
        cursor += entry_size;
        state.entries.emplace_back(entry_term, std::move(entry_data), entry_size);
    }

    return true;
}

} /* namespace chfs */
