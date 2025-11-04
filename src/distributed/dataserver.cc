#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // Validate args
  if (len == 0 || offset + len > block_allocator_->bm->block_size()) {
    return {};
  }

  // Helper: compute where the version is stored
  auto bm = block_allocator_->bm;
  const usize versions_per_block = bm->block_size() / sizeof(version_t);
  const block_id_t ver_block_id = (block_id / versions_per_block);
  const usize ver_offset = (block_id % versions_per_block) * sizeof(version_t);

  // Read version from version block and compare
  std::vector<u8> ver_buf(bm->block_size());
  bm->read_block(ver_block_id, ver_buf.data());
  version_t stored_version =
      *reinterpret_cast<version_t *>(ver_buf.data() + ver_offset);
  if (stored_version != version) {
    return {};
  }

  // Read requested bytes
  std::vector<u8> whole(bm->block_size());
  bm->read_block(block_id, whole.data());
  return std::vector<u8>(whole.begin() + offset, whole.begin() + offset + len);
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  if (buffer.empty())
    return true;
  auto bm = block_allocator_->bm;
  if (offset + buffer.size() > bm->block_size()) {
    return false;
  }
  auto res = bm->write_partial_block(block_id, buffer.data(), offset,
                                     buffer.size());
  return res.is_ok();
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  static std::mutex mtx;
  std::lock_guard<std::mutex> lk(mtx);

  auto bm = block_allocator_->bm;
  auto alloc_res = block_allocator_->allocate();
  if (alloc_res.is_err()) {
    return {0, 0};
  }
  auto bid = alloc_res.unwrap();
  // zero newly allocated block
  bm->zero_block(bid);

  // Increase version stored in version block
  const usize versions_per_block = bm->block_size() / sizeof(version_t);
  const block_id_t ver_block_id = (bid / versions_per_block);
  const usize ver_offset = (bid % versions_per_block) * sizeof(version_t);

  std::vector<u8> ver_buf(bm->block_size());
  bm->read_block(ver_block_id, ver_buf.data());
  auto pver = reinterpret_cast<version_t *>(ver_buf.data() + ver_offset);
  *pver = static_cast<version_t>(*pver + 1);
  bm->write_block(ver_block_id, ver_buf.data());

  return {bid, *pver};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  static std::mutex mtx;
  std::lock_guard<std::mutex> lk(mtx);
  auto bm = block_allocator_->bm;

  // zero the data block
  bm->zero_block(block_id);
  // bump version to invalidate stale mappings
  const usize versions_per_block = bm->block_size() / sizeof(version_t);
  const block_id_t ver_block_id = (block_id / versions_per_block);
  const usize ver_offset = (block_id % versions_per_block) * sizeof(version_t);
  std::vector<u8> ver_buf(bm->block_size());
  bm->read_block(ver_block_id, ver_buf.data());
  auto pver = reinterpret_cast<version_t *>(ver_buf.data() + ver_offset);
  *pver = static_cast<version_t>(*pver + 1);
  bm->write_block(ver_block_id, ver_buf.data());

  // release from allocator
  auto del = block_allocator_->deallocate(block_id);
  return del.is_ok();
}
} // namespace chfs