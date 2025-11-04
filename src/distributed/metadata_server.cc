#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>
#include <sstream>
#include <mutex>
#include <algorithm>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  std::lock_guard<std::mutex> lk(mtx_);
  if (type == RegularFileType) {
    auto res = operation_->mkfile(parent, name.c_str());
    if (res.is_ok())
      return res.unwrap();
    return 0;
  } else if (type == DirectoryType) {
    auto res = operation_->mkdir(parent, name.c_str());
    if (res.is_ok())
      return res.unwrap();
    return 0;
  }
  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  std::lock_guard<std::mutex> lk(mtx_);
  // Lookup target inode
  auto lookup_res = operation_->lookup(parent, name.c_str());
  if (lookup_res.is_err())
    return false;
  auto target = lookup_res.unwrap();

  // Check type
  auto type_res = operation_->gettype(target);
  if (type_res.is_err())
    return false;
  if (type_res.unwrap() == InodeType::Directory) {
    // Remove the entry from parent directory file content and free inode
    auto rd = operation_->read_file(parent);
    if (rd.is_err())
      return false;
    std::string dir_str(rd.unwrap().begin(), rd.unwrap().end());
    auto updated = rm_from_directory(dir_str, name);
    std::vector<u8> buf(updated.begin(), updated.end());
    if (operation_->write_file(parent, buf).is_err())
      return false;
    if (operation_->inode_manager_->free_inode(target).is_err())
      return false;
    return true;
  }
  auto res = operation_->unlink(parent, name.c_str());
  return res.is_ok();
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  auto res = operation_->lookup(parent, name.c_str());
  if (res.is_ok())
    return res.unwrap();
  return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  std::lock_guard<std::mutex> lk(mtx_);
  auto rd = operation_->read_file(id);
  if (rd.is_err())
    return {};
  auto bytes = rd.unwrap();
  if (bytes.empty())
    return {};
  // stored as msgpack vector<BlockInfo>
  try {
    return deserialize_object<std::vector<BlockInfo>>(bytes);
  } catch (...) {
    return {};
  }
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  std::lock_guard<std::mutex> lk(mtx_);

  if (num_data_servers == 0)
    return {0, 0, 0};
  auto machine_id = generator.rand(1, num_data_servers);
  auto cli_it = clients_.find(machine_id);
  if (cli_it == clients_.end())
    return {0, 0, 0};
  auto res = cli_it->second->call("alloc_block");
  if (res.is_err())
    return {0, 0, 0};
  auto [block_id, version] =
      res.unwrap()->as<std::pair<block_id_t, version_t>>();

  // Append mapping and persist (read without calling get_block_map to avoid re-lock)
  std::vector<BlockInfo> mapping;
  auto rd = operation_->read_file(id);
  if (rd.is_ok()) {
    auto bytes = rd.unwrap();
    if (!bytes.empty()) {
      try {
        mapping = deserialize_object<std::vector<BlockInfo>>(bytes);
      } catch (...) {
        mapping.clear();
      }
    }
  }
  mapping.emplace_back(block_id, machine_id, version);
  auto bytes = serialize_object(mapping);
  if (operation_->write_file(id, bytes).is_err())
    return {0, 0, 0};

  return {block_id, machine_id, version};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  std::lock_guard<std::mutex> lk(mtx_);

  auto it = clients_.find(machine_id);
  if (it == clients_.end())
    return false;
  auto del_res = it->second->call("free_block", block_id);
  if (del_res.is_err() || !del_res.unwrap()->as<bool>())
    return false;

  // update mapping
  std::vector<BlockInfo> mapping;
  auto rd = operation_->read_file(id);
  if (rd.is_ok()) {
    auto bytes = rd.unwrap();
    if (!bytes.empty()) {
      try {
        mapping = deserialize_object<std::vector<BlockInfo>>(bytes);
      } catch (...) {
        mapping.clear();
      }
    }
  }
  bool removed = false;
  mapping.erase(std::remove_if(mapping.begin(), mapping.end(),
                               [&](const BlockInfo &bi) {
                                 if (removed)
                                   return false;
                                 auto b = std::get<0>(bi);
                                 auto m = std::get<1>(bi);
                                 if (b == block_id && m == machine_id) {
                                   removed = true;
                                   return true;
                                 }
                                 return false;
                               }),
                mapping.end());
  auto bytes = serialize_object(mapping);
  if (operation_->write_file(id, bytes).is_err())
    return false;
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  std::lock_guard<std::mutex> lk(mtx_);
  std::list<DirectoryEntry> list;
  if (read_directory(operation_.get(), node, list).is_err())
    return {};
  std::vector<std::pair<std::string, inode_id_t>> out;
  for (auto &e : list) {
    out.emplace_back(e.name, e.id);
  }
  return out;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  std::lock_guard<std::mutex> lk(mtx_);
  auto ta = operation_->get_type_attr(id);
  if (ta.is_err())
    return {0, 0, 0, 0, 0};
  auto [ty, attr] = ta.unwrap();
  if (ty == InodeType::FILE) {
    auto mapping = get_block_map(id);
    attr.size = mapping.size() * DiskBlockSize;
  }
  return {attr.size, attr.atime, attr.mtime, attr.ctime,
          ty == InodeType::Directory ? DirectoryType : RegularFileType};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs