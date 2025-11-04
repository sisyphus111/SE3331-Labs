#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent,
                                    name);
  if (res.is_err())
    return ChfsResult<inode_id_t>(res.unwrap_error());
  return ChfsResult<inode_id_t>(res.unwrap()->as<inode_id_t>());
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto res = metadata_server_->call("unlink", parent, name);
  if (res.is_err())
    return ChfsNullResult(res.unwrap_error());
  return res.unwrap()->as<bool>() ? KNullOk : ChfsNullResult(ErrorType::INVALID);
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto res = metadata_server_->call("lookup", parent, name);
  if (res.is_err())
    return ChfsResult<inode_id_t>(res.unwrap_error());
  return ChfsResult<inode_id_t>(res.unwrap()->as<inode_id_t>());
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto res = metadata_server_->call("readdir", id);
  if (res.is_err())
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(
        res.unwrap_error());
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(
      res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>());
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto res = metadata_server_->call("get_type_attr", id);
  if (res.is_err())
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
  auto t = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  FileAttr fa;
  fa.size = std::get<0>(t);
  fa.atime = std::get<1>(t);
  fa.mtime = std::get<2>(t);
  fa.ctime = std::get<3>(t);
  InodeType ty = (std::get<4>(t) == 2) ? InodeType::Directory : InodeType::FILE;
  return ChfsResult<std::pair<InodeType, FileAttr>>(std::make_pair(ty, fa));
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  if (size == 0)
    return ChfsResult<std::vector<u8>>(std::vector<u8>());
  auto map_res = metadata_server_->call("get_block_map", id);
  if (map_res.is_err())
    return ChfsResult<std::vector<u8>>(map_res.unwrap_error());
  auto mapping = map_res.unwrap()
                      ->as<std::vector<std::tuple<block_id_t, mac_id_t, version_t>>>();

  auto [first_idx, last_idx, first_off, last_sz] = dispatch_request(offset, size);
  if (mapping.size() <= last_idx)
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);

  std::vector<u8> out;
  out.reserve(size);
  for (usize i = first_idx; i <= last_idx; ++i) {
    auto [bid, mid, ver] = mapping[i];
    auto cli = data_servers_.at(mid);
    usize start = (i == first_idx) ? first_off : 0;
    usize len = (i == last_idx)
                    ? ((i == first_idx) ? last_sz - first_off : last_sz)
                    : (DiskBlockSize - start);
    auto res = cli->call("read_data", bid, start, len, ver);
    if (res.is_err())
      return ChfsResult<std::vector<u8>>(res.unwrap_error());
    auto part = res.unwrap()->as<std::vector<u8>>();
    out.insert(out.end(), part.begin(), part.end());
  }
  return ChfsResult<std::vector<u8>>(std::move(out));
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  auto map_res = metadata_server_->call("get_block_map", id);
  if (map_res.is_err())
    return ChfsNullResult(map_res.unwrap_error());
  auto mapping = map_res.unwrap()
                      ->as<std::vector<std::tuple<block_id_t, mac_id_t, version_t>>>();

  auto [first_idx, last_idx, first_off, last_sz] =
      dispatch_request(offset, data.size());

  while (mapping.size() <= last_idx) {
    auto ares = metadata_server_->call("alloc_block", id);
    if (ares.is_err())
      return ChfsNullResult(ares.unwrap_error());
    auto bi = ares.unwrap()->as<std::tuple<block_id_t, mac_id_t, version_t>>();
    mapping.push_back(bi);
  }

  usize written = 0;
  for (usize i = first_idx; i <= last_idx; ++i) {
    auto [bid, mid, _ver] = mapping[i];
    auto cli = data_servers_.at(mid);
    usize start = (i == first_idx) ? first_off : 0;
    usize len = (i == last_idx)
                    ? ((i == first_idx) ? last_sz - first_off : last_sz)
                    : (DiskBlockSize - start);
    std::vector<u8> part(data.begin() + written, data.begin() + written + len);
    auto wres = cli->call("write_data", bid, start, part);
    if (wres.is_err() || !wres.unwrap()->as<bool>())
      return ChfsNullResult(ErrorType::INVALID);
    written += len;
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  if (res.is_err())
    return ChfsNullResult(res.unwrap_error());
  return res.unwrap()->as<bool>() ? KNullOk : ChfsNullResult(ErrorType::INVALID);
}

} // namespace chfs