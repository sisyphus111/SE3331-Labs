#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {
  // Append the new directory entry formatted as "name:inode"
  if (!src.empty() && src.back() != '/') {
    src.push_back('/');
  }
  src += filename + ":" + inode_id_to_string(id);
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {
  list.clear();
  if (src.empty()) {
    return;
  }
  size_t start = 0;
  while (start <= src.size()) {
    size_t end = src.find('/', start);
    if (end == std::string::npos) {
      end = src.size();
    }
    if (end > start) {
      std::string token = src.substr(start, end - start);
      // token expected format: name:inode
      auto pos = token.find(':');
      if (pos != std::string::npos) {
        DirectoryEntry entry;
        entry.name = token.substr(0, pos);
        auto id_str = token.substr(pos + 1);
        entry.id = string_to_inode_id(id_str);
        list.push_back(std::move(entry));
      }
    }
    if (end == src.size()) break;
    start = end + 1;
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");
  // Parse, filter, and serialize back
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  list.remove_if([&](const DirectoryEntry &e) { return e.name == filename; });
  res = dir_list_to_string(list);
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  auto res = fs->read_file(id);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }
  auto buf = res.unwrap();
  std::string input(buf.begin(), buf.end());
  parse_directory(input, list);
  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;
  auto rd = read_directory(this, id, list);
  if (rd.is_err()) {
    return ChfsResult<inode_id_t>(rd.unwrap_error());
  }
  for (const auto &e : list) {
    if (e.name == std::string(name)) {
      return ChfsResult<inode_id_t>(e.id);
    }
  }
  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  auto lookup_res = this->lookup(id, name);


  if(lookup_res.is_ok()){
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }

  // 2. Create the new inode.
  auto new_inode_res = this->alloc_inode(type);
  if (new_inode_res.is_err()) {
    return ChfsResult<inode_id_t>(new_inode_res.unwrap_error());
  }
  auto new_inode_id = new_inode_res.unwrap();

  // 3. Append the new entry to the parent directory.
  auto rd = this->read_file(id);
  if (rd.is_err()) {
    return ChfsResult<inode_id_t>(rd.unwrap_error());
  }

  auto dir = rd.unwrap();
  std::string dir_str(dir.begin(), dir.end());
  auto updated = append_to_directory(dir_str, name, new_inode_id);
  std::vector<u8> out(updated.begin(), updated.end());
  auto wr = this->write_file(id, out);

  
  if (wr.is_err()) {
    return ChfsResult<inode_id_t>(wr.unwrap_error());
  }

  return ChfsResult<inode_id_t>(new_inode_id);
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {
  // Find target
  auto target = this->lookup(parent, name);
  if (target.is_err()) {
    return ChfsNullResult(target.unwrap_error());
  }
  auto tid = target.unwrap();

  // Do not remove directories in this lab (return NotEmpty as spec says)
  auto type_res = this->gettype(tid);
  if (type_res.is_err()) {
    return ChfsNullResult(type_res.unwrap_error());
  }
  if (type_res.unwrap() == InodeType::Directory) {
    return ChfsNullResult(ErrorType::NotEmpty);
  }

  // 1. Remove the file contents and inode
  auto rm = this->remove_file(tid);
  if (rm.is_err()) {
    return rm;
  }

  // 2. Remove entry from parent directory
  auto rd = this->read_file(parent);
  if (rd.is_err()) {
    return ChfsNullResult(rd.unwrap_error());
  }
  std::string dir_str(rd.unwrap().begin(), rd.unwrap().end());
  auto updated = rm_from_directory(dir_str, std::string(name));
  std::vector<u8> out(updated.begin(), updated.end());
  return this->write_file(parent, out);
}

} // namespace chfs
