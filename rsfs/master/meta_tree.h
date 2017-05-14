// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_MASTER_META_TREE_H
#define RSFS_MASTER_META_TREE_H

#include <string>

#include "leveldb/db.h"

#include "rsfs/proto/master_rpc.pb.h"
#include "rsfs/proto/meta_tree.pb.h"
#include "rsfs/proto/proto_helper.h"
#include "rsfs/proto/status_code.pb.h"

namespace rsfs {
namespace master {

class NodeManager;

class MetaTree {
public:
    MetaTree(NodeManager* node_manager);
    ~MetaTree();

    bool OpenFile(TreeNode* meta,
                  bool create_if_miss, StatusCode* code);
    bool CloseFile(TreeNode* meta, StatusCode* code);

    bool ListFile(const std::string& path_start,
                  const std::string& path_end, uint64_t size_limit,
                  TreeNodeList* meta_list, std::string* last_key,
                  StatusCode* code);

private:
    bool LoadDatabase(const std::string& db_path, leveldb::DB** db_handler);
    bool SplitTablePath(const std::string& full_path,
                        std::string* dir, std::string* file);
    leveldb::DB* PickDatabse(const std::string& full_path,
                             StatusCode* code);

private:
    uint64_t m_file_id;
    leveldb::DB* m_dir_meta;
    leveldb::DB* m_file_meta;
    std::string m_dir_meta_path;
    std::string m_file_meta_path;

    NodeManager* m_node_manager;
};

} // namespace master
} // namespace rsfs

#endif // RSFS_MASTER_META_TREE_H
