// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/master/meta_tree.h"

#include "common/base/string_ext.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/master/node_manager.h"
#include "rsfs/proto/proto_helper.h"
#include "rsfs/utils/atomic.h"

DECLARE_string(rsfs_master_db_path);
DECLARE_string(rsfs_master_dirmeta_path);
DECLARE_string(rsfs_master_filemeta_path);

namespace rsfs {
namespace master {

MetaTree::MetaTree(NodeManager* node_manager)
    : m_file_id(1), m_dir_meta(NULL), m_file_meta(NULL),
      m_dir_meta_path(FLAGS_rsfs_master_db_path + "/" +
                        FLAGS_rsfs_master_dirmeta_path),
      m_file_meta_path(FLAGS_rsfs_master_db_path + "/" +
                       FLAGS_rsfs_master_filemeta_path),
      m_node_manager(node_manager) {
    LoadDatabase(m_dir_meta_path, &m_dir_meta);
    LoadDatabase(m_file_meta_path, &m_file_meta);
}

MetaTree::~MetaTree() {}

bool MetaTree::OpenFile(TreeNode* meta,
                        bool create_if_miss, StatusCode* code) {
    leveldb::DB* access_db = PickDatabse(meta->name(), code);
    if (access_db == NULL) {
        return false;
    }

    std::string value;
    leveldb::Status status = access_db->Get(leveldb::ReadOptions(),
                                            meta->name(), &value);
    if (status.ok() && meta->status() == kMetaReadOpen) {
        LOG(INFO) << "meta found (path: " << meta->name() << ")";
        if (value.empty() || !StringToTreeNodePB(value, meta)) {
            LOG(ERROR) << "fail to parse to tree meta";
            *code = kIOError;
            return false;
        }
        return true;
    } else if (status.ok() && meta->status() == kMetaWriteOpen) {
        LOG(INFO) << "meta has been exist (path: " << meta->name() << ")";
        *code = kIOError;
        return false;
    } else if (!status.ok() && meta->status() == kMetaWriteOpen
               && !create_if_miss) {
        LOG(INFO) << "meta not exist (path: " << meta->name() << ")";
        *code = kIOError;
        return false;
    }
    LOG(INFO) << "create new meta pb node (path: " << meta->name() << ")";
    if (!m_node_manager->AllocNode(meta->chunk_num(), meta->mutable_chunks())) {
        LOG(ERROR) << "fail to allocate chunk node (path: " << meta->name() << ")";
        *code = kMasterNotAlloc;
        return false;
    }
    meta->set_fid(atomic_inc_ret_old64(&m_file_id));
    meta->set_file_size(0);
//     meta->set_crash_slice(-1);
//     meta->set_crash_num(0);
    value = "";
//     meta->set_status(kMetaReady);
    if (!TreeNodePBToString(*meta, &value)) {
        LOG(ERROR) << "fail to parse meta pb to string";
        *code = kIOError;
        return false;
    }
    status = access_db->Put(leveldb::WriteOptions(), meta->name(), value);
    if (!status.ok()) {
        LOG(ERROR) << "fail to put meta node to storage (path: " << meta->name() << ")";
        *code = kIOError;
        return false;
    }
    return true;
}

bool MetaTree::CloseFile(TreeNode* meta, StatusCode* code) {
    leveldb::DB* access_db = PickDatabse(meta->name(), code);
    if (access_db == NULL) {
        return false;
    }

    std::string value;
    leveldb::Status status = access_db->Get(leveldb::ReadOptions(),
                                            meta->name(), &value);
    if (!status.ok() || value.empty()) {
        LOG(ERROR) << "dirty meta info (path: " << meta->name() << ")";
        return false;
    }
    TreeNode org_meta;
    if (!StringToTreeNodePB(value, &org_meta)) {
        LOG(ERROR) << "fail to parse tree meta (path: " << meta->name() << ")";
        return false;
    }
    if (org_meta.status() == kMetaWriteOpen) {
        org_meta.set_file_size(meta->file_size());
        if (meta->tail_slice() >= 0) {
            org_meta.set_tail_slice(meta->tail_slice());
            org_meta.set_tail_num(meta->tail_num());
        }
    } else if (org_meta.crash_slice() != meta->crash_slice()
               || org_meta.crash_num() != meta->crash_num()) {
        org_meta.set_crash_slice(meta->crash_slice());
        org_meta.set_crash_num(meta->crash_num());
    }
    LOG(INFO) << "updated meta: " << org_meta.ShortDebugString();

    org_meta.set_status(kMetaReady);
    value = "";
    if (!TreeNodePBToString(org_meta, &value)) {
        LOG(ERROR) << "fail to parse meta pb to string";
        *code = kIOError;
        return false;
    }
    status = access_db->Put(leveldb::WriteOptions(), org_meta.name(), value);
    if (!status.ok()) {
        LOG(ERROR) << "fail to put meta node to storage (path: " << org_meta.name() << ")";
        *code = kIOError;
        return false;
    }

    return true;
}

bool MetaTree::ListFile(const std::string& path_start,
                        const std::string& path_end, uint64_t size_limit,
                        TreeNodeList* meta_list, std::string* last_key,
                        StatusCode* code) {
    leveldb::DB* access_db = PickDatabse(path_start, code);
    if (access_db == NULL) {
        return false;
    }

    uint64_t size = 0;
    leveldb::Iterator* it = access_db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(path_start); it->Valid() && it->key().ToString() < path_end
         && size <= size_limit; it->Next()) {
        TreeNode meta;
        if (!StringToTreeNodePB(it->value().ToString(), &meta)) {
            LOG(WARNING) << "fail to parse tree meta (path: "
                << it->key().ToString() << "), skip";
            continue;
        }
        meta_list->Add()->CopyFrom(meta);
        size += meta.ByteSize();
    }
    delete it;
    return true;
}

bool MetaTree::LoadDatabase(const std::string& db_path, leveldb::DB** db_handler) {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_path, db_handler);
    CHECK(status.ok()) << ", fail to open db: " << db_path
        << ", status: " << status.ToString();
    return true;
}

leveldb::DB* MetaTree::PickDatabse(const std::string& full_path,
                                   StatusCode* code) {
    LOG(INFO) << "pick database by path: " << full_path;
    std::string dir_path;
    std::string file_path;
    if (!SplitTablePath(full_path, &dir_path, &file_path)) {
        LOG(ERROR) << "invalid db path: " << full_path;
        *code = kBadParameter;
        return NULL;
    }

    leveldb::DB* access_db = m_file_meta;
    if (file_path.empty()) {
        access_db = m_dir_meta;
    }
    return access_db;
}

bool MetaTree::SplitTablePath(const std::string& full_path,
                              std::string* dir, std::string* file) {
    // 1. /a/b/.../c/db_name/tablet_name/file
    //    ==> /a/b/.../c/db_name & tablet_name/file
    // 2. /a/b/.../c/db_name/tablet_name/lost/file
    //    ==> /a/b/.../c/db_name & tablet_name/lost/file

    std::string my_dir = full_path;
    std::string my_file;
    uint32_t split_cnt = 2;
    while (split_cnt > 0) {
        split_cnt--;
        std::string tmp_dir;
        std::string tmp_file;
        SplitStringEnd(my_dir, &tmp_dir, &tmp_file, "/");
        VLOG(5) << "my_dir: " << my_dir
            << ", tmp_dir: " << tmp_dir
            << ", tmp_file: " << tmp_file
            << ", my_file: " << my_file;
        if (tmp_dir.empty() || tmp_file.empty()) {
            return false;
        }
        my_dir = tmp_dir;
        my_file = tmp_file + "/" + my_file;
        if (tmp_file == "lost") {
            split_cnt++;
        }
    }
    if (file) {
        *file = my_file;
    }
    if (dir) {
        *dir = my_dir;
    }
    return true;
}

} // namespace master
} // namespace rsfs
