// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/snode/block_manager.h"

#include "common/base/string_number.h"
#include "common/file/file_types.h"
#include "thirdparty/glog/logging.h"

DECLARE_string(rsfs_snode_path_prefix);

namespace rsfs {
namespace snode {

BlockStream::BlockStream(FileStream* stream, Type type)
    : m_stream(stream), m_type(type), m_ref_count(1) {}

BlockStream::~BlockStream() {}

FileStream* BlockStream::GetFileStream() {
    return m_stream;
}

BlockStream::Type BlockStream::GetType() const {
    return m_type;
}

int32_t BlockStream::AddRef() {
    MutexLocker lock(m_mutex);
    ++m_ref_count;
    return m_ref_count;
}

int32_t BlockStream::DecRef() {
    int32_t ref = 0;
    {
        MutexLocker lock(m_mutex);
        ref = (--m_ref_count);
    }
    if (ref == 0) {
        delete this;
    }
    return ref;
}

int32_t BlockStream::GetRef() const {
    return m_ref_count;
}

BlockManager::BlockManager() {}

BlockManager::~BlockManager() {}


bool BlockManager::NewBlockStream(uint64_t block_id, BlockStream::Type type) {
    std::string path = FLAGS_rsfs_snode_path_prefix + "/" + NumberToString(block_id);
    FileErrorCode err = kFileSuccess;
    FileStream* file = new FileStream;

    uint32_t mode = FILE_READ;
    if (type == BlockStream::APPEND) {
        mode = FILE_APPEND;
    }
    if (!file->Open(path, mode, &err)) {
        LOG(ERROR) << "fail to create file stream for block [id: "
            << block_id << "]";
        return false;
    }

    MutexLocker lock(m_mutex_list);
    m_block_io[block_id] = new BlockStream(file, type);
    LOG(INFO) << "block #" << block_id << " open success";
    return true;
}

BlockStream* BlockManager::GetBlockStream(uint64_t block_id) {
    MutexLocker lock(m_mutex_list);
    std::map<uint64_t, BlockStream*>::iterator it =
        m_block_io.find(block_id);
    if (it == m_block_io.end()) {
        LOG(ERROR) << "block [id: " << block_id << "] not exist";
        return NULL;
    }
    it->second->AddRef();
    return it->second;
}

bool BlockManager::AddBlockStream(uint64_t block_id, BlockStream* stream) {
    MutexLocker lock(m_mutex_list);
    std::map<uint64_t, BlockStream*>::iterator it =
        m_block_io.find(block_id);
    if (it != m_block_io.end()) {
        LOG(ERROR) << "block [id: " << block_id << "] already exist";
        return false;
    }
    m_block_io[block_id] = stream;
    return true;
}

bool BlockManager::RemoveBlockStream(uint64_t block_id) {
    MutexLocker lock(m_mutex_list);
    std::map<uint64_t, BlockStream*>::iterator it =
        m_block_io.find(block_id);
    if (it == m_block_io.end()) {
        LOG(ERROR) << "block [id: " << block_id << "] not exist";
        return false;
    } else if (it->second->DecRef() == 0) {
        m_block_io.erase(it);
    }
    return true;
}

} // namespace snode
} // namespace rsfs

