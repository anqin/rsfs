// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/sdk/local_sdk.h"

#include "common/base/string_ext.h"
#include "common/file/file_path.h"
#include "common/file/file_types.h"
#include "thirdparty/glog/logging.h"

namespace rsfs {
namespace sdk {

REGISTER_RSFS_SDK(LOCAL_SDK_PREFIX, LocalSDK);

LocalSDK::LocalSDK()
    : m_file_stream(new FileStream()) {}

LocalSDK::~LocalSDK() {}

std::string LocalSDK::GetImplName() {
    return LOCAL_SDK_PREFIX;
}

bool LocalSDK::OpenImpl(const std::string& file_path,
                        const std::string& mode,
                        ErrorCode* err) {
    if (file_path.empty()) {
        err->SetFailed(ErrorCode::kBadParam);
        return false;
    }

    FileOpenMode open_mode = FILE_READ;
    if (mode == "w") {
        open_mode = FILE_WRITE;
    }

    FileErrorCode ret_code = kFileSuccess;
    if (!m_file_stream->Open(file_path, open_mode, &ret_code)) {
        LOG(ERROR) << "fail to open file: " << file_path;
        err->SetFailed(ErrorCode::kSystem);
        return false;
    }
    m_file_path = file_path;
    return true;
}

bool LocalSDK::Close(ErrorCode* err) {
    FileErrorCode ret_code = kFileSuccess;
    if (!m_file_stream->Close(&ret_code)) {
        LOG(ERROR) << "fail to close file";
        err->SetFailed(ErrorCode::kSystem);
        return false;
    }
    return true;
}

int64_t LocalSDK::Read(void* buf, uint32_t buf_size, ErrorCode* err) {
    FileErrorCode ret_code = kFileSuccess;
    int64_t read_count = m_file_stream->Read(buf, buf_size, &ret_code);
    if (ret_code != kFileSuccess) {
        err->SetFailed(ErrorCode::kSystem);
    }
    return read_count;
}

int64_t LocalSDK::Read(void* buf, uint32_t buf_size, int64_t offset,
                       ErrorCode* err) {
    int64_t org_offset = m_file_stream->Tell();
    FileErrorCode ret_code = kFileSuccess;
    if (m_file_stream->Seek(offset, SEEK_SET, &ret_code) < 0) {
        LOG(ERROR) << "fail to seek to: " << offset << " in file:" << m_file_path;
        return -1;
    }
    int64_t read_count = m_file_stream->Read(buf, buf_size, &ret_code);
    if (m_file_stream->Seek(org_offset, SEEK_SET, &ret_code) < 0) {
        LOG(WARNING) << "fail to seek back to: " << org_offset
            << " in file:" << m_file_path;
    }
    return read_count;
}

int64_t LocalSDK::Write(void* buf, uint32_t buf_size, ErrorCode* err) {
    FileErrorCode ret_code = kFileSuccess;
    int64_t write_count = m_file_stream->Write(buf, buf_size, &ret_code);
    if (ret_code != kFileSuccess) {
        err->SetFailed(ErrorCode::kSystem);
    }
    return write_count;
}

int64_t LocalSDK::GetSize(ErrorCode* err) {
    return m_file_stream->GetSize(m_file_path);
}

bool LocalSDK::ListImpl(const std::string& start, const std::string& end,
                        std::string* last_one, std::vector<TreeNode>* list,
                        ErrorCode* err) {
    std::string path;
    SplitStringEnd(start, &path, NULL, "/");
    std::vector<std::string> file_names;
    if (!ListCurrentDir(path, &file_names)) {
        err->SetFailed(ErrorCode::kSystem);
        return false;
    }
    for (size_t i = 0 ; i < file_names.size() ; ++i) {
        std::string abs_path = path + "/" + file_names[i];
        struct stat filestat;
        if (0 == stat(abs_path.c_str(),  &filestat)) {
            TreeNode node;
            node.set_name(abs_path);
            StatToTreeNode(filestat, &node);
            list->push_back(node);
        } else {
            LOG(ERROR) << "invalid file path: " << abs_path;
        }
    }
    *last_one = end;
    return true;
}

void LocalSDK::StatToTreeNode(const struct stat& st, TreeNode* tn) {
    tn->set_fid(st.st_ino);
    tn->set_status(kMetaReady);
    tn->set_file_size(st.st_size);
    tn->set_chunk_num(1);
//     tn->mutable_chunks()->Add();
}

} // namespace sdk
} // namespace rsfs
