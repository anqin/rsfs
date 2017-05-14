// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_SDK_LOCAL_SDK_H
#define RSFS_SDK_LOCAL_SDK_H

#include "rsfs/sdk/sdk.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/base/scoped_ptr.h"
#include "common/file/file_stream.h"

namespace rsfs {
namespace sdk {

const std::string LOCAL_SDK_PREFIX = "/local/";

class LocalSDK : public SDK {
public:
    LocalSDK();
    virtual ~LocalSDK();

    std::string GetImplName();

    bool Close(ErrorCode* err);

    int64_t Read(void* buf, uint32_t buf_size, ErrorCode* err);

    int64_t Read(void* buf, uint32_t buf_size, int64_t offset,
                 ErrorCode* err);

    int64_t Write(void* buf, uint32_t buf_size, ErrorCode* err);

    int64_t GetSize(ErrorCode* err);

protected:
    bool OpenImpl(const std::string& file_path,
                  const std::string& mode,
                  ErrorCode* err);
    bool ListImpl(const std::string& start, const std::string& end,
                  std::string* last_one, std::vector<TreeNode>* list,
                  ErrorCode* err);

protected:
    void StatToTreeNode(const struct stat& st, TreeNode* tn);

private:
    scoped_ptr<FileStream> m_file_stream;
    std::string m_file_path;
};


} // namespace sdk
} // namespace rsfs

#endif // RSFS_SDK_LOCAL_SDK_H
