// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_SDK_SDK_H
#define RSFS_SDK_SDK_H

#include "common/base/class_register.h"
#include "common/base/stdint.h"

#include "rsfs/proto/meta_tree.pb.h"
#include "rsfs/sdk/error_code.h"

namespace rsfs {
namespace sdk {

class SDK {
public:
    SDK() {}
    virtual ~SDK() {}

    static SDK* Open(const std::string& file_path,
                     const std::string& mode,
                     ErrorCode* err);

    virtual bool Close(ErrorCode* err) = 0;

    virtual int64_t Read(void* buf, uint32_t buf_size, ErrorCode* err) = 0;

    virtual int64_t Read(void* buf, uint32_t buf_size, int64_t offset,
                         ErrorCode* err) = 0;

    virtual int64_t Write(void* buf, uint32_t buf_size, ErrorCode* err) = 0;

    virtual int64_t GetSize(ErrorCode* err) = 0;

    static bool IsExist(const std::string& full_path, ErrorCode* err);

    static bool Copy(const std::string& src_path, const std::string& dst_path);

    static bool Remove(const std::string& full_path, bool is_recusive = false,
                       ErrorCode* err = NULL);

    static bool List(const std::string& full_path, std::vector<TreeNode>* list,
                     ErrorCode* err);
    static bool Listx(SDK* sdk_impl, const std::string& start, const std::string& end,
                      std::string* last_one, std::vector<TreeNode>* list,
                      ErrorCode* err);

    virtual std::string GetImplName() = 0;

    static SDK* CreateSDKImpl(const std::string& impl_type);

protected:
    virtual bool OpenImpl(const std::string& file_path,
                          const std::string& mode,
                          ErrorCode* err) = 0;
    virtual bool ListImpl(const std::string& start, const std::string& end,
                          std::string* last_one, std::vector<TreeNode>* list,
                          ErrorCode* err) = 0;
};

CLASS_REGISTER_DEFINE_REGISTRY(CommonSDK, rsfs::sdk::SDK);

#define REGISTER_RSFS_SDK(format, class_name)\
    CLASS_REGISTER_OBJECT_CREATOR(CommonSDK, \
                                  rsfs::sdk::SDK, \
                                  format, class_name)\

#define CREATE_RSFS_SDK_OBJECT(format_as_string)\
    CLASS_REGISTER_CREATE_OBJECT(CommonSDK, format_as_string)

} // namespace sdk
} // namespace rsfs

#endif // RSFS_SDK_SDK_H
