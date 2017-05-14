// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "rsfs/sdk/sdk.h"

#include "common/base/scoped_ptr.h"
#include "common/base/string_ext.h"
#include "common/collection/rscode/rscode.h"
#include "common/file/file_path.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "global_config.h"
#include "rsfs/sdk/local_sdk.h"
#include "rsfs/sdk/rsfs_sdk.h"
#include "rsfs/utils/utils_cmd.h"

DECLARE_int32(rsfs_sdk_rscode_block_size);

namespace rsfs {
namespace sdk {


SDK* SDK::Open(const std::string& file_path,
               const std::string& mode,
               ErrorCode* error_code) {
    if (file_path.empty()) {
        error_code->SetFailed(ErrorCode::kBadParam);
        return NULL;
    }

    std::string prefix = GetPathPrefix(file_path);
    SDK *sdk_impl = CreateSDKImpl(prefix);
    if (!sdk_impl->OpenImpl(file_path, mode, error_code)) {
        delete sdk_impl;
        return NULL;
    }

    return sdk_impl;
}

SDK* SDK::CreateSDKImpl(const std::string& impl_type) {
#if defined(USE_BAIDU_COMAKE)
    SDK* sdk_impl = NULL;
    // Baidu comake does not support class register
    if (impl_type == RSFS_SDK_PREFIX) {
        sdk_impl = new RsfsSDK();
//     } else if (impl_type == STDDEV_SDK_PREFIX) {
//         sdk_impl = new StddevSDK();
    } else {
        sdk_impl = new LocalSDK();
    }
#else
    SDK* sdk_impl = CREATE_RSFS_SDK_OBJECT(impl_type);
    if (sdk_impl == NULL) {
        sdk_impl = CREATE_RSFS_SDK_OBJECT(LOCAL_SDK_PREFIX);
    }
#endif
    CHECK(sdk_impl != NULL) << "miss link specific sdk library, impl type: "
        << impl_type;
    return sdk_impl;
}


bool SDK::IsExist(const std::string& full_path, ErrorCode* err) {

    return false;
}

bool SDK::Copy(const std::string& src_path, const std::string& dst_path) {
    ErrorCode err;
    scoped_ptr<SDK> src_file(Open(src_path, "r", &err));
    CHECK(src_file.get() != NULL);
    scoped_ptr<SDK> dst_file(Open(dst_path, "w", &err));
    CHECK(dst_file.get() != NULL);

    const uint32_t BUFFER_SIZE = FLAGS_rsfs_sdk_rscode_block_size;
    char* buffer = new char[BUFFER_SIZE];
    int64_t src_len = src_file->GetSize(&err);
    int64_t remain_len = src_len;

    uint32_t package_no = 0;
    while (remain_len > 0) {
        *buffer = '\0';

        int64_t copy_len = BUFFER_SIZE;
        if (copy_len > remain_len) {
            copy_len = remain_len;
        }
        err.Reset();
        int64_t read_count = src_file->Read(buffer, copy_len, &err);
        if (read_count != copy_len) {
            LOG(WARNING) << "missing read, expected " << copy_len << " (bytes), but "
                << read_count << ", err: " << ErrorCodeToString(err);
        }
        err.Reset();
        int64_t write_count = dst_file->Write(buffer, read_count, &err);
        if (read_count != copy_len) {
            LOG(ERROR) << "missing write, expected " << read_count << " (bytes), but "
                << write_count << ", err: " << ErrorCodeToString(err);
            break;
        }
        remain_len -= read_count;
        LOG(INFO) << "block #" << package_no++
            << ", md5: " << utils::GetMd5(buffer, read_count);
    }
    delete buffer;
    src_file->Close(&err);
    dst_file->Close(&err);

    return true;
}

bool SDK::Remove(const std::string& full_path, bool is_recusive,
                 ErrorCode* err) {
    return false;
}


bool SDK::List(const std::string& full_path, std::vector<TreeNode>* list,
               ErrorCode* err) {
    scoped_ptr<SDK> sdk_impl(CreateSDKImpl(GetPathPrefix(full_path)));
    std::string last_one;
    return Listx(sdk_impl.get(), full_path + "#", full_path + "~", &last_one, list,
                 err);
}

bool SDK::Listx(SDK* sdk_impl, const std::string& start, const std::string& end,
                std::string* last_one, std::vector<TreeNode>* list,
                ErrorCode* err) {
    return sdk_impl->ListImpl(start, end, last_one, list, err);
}

} // namespace sdk
} // namespace rsfs
