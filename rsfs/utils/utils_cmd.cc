// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/utils/utils_cmd.h"

#include <openssl/md5.h>
#include <stdio.h>
#include <stdlib.h>

#include "common/base/scoped_ptr.h"
#include "common/base/string_ext.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/types.h"

DECLARE_string(log_dir);

namespace rsfs {
namespace utils {

std::string GetBinaryLocationDir() {
    char exec_full_path[1024] = {'\0'};
    readlink ("/proc/self/exe", exec_full_path, 1024);
    VLOG(5) << "current binary location: " << exec_full_path;

    std::string full_dir;
    SplitStringPath(exec_full_path, &full_dir, NULL);
    return full_dir;
}

std::string GetCurrentLocationDir() {
    char current_path[1024] = {'\0'};
    std::string current_dir;

    if (getcwd(current_path, 1024)) {
        current_dir = current_path;
    }
    return current_dir;
}

std::string GetValueFromeEnv(const std::string& env_name) {
    if (env_name.empty()) {
        return "";
    }

    const char* env = getenv(env_name.c_str());
    if (!env) {
        VLOG(5) << "fail to fetch from env: " << env_name;
        return "";
    }
    return env;
}

std::string ConvertByteToString(const uint64_t size) {
    std::string hight_unit;
    double min_size;
    const uint64_t kKB = 1024;
    const uint64_t kMB = kKB * 1024;
    const uint64_t kGB = kMB * 1024;
    const uint64_t kTB = kGB * 1024;
    const uint64_t kPB = kTB * 1024;

    if (size == 0) {
        return "0 ";
    }

    if (size > kPB) {
        min_size = (1.0 * size) / kPB;
        hight_unit = "P";
    } else if (size > kTB) {
        min_size = (1.0 * size) / kTB;
        hight_unit = "T";
    } else if (size > kGB) {
        min_size = (1.0 * size) / kGB;
        hight_unit = "G";
    } else if (size > kMB) {
        min_size = (1.0 * size) / kMB;
        hight_unit = "M";
    } else if (size > kKB) {
        min_size = (1.0 * size) / kKB;
        hight_unit = "K";
    } else {
        min_size = size;
        hight_unit = " ";
    }

    if ((int)min_size - min_size == 0) {
        return StringFormat("%d%s", (int)min_size, hight_unit.c_str());
    } else {
        return StringFormat("%.2f%s", min_size, hight_unit.c_str());
    }
}

bool ExecuteShellCmd(const std::string cmd, std::string* ret_str) {
    char output_buffer[80];
    FILE *fp = popen(cmd.c_str(), "r");
    if (!fp) {
        LOG(ERROR) << "fail to execute cmd: " << cmd;
        return false;
    }
    fgets(output_buffer, sizeof(output_buffer), fp);
    pclose(fp);
    if (ret_str) {
        *ret_str = std::string(output_buffer);
    }
    return true;
}

std::string GetLocalHostAddr() {
    std::string cmd =
        "/sbin/ifconfig | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'";
    std::string addr;
    if (!ExecuteShellCmd(cmd, &addr)) {
        LOG(ERROR) << "fail to fetch local host addr";
    } else if (addr.length() > 1) {
        addr.erase(addr.length() - 1, 1);
    }
    return addr;
}

std::string GetLocalHostName() {
    char str[kMaxHostNameSize + 1];
    if (0 != gethostname(str, kMaxHostNameSize + 1)) {
        LOG(FATAL) << "gethostname fail";
        exit(1);
    }
    std::string hostname(str);
    return hostname;
}

void SetupLog(const std::string& name) {
    // log info/warning/error/fatal to rsfs.log
    // log warning/error/fatal to rsfs.wf

    std::string program_name = "rsfs";
    if (!name.empty()) {
        program_name = name;
    }

    std::string log_filename = FLAGS_log_dir + "/" + program_name + ".INFO.";
    std::string wf_filename = FLAGS_log_dir + "/" + program_name + ".WARNING.";
    google::SetLogDestination(google::INFO, log_filename.c_str());
    google::SetLogDestination(google::WARNING, wf_filename.c_str());
    google::SetLogDestination(google::ERROR, "");
    google::SetLogDestination(google::FATAL, "");

    google::SetLogSymlink(google::INFO, program_name.c_str());
    google::SetLogSymlink(google::WARNING, program_name.c_str());
    google::SetLogSymlink(google::ERROR, "");
    google::SetLogSymlink(google::FATAL, "");
}

std::string GetMd5(const char* buf, uint32_t buf_size) {
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);
    MD5_Update(&md5_ctx, buf, buf_size);

    unsigned char md5_out[MD5_DIGEST_LENGTH] = {'\0'};
    MD5_Final(md5_out, &md5_ctx);
    char md5_out_str[MD5_DIGEST_LENGTH * 2 + 1] = {'\0'};
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        sprintf(&md5_out_str[i*2], "%02x", (unsigned int)md5_out[i]);
    }
    return md5_out_str;
}

std::string TruncateString(const std::string& str, uint32_t width) {
    return str;
}

} // namespace utils
} // namespace rsfs
