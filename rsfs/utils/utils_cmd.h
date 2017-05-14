// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_UTILS_UTILS_CMD_H
#define RSFS_UTILS_UTILS_CMD_H

#include <map>
#include <string>
#include "stdint.h"

namespace rsfs {
namespace utils {

std::string GetBinaryLocationDir();

std::string GetCurrentLocationDir();

std::string GetValueFromeEnv(const std::string& env_name);

std::string ConvertByteToString(const uint64_t size);

std::string GetLocalHostAddr();

std::string GetLocalHostName();

bool ExecuteShellCmd(const std::string cmd,
                     std::string* ret_str = NULL);

void SetupLog(const std::string& program_name);

std::string GetMd5(const char* buf, uint32_t buf_size);

std::string TruncateString(const std::string& str, uint32_t width);

} // namespace utils
} // namespace rsfs

#endif // RSFS_UTILS_UTILS_CMD_H
