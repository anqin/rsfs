// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef RSFS_TYPES_H
#define RSFS_TYPES_H

#include "common/base/stdint.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gflags/gflags.h"


namespace rsfs {

typedef int32_t TabletNodeId;

const int32_t kInvalidId = -1;
const uint64_t kSequenceIDStart = 0;
const uint64_t kInvalidTimerId = 0;
const uint32_t kUnknownId = -1U;
const uint32_t kInvalidSessionId = -1U;
const std::string kUnknownAddr = "255.255.255.255:0000";
const uint64_t kMaxTimeStamp = (1ULL << 56) - 1;
const uint32_t kMaxHostNameSize = 255;
const std::string kMasterNodePath = "/master";
const std::string kMasterLockPath = "/master-lock";
const std::string kTsListPath = "/ts";
const std::string kKickPath = "/kick";
const std::string kRootTabletNodePath = "/root_table";
const std::string kSafeModeNodePath = "/safemode";
const std::string kSms = "[SMS] ";
const std::string kMail = "[MAIL] ";
const int64_t kLatestTs = INT64_MAX;
const int64_t kOldestTs = INT64_MIN;

} // namespace rsfs

#endif // RSFS_TYPES_H
