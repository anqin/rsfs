// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include <stdio.h>

#include "rsfs/proto/status_code.pb.h"
#include "rsfs/proto/proto_helper.h"

namespace rsfs {

std::string StatusCodeToString(int32_t status) {
    switch (status) {
    case kUnDefined:
        return "kUnDefined";
    // master
    case kMasterNotInited:
        return "kMasterNotInited";
    case kMasterIsBusy:
        return "kMasterIsBusy";
    case kMasterIsSecondary:
        return "kMasterIsSecondary";
    case kMasterIsReadonly:
        return "kMasterIsReadonly";
    case kMasterOnRestore:
        return "kMasterOnRestore";
    case kMasterIsRunning:
        return "kMasterIsRunning";
    case kMasterOnWait:
        return "kMasterOnWait";

    // snode
    case kSNodeNotInited:
        return "kSNodeNotInited";
    case kSNodeIsBusy:
        return "kSNodeIsBusy";
    case kSNodeIsIniting:
        return "kSNodeIsIniting";
    case kSNodeIsReadonly:
        return "kSNodeIsReadonly";
    case kSNodeIsRunning:
        return "kSNodeIsRunning";
    case kSNodeNotStream:
        return "kSNodeNotStream";
    case kSNodeErrStream:
        return "kSNodeErrStream";

    // ACL & system
    case kIllegalAccess:
        return "kIllegalAccess";
    case kNotPermission:
        return "kNotPermission";
    case kIOError:
        return "kIOError";
    case kBadParameter:
        return "kBadParameter";

    //// master rpc ////

    // register
    case kMasterOk:
        return "kMasterOk";
    case kMasterNotAlloc:
        return "kMasterNotAlloc";
    case kInvalidSequenceId:
        return "kInvalidSequenceId";
    case kInvalidTabletNodeInfo:
        return "kInvalidTabletNodeInfo";

    // report
    case kSNodeNotRegistered:
        return "kSNodeNotRegistered";


    // RPC
    case kRPCError:
        return "kRPCError";
    case kServerError:
        return "kServerError";
    case kClientError:
        return "kClientError";
    case kConnectError:
        return "kConnectError";
    case kRPCTimeout:
        return "kRPCTimeout";

    case kMetaNotInited:
        return "kMetaNotInited";
    case kMetaReadOpen:
        return "kMetaReadOpen";
    case kMetaWriteOpen:
        return "kMetaWriteOpen";
    case kMetaReady:
        return "kMetaReady";
    default:
        ;
    }
    char num[16];
    snprintf(num, 16, "%d", status);
    num[15] = '\0';
    return num;
}

bool TreeNodePBToString(const TreeNode& message, std::string* output) {
    if (!message.IsInitialized()) {
        LOG(ERROR) << "missing required fields: "
                << message.InitializationErrorString();
        return false;
    }
    if (!message.AppendToString(output)) {
        LOG(ERROR) << "fail to convert to string";
        return false;
    }

    return true;
}

bool StringToTreeNodePB(const std::string& str, TreeNode* message) {
    if (!message->ParseFromArray(str.c_str(), str.size())) {
        LOG(WARNING) << "missing required fields: "
            << message->InitializationErrorString();;
        return false;
    }
    return true;
}

} // namespace tera
