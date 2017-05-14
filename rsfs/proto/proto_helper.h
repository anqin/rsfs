// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_PROTO_PROTO_HELPER_H
#define RSFS_PROTO_PROTO_HELPER_H

#include <string>

#include "rsfs/proto/master_rpc.pb.h"
#include "rsfs/proto/meta_tree.pb.h"

namespace rsfs {

typedef ::google::protobuf::RepeatedPtrField< SNodeInfo> SNodeInfoList;
typedef ::google::protobuf::RepeatedPtrField< TreeNode> TreeNodeList;

std::string StatusCodeToString(int32_t status);

bool TreeNodePBToString(const TreeNode& message, std::string* output);
bool StringToTreeNodePB(const std::string& str, TreeNode* message);


} // namespace rsfs

#endif // RSFS_PROTO_PROTO_HELPER_H
