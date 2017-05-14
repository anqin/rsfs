// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_MASTER_NODE_MANAGER_H
#define RSFS_MASTER_NODE_MANAGER_H

#include <map>
#include <string>

#include "common/lock/rwlock.h"

#include "rsfs/proto/proto_helper.h"
#include "rsfs/proto/master_rpc.pb.h"

namespace rsfs {
namespace master {

class NodeState;

class NodeManager {
public:
    NodeManager();
    ~NodeManager();

    bool Register(const RegisterRequest* request,
                  RegisterResponse* response);

    bool Report(const ReportRequest* request,
                ReportResponse* response);

    bool AllocNode(uint32_t num, SNodeInfoList* node_list);

private:
    mutable RWLock m_rwlock;
    std::map<std::string, NodeState*> m_node_list;
};

} // namespace master
} // namespace rsfs

#endif // RSFS_MASTER_NODE_MANAGER_H
