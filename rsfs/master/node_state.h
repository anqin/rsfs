// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_MASTER_NODE_STATE_H
#define RSFS_MASTER_NODE_STATE_H

#include "rsfs/proto/master_rpc.pb.h"
#include "rsfs/proto/snode_info.pb.h"
#include "rsfs/proto/status_code.pb.h"

namespace rsfs {
namespace master {

class NodeState {
public:
    NodeState(const SNodeInfo& node_info);
    ~NodeState();

    bool Report(const ReportRequest* request,
                ReportResponse* response);

    SNodeInfo GetSNodeInfo() const;
    StatusCode GetStatus() const;

private:
    SNodeInfo m_snode_info;
};

} // namespace master
} // namespace rsfs

#endif // RSFS_MASTER_NODE_STATE_H
