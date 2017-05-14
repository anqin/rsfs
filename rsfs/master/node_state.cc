// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "rsfs/master/node_state.h"

namespace rsfs {
namespace master {


NodeState::NodeState(const SNodeInfo& node_info)
    : m_snode_info(node_info) {}

NodeState::~NodeState() {}

SNodeInfo NodeState::GetSNodeInfo() const {
    return m_snode_info;
}

StatusCode NodeState::GetStatus() const {
    return m_snode_info.status();
}

bool NodeState::Report(const ReportRequest* request,
                       ReportResponse* response) {
    m_snode_info.CopyFrom(request->snode_info());
    response->set_status(kMasterOk);
    return true;
}

} // namespace master
} // namespace rsfs
