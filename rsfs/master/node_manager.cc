// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/master/node_manager.h"

#include "rsfs/master/node_state.h"

namespace rsfs {
namespace master {


NodeManager::NodeManager() {}

NodeManager::~NodeManager() {}

bool NodeManager::Register(const RegisterRequest* request,
                           RegisterResponse* response) {
    RWLock::WriterLocker locker(&m_rwlock);
    std::map<std::string, NodeState*>::iterator it =
        m_node_list.find(request->snode_info().addr());
    if (it != m_node_list.end()) {
        LOG(INFO) << "node: " << request->snode_info().addr()
            << " has registered";
    } else {
        LOG(INFO) << "new register node: "
            << request->snode_info().addr();

        m_node_list[request->snode_info().addr()]
            = new NodeState(request->snode_info());
    }
    response->set_status(kMasterOk);
    return true;
}

bool NodeManager::Report(const ReportRequest* request,
                         ReportResponse* response) {
    NodeState* node_state = NULL;
    {
        RWLock::ReaderLocker locker(m_rwlock);
        std::map<std::string, NodeState*>::iterator it =
            m_node_list.find(request->snode_info().addr());
        if (it == m_node_list.end()) {
            LOG(ERROR) << "invalid report from node: "
                << request->snode_info().addr();
            response->set_status(kSNodeNotExist);
            return false;
        } else if (it->second->GetStatus() != kSNodeIsRunning) {
            LOG(ERROR) << "node is not running, current status: "
                << StatusCodeToString(it->second->GetStatus());
            response->set_status(kMasterOk);
        }
        node_state = it->second;
    }
    CHECK(node_state != NULL);
    return node_state->Report(request, response);
}

bool NodeManager::AllocNode(uint32_t num, SNodeInfoList* node_list) {
    RWLock::WriterLocker locker(&m_rwlock);
    std::map<std::string, NodeState*>::iterator it =
        m_node_list.begin();
    uint32_t try_count = 0;
    for (uint32_t i = 0; i < num; ++i) {
        SNodeInfo* node_info = node_list->Add();
        while (true) {
            it++;
            if (it == m_node_list.end()) {
                it = m_node_list.begin();
                if (try_count == m_node_list.size()) {
                    return false;
                }
                try_count = 0;
            }
            if (it->second->GetStatus() == kSNodeIsRunning) {
                node_info->CopyFrom(it->second->GetSNodeInfo());
                break;
            }
            try_count++;
        }
    }
    return true;
}


} // namespace master
} // namespace rsfs

