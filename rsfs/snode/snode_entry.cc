// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "rsfs/snode/snode_entry.h"

#include "common/net/ip_address.h"
#include "common/thread/this_thread.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/master/master_client.h"
#include "rsfs/proto/snode_info.pb.h"
#include "rsfs/snode/remote_snode.h"
#include "rsfs/snode/snode_impl.h"
#include "rsfs/utils/utils_cmd.h"

DECLARE_string(rsfs_snode_addr);
DECLARE_string(rsfs_snode_port);
DECLARE_int64(rsfs_heartbeat_period);

namespace rsfs {
namespace snode {

SNodeEntry::SNodeEntry()
    : m_snode_impl(NULL),
      m_remote_snode(NULL),
      m_master_client(new master::MasterClient()),
      m_bobby_server(NULL) {}

SNodeEntry::~SNodeEntry() {}

bool SNodeEntry::StartServer() {
    IpAddress snode_addr(utils::GetLocalHostAddr(), FLAGS_rsfs_snode_port);
    FLAGS_rsfs_snode_addr = snode_addr.GetIp();
    LOG(INFO) << "Start RPC server at: " << FLAGS_rsfs_snode_addr;

    SNodeInfo snode_info;
    snode_info.set_addr(snode_addr.ToString());
    snode_info.set_status(kSNodeIsRunning);

    m_snode_impl.reset(new SNodeImpl(snode_info, m_master_client.get()));
    m_remote_snode.reset(new RemoteSNode(m_snode_impl.get()));

    m_bobby_server.reset(new bobby::BobbyServer(snode_addr.GetIp(),
                                                snode_addr.GetPort()));
    m_bobby_server->RegisterService(m_remote_snode.get());
    if (!m_bobby_server->StartServer()) {
        LOG(ERROR) << "start RPC server error";
        return false;
    } else if (!m_snode_impl->Register()) {
        LOG(ERROR) << "fail to register to master";
        return false;
    }

    LOG(INFO) << "finish starting RPC server";
    return true;
}

void SNodeEntry::ShutdownServer() {
    LOG(INFO) << "shut down server";
    m_bobby_server->StopServer();
    m_snode_impl.reset();
    m_master_client.reset();
}

bool SNodeEntry::Run() {
    m_snode_impl->Report();
    ThisThread::Sleep(FLAGS_rsfs_heartbeat_period);
    return true;
}

} // namespace snode
} // namespace rsfs
