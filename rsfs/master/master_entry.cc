// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "rsfs/master/master_entry.h"

#include "common/net/ip_address.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/master/master_impl.h"
#include "rsfs/master/remote_master.h"
#include "rsfs/utils/utils_cmd.h"

DECLARE_string(rsfs_master_addr);
DECLARE_string(rsfs_master_port);

namespace rsfs {
namespace master {

MasterEntry::MasterEntry()
    : m_master_impl(NULL),
      m_remote_master(NULL),
      m_bobby_server(NULL) {}

MasterEntry::~MasterEntry() {}

bool MasterEntry::StartServer() {
    IpAddress master_addr(utils::GetLocalHostAddr(), FLAGS_rsfs_master_port);
    FLAGS_rsfs_master_addr = master_addr.ToString();
    LOG(INFO) << "Start master RPC server at: " << FLAGS_rsfs_master_addr;

    m_master_impl.reset(new MasterImpl());
    m_remote_master = new RemoteMaster(m_master_impl.get());

    if (!m_master_impl->Init()) {
        return false;
    }

    m_bobby_server.reset(new bobby::BobbyServer(master_addr.GetIp(),
                                                master_addr.GetPort()));
    m_bobby_server->RegisterService(m_remote_master);
    if (!m_bobby_server->StartServer()) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }

    LOG(INFO) << "finish starting master server";
    return true;
}

void MasterEntry::ShutdownServer() {
    m_bobby_server->StopServer();
    m_master_impl.reset();
}

} // namespace master
} // namespace rsfs
