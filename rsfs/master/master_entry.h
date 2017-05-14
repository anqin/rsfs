// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef RSFS_MASTER_MASTER_ENTRY_H
#define RSFS_MASTER_MASTER_ENTRY_H

#include "bobby/bobby_server.h"

#include "common/base/scoped_ptr.h"
#include "rsfs/rsfs_entry.h"

namespace rsfs {
namespace master {

class MasterImpl;
class RemoteMaster;

class MasterEntry : public RsfsEntry {
public:
    MasterEntry();
    ~MasterEntry();

    bool StartServer();
    void ShutdownServer();

private:
    bool InitZKAdaptor();

private:
    scoped_ptr<MasterImpl> m_master_impl;
    //scoped_ptr<RemoteMaster> m_remote_master;
    RemoteMaster* m_remote_master;
    scoped_ptr<bobby::BobbyServer> m_bobby_server;
};

} // namespace master
} // namespace rsfs

#endif // RSFS_MASTER_MASTER_ENTRY_H
